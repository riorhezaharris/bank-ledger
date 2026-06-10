//go:build integration

// Package integration_test is an end-to-end test suite for the 3-node CP cluster.
//
// Prerequisites:
//
//	make up   — cluster must be running before executing these tests.
//
// Run:
//
//	go test -tags integration -v -count=1 ./test/integration/
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riorhezaharris/bank-ledger/internal/domain"
)

const (
	node1URL = "http://localhost:8081"
	node2URL = "http://localhost:8082"
	node3URL = "http://localhost:8083"

	node1Container        = "bank-ledger-node1-1"
	node2Container        = "bank-ledger-node2-1"
	defaultNode3Container = "bank-ledger-node3-1"
)

var (
	node3Container string

	// Cluster-network IPs of node1/node2; discovered once in TestMain and
	// used by every call to blockPeers / unblockPeers.
	node1ClusterIP string
	node2ClusterIP string

	// httpClient has an explicit timeout so poll loops never hang on a stalled TCP connection.
	httpClient = &http.Client{Timeout: 5 * time.Second}
)

func TestMain(m *testing.M) {
	node3Container = os.Getenv("NODE3_CONTAINER")
	if node3Container == "" {
		node3Container = defaultNode3Container
	}

	for _, u := range []string{node1URL, node2URL, node3URL} {
		if err := pollCanWrite(u, true, 30*time.Second); err != nil {
			log.Fatalf("cluster not ready at %s: %v\nRun 'make up' first.", u, err)
		}
	}

	// Discover cluster IPs for iptables-based partition simulation.
	var err error
	node1ClusterIP, err = clusterIP(node1Container)
	if err != nil {
		log.Fatalf("discover node1 cluster IP: %v", err)
	}
	node2ClusterIP, err = clusterIP(node2Container)
	if err != nil {
		log.Fatalf("discover node2 cluster IP: %v", err)
	}

	log.Printf("cluster ready  node3=%s  node1-ip=%s  node2-ip=%s",
		node3Container, node1ClusterIP, node2ClusterIP)
	os.Exit(m.Run())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestAllNodesHealthy(t *testing.T) {
	for _, u := range []string{node1URL, node2URL, node3URL} {
		h := getHealth(t, u)
		if !h.CanWrite {
			t.Errorf("node at %s: expected canWrite=true, got false", u)
		}
	}
}

// TestPartitionScenario is the core CP demonstration. Sub-tests run in order
// and share the partition/heal lifecycle so they cannot be run independently.
func TestPartitionScenario(t *testing.T) {
	t.Cleanup(func() {
		// Best-effort heal so the cluster is clean for the next test run.
		unblockPeers()
		_ = pollCanWrite(node3URL, true, 10*time.Second)
	})

	// ── Step 1: Isolate node3 ─────────────────────────────────────────────────
	// We add iptables DROP rules inside node3's network namespace so it cannot
	// send to or receive from node1/node2. Port publishing (localhost:8083) is
	// unaffected — Docker's DNAT rules are never touched.

	t.Log("blocking node3's cluster traffic to node1 and node2 via iptables...")
	blockPeers(t)

	// ── Step 2: Wait for node3 to detect the partition ────────────────────────
	// 3 missed heartbeats × 500ms = ~1.5s. Allow 5s for safety.

	t.Run("partition_detected", func(t *testing.T) {
		t.Log("waiting for node3 to flip canWrite=false (~1.5s)...")
		if err := pollCanWrite(node3URL, false, 5*time.Second); err != nil {
			t.Fatalf("node3 did not detect partition: %v", err)
		}
		t.Log("node3 correctly reports canWrite=false")
	})

	// ── Step 3: Isolated node must reject writes ───────────────────────────────

	t.Run("isolated_node_rejects_writes", func(t *testing.T) {
		status, body := postAccount(node3URL, "reject-me-"+uuid.New().String())
		if status == 0 {
			t.Fatalf("could not reach node3 at all (status=0): %s", body)
		}
		if status != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 from isolated node3, got %d\n%s", status, body)
		}
		t.Logf("node3 correctly returned 503: %s", body)
	})

	// ── Step 4: Majority must remain available ────────────────────────────────

	t.Run("majority_accepts_writes", func(t *testing.T) {
		for _, u := range []string{node1URL, node2URL} {
			status, body := postAccount(u, "majority-"+uuid.New().String())
			if status != http.StatusCreated {
				t.Fatalf("expected 201 from %s, got %d\n%s", u, status, body)
			}
			t.Logf("%s accepted write: %s", u, body)
		}
	})

	// ── Step 5: Create a canary account on the majority ───────────────────────
	// We will verify this account appears on node3 after the heal, proving resync.

	canaryName := "canary-" + uuid.New().String()
	var canaryID string

	t.Run("create_canary_during_partition", func(t *testing.T) {
		status, body := postAccount(node1URL, canaryName)
		if status != http.StatusCreated {
			t.Fatalf("create canary: expected 201, got %d\n%s", status, body)
		}
		var a domain.Account
		if err := json.Unmarshal(body, &a); err != nil {
			t.Fatalf("parse account: %v", err)
		}
		canaryID = a.ID
		t.Logf("canary id=%s", canaryID)
	})

	if canaryID == "" {
		t.Fatal("canary account ID not set — cannot run resync test")
	}

	// ── Step 6: Heal ──────────────────────────────────────────────────────────

	t.Log("removing iptables DROP rules on node3 (heal)...")
	unblockPeers()

	// ── Step 7: Wait for node3 to resync and rejoin ───────────────────────────

	t.Run("node_rejoins_quorum_after_heal", func(t *testing.T) {
		t.Log("waiting for node3 to resync and set canWrite=true...")
		if err := pollCanWrite(node3URL, true, 10*time.Second); err != nil {
			t.Fatalf("node3 did not rejoin quorum: %v", err)
		}
		t.Log("node3 correctly reports canWrite=true")
	})

	// ── Step 8: Canary account must be visible on node3 (resync proof) ────────

	t.Run("node_resynced_canary_account_visible", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var lastStatus int
		for {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
				node3URL+"/accounts/"+canaryID, nil)
			resp, err := httpClient.Do(req)
			if err == nil {
				lastStatus = resp.StatusCode
				resp.Body.Close()
				if lastStatus == http.StatusOK {
					t.Logf("canary %s visible on node3 — resync confirmed", canaryID)
					return
				}
			}
			select {
			case <-ctx.Done():
				t.Fatalf("canary %s not visible on node3 after heal (last status: %d)", canaryID, lastStatus)
			case <-time.After(300 * time.Millisecond):
			}
		}
	})

	// ── Step 9: node3 must accept new writes ──────────────────────────────────

	t.Run("node_accepts_new_writes_after_resync", func(t *testing.T) {
		status, body := postAccount(node3URL, "post-heal-"+uuid.New().String())
		if status != http.StatusCreated {
			t.Fatalf("expected 201 from node3 after heal, got %d\n%s", status, body)
		}
		t.Logf("node3 accepted write after resync: %s", body)
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

type healthResponse struct {
	CanWrite bool `json:"can_write"`
}

func getHealth(t *testing.T, nodeURL string) healthResponse {
	t.Helper()
	resp, err := httpClient.Get(nodeURL + "/health")
	if err != nil {
		t.Fatalf("GET %s/health: %v", nodeURL, err)
	}
	defer resp.Body.Close()
	var h healthResponse
	json.NewDecoder(resp.Body).Decode(&h)
	return h
}

// pollCanWrite polls /health until can_write matches want, or timeout elapses.
// A connection failure counts as canWrite=false — an unreachable node cannot write.
func pollCanWrite(nodeURL string, want bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := httpClient.Get(nodeURL + "/health")
		if err != nil {
			if !want {
				return nil // connection failure == canWrite=false
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		var h healthResponse
		json.NewDecoder(resp.Body).Decode(&h)
		resp.Body.Close()
		if h.CanWrite == want {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %s waiting for canWrite=%v", timeout, want)
}

// postAccount creates an account and returns the HTTP status and raw body.
func postAccount(nodeURL, name string) (int, []byte) {
	body, _ := json.Marshal(map[string]string{"name": name, "currency": "USD"})
	resp, err := httpClient.Post(nodeURL+"/accounts", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, []byte(err.Error())
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// blockPeers adds iptables DROP rules inside node3 for node1 and node2's
// cluster IPs, fully simulating a minority partition without touching Docker
// networks or port-publishing DNAT rules.
func blockPeers(t *testing.T) {
	t.Helper()
	for _, ip := range []string{node1ClusterIP, node2ClusterIP} {
		mustExec(t, "docker", "exec", node3Container, "iptables", "-A", "OUTPUT", "-d", ip, "-j", "DROP")
		mustExec(t, "docker", "exec", node3Container, "iptables", "-A", "INPUT", "-s", ip, "-j", "DROP")
	}
}

// unblockPeers removes the DROP rules added by blockPeers.
// It is idempotent — errors are silently ignored so it can be called from
// t.Cleanup even when the rules were already removed by the heal step.
func unblockPeers() {
	for _, ip := range []string{node1ClusterIP, node2ClusterIP} {
		exec.Command("docker", "exec", node3Container, "iptables", "-D", "OUTPUT", "-d", ip, "-j", "DROP").Run()
		exec.Command("docker", "exec", node3Container, "iptables", "-D", "INPUT", "-s", ip, "-j", "DROP").Run()
	}
}

// clusterIP returns the container's IP address on the bank-ledger_cluster network.
func clusterIP(container string) (string, error) {
	out, err := exec.Command("docker", "inspect", container,
		"--format", `{{(index .NetworkSettings.Networks "bank-ledger_cluster").IPAddress}}`).Output()
	if err != nil {
		return "", err
	}
	ip := strings.TrimSpace(string(out))
	if ip == "" {
		return "", fmt.Errorf("container %s has no bank-ledger_cluster IP — is it running?", container)
	}
	return ip, nil
}

// mustExec runs a command and fails the test immediately on error.
func mustExec(t *testing.T, name string, args ...string) {
	t.Helper()
	if out, err := exec.Command(name, args...).CombinedOutput(); err != nil {
		t.Fatalf("exec %q %v:\n%v\n%s", name, args, err, out)
	}
}
