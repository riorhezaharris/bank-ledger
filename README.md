# Bank Ledger — CP-Strategy Distributed Ledger

A strongly consistent, partition-tolerant bank ledger built from first principles to demonstrate CP system design. Three nodes replicate every write via two-phase commit under a strict quorum rule (R+W > N). When a network partition isolates a minority node, it immediately rejects writes and self-heals through incremental resync once the partition clears.

## Table of Contents

- [Design goals](#design-goals)
- [Architecture overview](#architecture-overview)
- [Key design decisions](#key-design-decisions)
  - [Quorum (N=3, W=2, R=2)](#quorum-n3-w2-r2)
  - [Any-node coordinator](#any-node-coordinator)
  - [Two-phase commit](#two-phase-commit)
  - [Eager heartbeat partition detection](#eager-heartbeat-partition-detection)
  - [Incremental resync on partition recovery](#incremental-resync-on-partition-recovery)
  - [Double-entry accounting](#double-entry-accounting)
  - [Idempotency and safe retries](#idempotency-and-safe-retries)
- [Project structure](#project-structure)
- [API reference](#api-reference)
- [Getting started](#getting-started)
- [Running the integration tests](#running-the-integration-tests)
- [Live partition demo](#live-partition-demo)

---

## Design goals

| Goal | How it is met |
|------|--------------|
| **Strong consistency** | Every read overlaps every write: quorum R=2, W=2, N=3 → R+W=4 > N=3 |
| **Partition tolerance** | Minority node detects isolation via heartbeat and rejects writes; majority continues |
| **No data loss** | 2PC prevents a coordinator crash from leaving nodes diverged |
| **No money created or destroyed** | Double-entry accounting; every transfer is exactly one DEBIT + one CREDIT |
| **Safe client retries** | Per-transfer idempotency key; duplicate commits are detected at quorum level |

This is a **CP system** in the CAP sense: it sacrifices availability on the minority side of a partition rather than risk returning stale or inconsistent data.

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────────┐
│                       Client                                │
└──────────┬──────────────────┬───────────────────────────────┘
           │ :8081            │ :8082          │ :8083
    ┌──────▼──────┐    ┌──────▼──────┐  ┌──────▼──────┐
    │   node1     │    │   node2     │  │   node3     │
    │ coordinator │◄──►│ coordinator │◄►│ coordinator │
    │  heartbeat  │    │  heartbeat  │  │  heartbeat  │
    └──────┬──────┘    └──────┬──────┘  └──────┬──────┘
           │                  │                │
    ┌──────▼──────┐    ┌──────▼──────┐  ┌──────▼──────┐
    │  postgres1  │    │  postgres2  │  │  postgres3  │
    └─────────────┘    └─────────────┘  └─────────────┘
```

- Every node is an **identical peer** — there is no elected primary.
- Any node can coordinate a write. The coordinating node runs 2PC across itself and at least one peer.
- Each node owns a **dedicated Postgres instance**; there is no shared storage.
- Nodes discover each other and monitor liveness via a background **heartbeat** goroutine.
- Six containers total, connected by two Docker networks:
  - `bank-ledger_cluster` — inter-node communication (heartbeats, 2PC RPCs, resync). Severing this network for one container simulates a minority partition.
  - `bank-ledger_ext{1,2,3}` — per-node external bridges used exclusively for host→node port publishing. These are never severed, so `localhost:808{1,2,3}` stays reachable throughout partition tests.

---

## Key design decisions

### Quorum (N=3, W=2, R=2)

The quorum parameters enforce that every read intersects every write:

```
R + W > N   →   2 + 2 > 3   ✓
```

A write only succeeds when the coordinator AND at least one peer have durably stored the data. A read takes the response from the node with the latest `committed_at` timestamp across self and one peer, guaranteeing it will always see the most recent committed write.

A node that cannot contact enough peers to reach write quorum sets `canWrite = false` and returns `503 Service Unavailable` on every mutating request. It continues to serve reads from its local state.

### Any-node coordinator

There is no fixed primary. Any node that receives a write request becomes the coordinator for that request. The coordinator:

1. Checks its own `canWrite` flag (fast path for isolated nodes).
2. Performs a **quorum read** on the source account balance across self + one peer, taking the result from the node with the later `committed_at`.
3. Performs a **quorum idempotency check** — checks self and all peers for an existing transaction matching the idempotency key.
4. Runs **2PC** to commit the transfer.

This avoids a single point of failure and eliminates leader-election complexity.

### Two-phase commit

Every transfer follows a two-phase protocol:

```
Coordinator                     Peer(s)
    │                               │
    │── WritePending ──────────────►│  Phase 1 (PREPARE)
    │◄─ ACK ────────────────────────│
    │                               │
    │── CommitPending (local) ─────►│  Phase 2 (COMMIT)
    │── Commit (async fanout) ─────►│
```

**Phase 1 — PREPARE**: The coordinator writes a `pending_transactions` row locally and fans out a PREPARE RPC to peers. It waits until W−1 peers ACK before proceeding. If it cannot collect enough ACKs, it rolls back locally and returns `503 Insufficient Quorum`.

**Phase 2 — COMMIT**: The coordinator promotes the pending row into live `transactions` + `entries` under a `FOR UPDATE` lock on the source account (re-validates balance under the lock to serialize concurrent debits). Peer commits are best-effort fire-and-forget; stragglers catch up via resync.

**TTL-based orphan cleanup**: A background ticker runs every second and deletes `pending_transactions` rows whose `expires_at` has passed (default TTL: 5 seconds). This rolls back any PREPARE that lost its coordinator before reaching COMMIT.

The rationale for 2PC over single-phase replication is documented in [`docs/adr/0001-two-phase-commit-for-replication.md`](docs/adr/0001-two-phase-commit-for-replication.md).

### Eager heartbeat partition detection

Each node runs a heartbeat goroutine that fires every **500 ms** and pings all peers with a context timeout equal to the interval. Consecutive failures are counted per peer:

```
misses[peer] < 3   →  peer considered alive
misses[peer] >= 3  →  peer considered dead
```

Three consecutive missed pings — approximately **1.5 seconds** — flip the node's `canWrite` flag. This is an eager approach: the node proactively detects isolation rather than waiting for a write to fail.

### Incremental resync on partition recovery

When the heartbeat transitions from `canWrite=false` to `canWrite=true` it triggers a **blocking resync** before allowing new writes:

```go
// heartbeat.go
if nowWritable && !wasWritable {
    resync(ctx, firstReachablePeer)  // must complete before SetCanWrite(true)
}
```

The recovering node asks a peer for all accounts and transactions committed after its own `MAX(committed_at)`. The peer returns a `SyncPayload` and the node applies it locally. Only after a successful resync does `canWrite` flip to true.

This guarantees that a rejoining node never serves stale data on its next read. The design is described in [`docs/adr/0002-incremental-resync-on-partition-recovery.md`](docs/adr/0002-incremental-resync-on-partition-recovery.md).

### Double-entry accounting

Every transfer produces exactly two `entries` rows inside one database transaction:

| Type   | Account        | Amount   |
|--------|----------------|----------|
| DEBIT  | `from_account` | +amount  |
| CREDIT | `to_account`   | +amount  |

The account balance is derived at read time: `SUM(amount) WHERE type=CREDIT` − `SUM(amount) WHERE type=DEBIT`. There are no mutable balance columns — the ledger is append-only. All amounts are stored as **int64 cents** to eliminate floating-point rounding errors.

### Idempotency and safe retries

Every transfer carries a client-supplied `idempotency_key`. Before committing, the coordinator performs a quorum idempotency check: if any node has already committed a transaction for that key, the existing transaction is returned immediately and the new request is a no-op. The check spans at least W nodes so it cannot miss a commit that satisfied quorum.

---

## Project structure

```
.
├── cmd/node/main.go               Entry point: wires all components, handles signals
├── internal/
│   ├── config/config.go           Reads NODE_ID, DATABASE_URL, PEER_ADDRS, etc.
│   ├── domain/types.go            Core types (Account, Entry, Transaction, Transfer …)
│   │                              and sentinel errors (ErrMinorityPartition, …)
│   ├── store/
│   │   ├── schema.sql             accounts, transactions, entries, pending_transactions
│   │   └── postgres.go            All DB operations (CommitPending uses FOR UPDATE)
│   ├── cluster/
│   │   ├── state.go               canWrite flag + per-peer miss counter (thread-safe)
│   │   ├── heartbeat.go           500ms ticker, miss accounting, resync on recovery
│   │   ├── coordinator.go         Quorum reads, 2PC orchestration, account creation
│   │   └── peer.go                HTTP client wrappers for all inter-node RPCs
│   └── api/
│       ├── server.go              Route registration, HTTP server configuration
│       └── handlers.go            Public + internal handlers, domain error mapping
├── test/integration/
│   └── cluster_test.go            End-to-end partition/resync scenario (iptables-based)
├── docs/adr/
│   ├── 0001-two-phase-commit-for-replication.md
│   └── 0002-incremental-resync-on-partition-recovery.md
├── Dockerfile                     Multi-stage build; final image: alpine + iptables
├── docker-compose.yml             3 nodes + 3 postgres instances, two network tiers
└── Makefile                       up / down / test-integration / demo-partition / demo-heal
```

---

## API reference

### Public API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/accounts` | Create an account. Body: `{"name": "Alice", "currency": "USD"}` |
| `GET`  | `/accounts/{id}` | Fetch an account by ID |
| `GET`  | `/accounts/{id}/balance` | Quorum-read the current balance |
| `POST` | `/transfers` | Execute a transfer (see below) |
| `GET`  | `/transfers/{idempotency_key}` | Look up a committed transfer |
| `GET`  | `/health` | Returns `{"can_write": true/false}` |

**Transfer request body:**
```json
{
  "idempotency_key": "pay-2024-001",
  "from_account_id": "<uuid>",
  "to_account_id":   "<uuid>",
  "amount_cents":    5000,
  "currency":        "USD"
}
```

**HTTP status codes:**

| Code | Meaning |
|------|---------|
| `201` | Created — transfer committed to quorum |
| `422` | Insufficient funds |
| `503` | Node is in minority partition (`ErrMinorityPartition`) or could not reach write quorum (`ErrInsufficientQuorum`) |
| `404` | Account or transfer not found |

### Internal node-to-node API

These endpoints are used exclusively for cluster communication and are not part of the public contract.

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/internal/ping` | Heartbeat liveness probe |
| `GET`  | `/internal/balance/{accountID}` | Peer balance read |
| `GET`  | `/internal/idempotency/{key}` | Peer idempotency check |
| `POST` | `/internal/prepare` | 2PC Phase 1: write pending transaction |
| `POST` | `/internal/commit` | 2PC Phase 2: commit pending transaction |
| `POST` | `/internal/rollback` | Roll back a pending transaction |
| `POST` | `/internal/accounts` | Replicate an account (best-effort) |
| `GET`  | `/internal/sync?after=<RFC3339>` | Return all accounts + transactions after timestamp |

---

## Getting started

**Prerequisites:** Docker Desktop, Go 1.22+, `make`.

```bash
# Clone and start the cluster
git clone https://github.com/riorhezaharris/bank-ledger
cd bank-ledger
make up
```

All three nodes are now running. Wait ~5 seconds for the postgres health checks and the first heartbeat round to complete, then verify:

```bash
make health
# === node1 === {"can_write":true}
# === node2 === {"can_write":true}
# === node3 === {"can_write":true}
```

Create two accounts and make a transfer:

```bash
# Create accounts
ALICE=$(curl -s -X POST http://localhost:8081/accounts \
  -H "Content-Type: application/json" \
  -d '{"name":"Alice","currency":"USD"}' | jq -r .id)

BOB=$(curl -s -X POST http://localhost:8081/accounts \
  -H "Content-Type: application/json" \
  -d '{"name":"Bob","currency":"USD"}' | jq -r .id)

# Transfer $50 from Alice to Bob
curl -s -X POST http://localhost:8081/transfers \
  -H "Content-Type: application/json" \
  -d "{
    \"idempotency_key\": \"demo-1\",
    \"from_account_id\": \"$ALICE\",
    \"to_account_id\":   \"$BOB\",
    \"amount_cents\":    5000,
    \"currency\":        \"USD\"
  }" | jq .
```

Stop the cluster:

```bash
make down
```

---

## Running the integration tests

The integration test suite requires a running cluster and uses `//go:build integration` to stay out of the default `go test ./...` run.

```bash
make up
# wait ~10s for all nodes to reach canWrite=true
make test-integration
```

Expected output:

```
=== RUN   TestAllNodesHealthy
--- PASS: TestAllNodesHealthy (0.01s)
=== RUN   TestPartitionScenario
=== RUN   TestPartitionScenario/partition_detected
--- PASS: TestPartitionScenario/partition_detected (1.52s)
=== RUN   TestPartitionScenario/isolated_node_rejects_writes
--- PASS: TestPartitionScenario/isolated_node_rejects_writes (0.00s)
=== RUN   TestPartitionScenario/majority_accepts_writes
--- PASS: TestPartitionScenario/majority_accepts_writes (0.02s)
=== RUN   TestPartitionScenario/create_canary_during_partition
--- PASS: TestPartitionScenario/create_canary_during_partition (0.00s)
=== RUN   TestPartitionScenario/node_rejoins_quorum_after_heal
--- PASS: TestPartitionScenario/node_rejoins_quorum_after_heal (0.62s)
=== RUN   TestPartitionScenario/node_resynced_canary_account_visible
--- PASS: TestPartitionScenario/node_resynced_canary_account_visible (0.00s)
=== RUN   TestPartitionScenario/node_accepts_new_writes_after_resync
--- PASS: TestPartitionScenario/node_accepts_new_writes_after_resync (0.01s)
--- PASS: TestPartitionScenario (4.21s)
PASS
```

**What the `TestPartitionScenario` test proves:**

| Subtest | What it proves |
|---------|---------------|
| `partition_detected` | Heartbeat correctly flips `canWrite=false` after 3 missed pings (~1.5s) |
| `isolated_node_rejects_writes` | Minority node returns 503 — no data accepted while quorum is lost |
| `majority_accepts_writes` | The two remaining nodes continue to operate normally |
| `create_canary_during_partition` | Data written during the partition is committed to the majority |
| `node_rejoins_quorum_after_heal` | Node detects recovery and completes resync before rejoining |
| `node_resynced_canary_account_visible` | Data that was missed during isolation is visible after resync |
| `node_accepts_new_writes_after_resync` | Fully recovered node accepts new writes |

**How the partition is simulated:**
The test uses `iptables` DROP rules inside node3's network namespace rather than Docker network disconnects. This surgically blocks only cluster traffic (node3 ↔ node1/node2) while leaving the host→node port-publishing DNAT rules intact. The container stays reachable at `localhost:8083` throughout, allowing the test to directly observe the 503 response from the isolated node.

---

## Live partition demo

You can run the partition/heal cycle by hand to observe the cluster behavior in real time.

**Terminal 1 — watch the logs:**
```bash
make logs
```

**Terminal 2 — run the demo:**
```bash
# Step 1: isolate node3
make demo-partition
# node3 logs: "[node3] canWrite=false — MINORITY PARTITION DETECTED"

# Step 2: confirm node3 rejects writes
curl -s -X POST http://localhost:8083/accounts \
  -H "Content-Type: application/json" \
  -d '{"name":"ShouldFail","currency":"USD"}'
# {"error":"node is in minority partition — write rejected"}

# Step 3: majority still works
curl -s http://localhost:8081/health  # {"can_write":true}
curl -s http://localhost:8082/health  # {"can_write":true}
curl -s http://localhost:8083/health  # {"can_write":false}

# Step 4: heal — node3 resyncs and rejoins
make demo-heal
# node3 logs: "[node3] partition healed — resyncing before rejoining quorum"
# node3 logs: "[node3] resync complete — rejoining quorum"
# node3 logs: "[node3] canWrite=true  (reachable=3/3)"

# Step 5: all nodes healthy again
make health
```
