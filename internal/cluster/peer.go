package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/riorhezaharris/bank-ledger/internal/domain"
)

type PeerClient struct {
	Addr   string
	client *http.Client
}

func NewPeerClients(addrs []string) []*PeerClient {
	peers := make([]*PeerClient, len(addrs))
	for i, addr := range addrs {
		peers[i] = &PeerClient{
			Addr:   addr,
			client: &http.Client{Timeout: 2 * time.Second},
		}
	}
	return peers
}

func (p *PeerClient) Ping(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.Addr+"/internal/ping", nil)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping returned %d", resp.StatusCode)
	}
	return nil
}

func (p *PeerClient) GetBalance(ctx context.Context, accountID string) (domain.BalanceResult, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		p.Addr+"/internal/balance/"+accountID, nil)
	resp, err := p.client.Do(req)
	if err != nil {
		return domain.BalanceResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return domain.BalanceResult{}, domain.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return domain.BalanceResult{}, fmt.Errorf("get balance returned %d", resp.StatusCode)
	}
	var result domain.BalanceResult
	return result, json.NewDecoder(resp.Body).Decode(&result)
}

func (p *PeerClient) GetTransactionByIdempotencyKey(ctx context.Context, key string) (*domain.Transaction, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		p.Addr+"/internal/idempotency/"+url.PathEscape(key), nil)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get idempotency returned %d", resp.StatusCode)
	}
	var t domain.Transaction
	return &t, json.NewDecoder(resp.Body).Decode(&t)
}

func (p *PeerClient) Prepare(ctx context.Context, pending domain.PendingTransaction) error {
	return p.postJSON(ctx, "/internal/prepare", pending)
}

func (p *PeerClient) Commit(ctx context.Context, txID string) error {
	return p.postJSON(ctx, "/internal/commit", map[string]string{"transaction_id": txID})
}

func (p *PeerClient) Rollback(ctx context.Context, txID string) error {
	return p.postJSON(ctx, "/internal/rollback", map[string]string{"transaction_id": txID})
}

func (p *PeerClient) ReplicateAccount(ctx context.Context, a domain.Account) error {
	return p.postJSON(ctx, "/internal/accounts", a)
}

func (p *PeerClient) GetSyncPayload(ctx context.Context, after time.Time) (domain.SyncPayload, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		p.Addr+"/internal/sync?after="+url.QueryEscape(after.Format(time.RFC3339Nano)), nil)
	resp, err := p.client.Do(req)
	if err != nil {
		return domain.SyncPayload{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return domain.SyncPayload{}, fmt.Errorf("sync returned %d", resp.StatusCode)
	}
	var payload domain.SyncPayload
	return payload, json.NewDecoder(resp.Body).Decode(&payload)
}

func (p *PeerClient) postJSON(ctx context.Context, path string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		p.Addr+path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %d", path, resp.StatusCode)
	}
	return nil
}
