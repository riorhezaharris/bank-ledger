package cluster

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/riorhezaharris/bank-ledger/internal/domain"
)

type localStore interface {
	GetBalance(ctx context.Context, accountID string) (domain.BalanceResult, error)
	GetTransactionByIdempotencyKey(ctx context.Context, key string) (*domain.Transaction, error)
	WritePending(ctx context.Context, p domain.PendingTransaction) error
	CommitPending(ctx context.Context, txID string) (*domain.Transaction, error)
	RollbackPending(ctx context.Context, txID string) error
	CreateAccount(ctx context.Context, a domain.Account) error
}

type Coordinator struct {
	store      localStore
	state      *State
	peers      []*PeerClient
	pendingTTL time.Duration
}

func NewCoordinator(store localStore, state *State, peers []*PeerClient, pendingTTL time.Duration) *Coordinator {
	return &Coordinator{store: store, state: state, peers: peers, pendingTTL: pendingTTL}
}

func (c *Coordinator) CreateAccount(ctx context.Context, name, currency string) (domain.Account, error) {
	if !c.state.CanWrite() {
		return domain.Account{}, domain.ErrMinorityPartition
	}
	a := domain.Account{
		ID:        uuid.New().String(),
		Name:      name,
		Currency:  currency,
		CreatedAt: time.Now().UTC(),
	}
	if err := c.store.CreateAccount(ctx, a); err != nil {
		return domain.Account{}, err
	}
	// Best-effort replication to peers — stragglers catch up via resync.
	for _, peer := range c.peers {
		go func(p *PeerClient) {
			if err := p.ReplicateAccount(context.Background(), a); err != nil {
				log.Printf("replicate account to %s: %v", p.Addr, err)
			}
		}(peer)
	}
	return a, nil
}

// ExecuteTransfer runs the full CP write path:
//  1. Reject immediately if this node is in minority partition.
//  2. Quorum read: balance + idempotency check across self + W-1 peers.
//  3. 2PC Phase 1 (PREPARE): write pending to self, fan out to peers.
//  4. 2PC Phase 2 (COMMIT): promote pending on self, fan out to peers.
func (c *Coordinator) ExecuteTransfer(ctx context.Context, t domain.Transfer) (*domain.Transaction, error) {
	if !c.state.CanWrite() {
		return nil, domain.ErrMinorityPartition
	}

	// --- Quorum read: balance ---
	balance, err := c.quorumBalance(ctx, t.FromAccountID)
	if err != nil {
		return nil, err
	}
	if balance < t.AmountCents {
		return nil, domain.ErrInsufficientFunds
	}

	// --- Quorum read: idempotency ---
	existing, err := c.quorumIdempotency(ctx, t.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}

	// --- 2PC Phase 1: PREPARE ---
	pending := domain.PendingTransaction{
		ID:             uuid.New().String(),
		IdempotencyKey: t.IdempotencyKey,
		FromAccountID:  t.FromAccountID,
		ToAccountID:    t.ToAccountID,
		AmountCents:    t.AmountCents,
		Currency:       t.Currency,
		ExpiresAt:      time.Now().Add(c.pendingTTL),
	}

	if err := c.store.WritePending(ctx, pending); err != nil {
		return nil, err
	}

	if err := c.fanoutPrepare(ctx, pending); err != nil {
		_ = c.store.RollbackPending(ctx, pending.ID)
		return nil, err
	}

	// --- 2PC Phase 2: COMMIT ---
	committed, err := c.store.CommitPending(ctx, pending.ID)
	if err != nil {
		// Local commit failed (e.g. balance gone by the time we locked).
		// Peers will roll back via TTL expiry.
		return nil, err
	}

	// Best-effort commit to peers — stragglers catch up via resync.
	for _, peer := range c.peers {
		go func(p *PeerClient) {
			if err := p.Commit(context.Background(), pending.ID); err != nil {
				log.Printf("commit to %s: %v (will self-heal via resync)", p.Addr, err)
			}
		}(peer)
	}

	return committed, nil
}

// quorumBalance reads the balance from self and W-1 peers and returns the
// authoritative value (from the node with the latest committed_at).
func (c *Coordinator) quorumBalance(ctx context.Context, accountID string) (int64, error) {
	self, err := c.store.GetBalance(ctx, accountID)
	if err != nil {
		return 0, err
	}

	peer, ok := c.firstPeerBalance(ctx, accountID)
	if !ok {
		return 0, domain.ErrInsufficientQuorum
	}

	if peer.LastCommittedAt.After(self.LastCommittedAt) {
		return peer.BalanceCents, nil
	}
	return self.BalanceCents, nil
}

func (c *Coordinator) firstPeerBalance(ctx context.Context, accountID string) (domain.BalanceResult, bool) {
	for _, peer := range c.peers {
		if b, err := peer.GetBalance(ctx, accountID); err == nil {
			return b, true
		}
	}
	return domain.BalanceResult{}, false
}

// quorumIdempotency checks self and one peer for an existing transaction.
func (c *Coordinator) quorumIdempotency(ctx context.Context, key string) (*domain.Transaction, error) {
	if t, err := c.store.GetTransactionByIdempotencyKey(ctx, key); err == nil && t != nil {
		return t, nil
	}
	for _, peer := range c.peers {
		if t, err := peer.GetTransactionByIdempotencyKey(ctx, key); err == nil && t != nil {
			return t, nil
		}
	}
	return nil, nil
}

// fanoutPrepare sends PREPARE to all peers and waits for W-1 ACKs.
// With W=2 and the coordinator counting as 1, we need 1 peer ACK.
func (c *Coordinator) fanoutPrepare(ctx context.Context, pending domain.PendingTransaction) error {
	needed := c.state.writeQuorum - 1 // W-1 = 1 peer ACK needed

	type result struct{ err error }
	ch := make(chan result, len(c.peers))

	var wg sync.WaitGroup
	for _, peer := range c.peers {
		wg.Add(1)
		go func(p *PeerClient) {
			defer wg.Done()
			ch <- result{err: p.Prepare(ctx, pending)}
		}(peer)
	}
	go func() { wg.Wait(); close(ch) }()

	acks := 0
	for r := range ch {
		if r.err == nil {
			acks++
			if acks >= needed {
				return nil
			}
		}
	}
	return domain.ErrInsufficientQuorum
}
