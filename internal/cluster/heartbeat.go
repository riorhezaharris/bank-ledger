package cluster

import (
	"context"
	"log"
	"time"

	"github.com/riorhezaharris/bank-ledger/internal/domain"
)

type syncer interface {
	GetLastCommittedAt(ctx context.Context) (time.Time, error)
	ApplyTransaction(ctx context.Context, t domain.Transaction) error
	CreateAccount(ctx context.Context, a domain.Account) error
}

type Heartbeat struct {
	nodeID   string
	peers    []*PeerClient
	state    *State
	store    syncer
	interval time.Duration
}

func NewHeartbeat(nodeID string, peers []*PeerClient, state *State, store syncer, interval time.Duration) *Heartbeat {
	return &Heartbeat{
		nodeID:   nodeID,
		peers:    peers,
		state:    state,
		store:    store,
		interval: interval,
	}
}

func (h *Heartbeat) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.beat(ctx)
		}
	}
}

func (h *Heartbeat) beat(ctx context.Context) {
	peerAddrs := make([]string, len(h.peers))
	for i, p := range h.peers {
		peerAddrs[i] = p.Addr
	}

	var reachable []*PeerClient
	for _, peer := range h.peers {
		pingCtx, cancel := context.WithTimeout(ctx, h.interval)
		err := peer.Ping(pingCtx)
		cancel()

		rejoined := h.state.RecordPing(peer.Addr, err == nil)
		if err == nil {
			reachable = append(reachable, peer)
		}
		_ = rejoined
	}

	wasWritable := h.state.CanWrite()
	nowWritable := h.state.ReachableCount(peerAddrs) >= h.state.writeQuorum

	if nowWritable && !wasWritable && len(reachable) > 0 {
		log.Printf("[%s] partition healed — resyncing before rejoining quorum", h.nodeID)
		if err := h.resync(ctx, reachable[0]); err != nil {
			log.Printf("[%s] resync failed: %v — staying offline", h.nodeID, err)
			return
		}
		log.Printf("[%s] resync complete — rejoining quorum", h.nodeID)
	}

	if nowWritable != wasWritable {
		h.state.SetCanWrite(nowWritable)
		if nowWritable {
			log.Printf("[%s] canWrite=true  (reachable=%d/%d)", h.nodeID, h.state.ReachableCount(peerAddrs), len(h.peers)+1)
		} else {
			log.Printf("[%s] canWrite=false — MINORITY PARTITION DETECTED", h.nodeID)
		}
	}
}

func (h *Heartbeat) resync(ctx context.Context, peer *PeerClient) error {
	lastAt, err := h.store.GetLastCommittedAt(ctx)
	if err != nil {
		return err
	}

	payload, err := peer.GetSyncPayload(ctx, lastAt)
	if err != nil {
		return err
	}

	for _, a := range payload.Accounts {
		if err := h.store.CreateAccount(ctx, a); err != nil {
			log.Printf("[%s] resync account %s: %v", h.nodeID, a.ID, err)
		}
	}

	for _, t := range payload.Transactions {
		if err := h.store.ApplyTransaction(ctx, t); err != nil {
			return err
		}
	}

	return nil
}
