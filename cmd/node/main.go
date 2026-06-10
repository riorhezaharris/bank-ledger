package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/riorhezaharris/bank-ledger/internal/api"
	"github.com/riorhezaharris/bank-ledger/internal/cluster"
	"github.com/riorhezaharris/bank-ledger/internal/config"
	"github.com/riorhezaharris/bank-ledger/internal/store"
)

func main() {
	cfg := config.Load()
	log.SetPrefix("[" + cfg.NodeID + "] ")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := connectWithRetry(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer db.Close()

	if err := store.Migrate(ctx, db); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	st := store.New(db)
	peers := cluster.NewPeerClients(cfg.PeerAddrs)
	state := cluster.NewState(cfg.NodeID, cfg.WriteQuorum)
	coord := cluster.NewCoordinator(st, state, peers, cfg.PendingTTL)

	hb := cluster.NewHeartbeat(cfg.NodeID, peers, state, st, cfg.HeartbeatInterval)
	go hb.Run(ctx)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := st.CleanupExpiredPending(ctx); err != nil {
					log.Printf("cleanup pending: %v", err)
				}
			}
		}
	}()

	srv := api.NewServer(cfg.NodeAddr, coord, st, state)

	go func() {
		log.Printf("listening on %s", cfg.NodeAddr)
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("server stopped: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	api.Shutdown(srv)
}

func connectWithRetry(ctx context.Context, url string) (*store.Pool, error) {
	for {
		pool, err := store.Connect(ctx, url)
		if err == nil {
			return pool, nil
		}
		log.Printf("db not ready (%v) — retrying in 2s", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
