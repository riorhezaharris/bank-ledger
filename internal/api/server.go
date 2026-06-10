package api

import (
	"context"
	"net/http"
	"time"

	"github.com/riorhezaharris/bank-ledger/internal/cluster"
	"github.com/riorhezaharris/bank-ledger/internal/store"
)

func NewServer(addr string, coord *cluster.Coordinator, st *store.Store, state *cluster.State) *http.Server {
	h := &Handler{coord: coord, store: st, state: state}

	mux := http.NewServeMux()

	// Public API
	mux.HandleFunc("POST /accounts", h.createAccount)
	mux.HandleFunc("GET /accounts/{id}", h.getAccount)
	mux.HandleFunc("GET /accounts/{id}/balance", h.getBalance)
	mux.HandleFunc("POST /transfers", h.createTransfer)
	mux.HandleFunc("GET /transfers/{id}", h.getTransfer)
	mux.HandleFunc("GET /health", h.health)

	// Internal node-to-node API
	mux.HandleFunc("GET /internal/ping", h.ping)
	mux.HandleFunc("GET /internal/balance/{accountID}", h.internalBalance)
	mux.HandleFunc("GET /internal/idempotency/{key}", h.internalIdempotency)
	mux.HandleFunc("POST /internal/prepare", h.internalPrepare)
	mux.HandleFunc("POST /internal/commit", h.internalCommit)
	mux.HandleFunc("POST /internal/rollback", h.internalRollback)
	mux.HandleFunc("POST /internal/accounts", h.internalAccount)
	mux.HandleFunc("GET /internal/sync", h.internalSync)

	return &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

func Shutdown(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
