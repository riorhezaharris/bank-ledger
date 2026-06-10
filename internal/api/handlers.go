package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/riorhezaharris/bank-ledger/internal/cluster"
	"github.com/riorhezaharris/bank-ledger/internal/domain"
	"github.com/riorhezaharris/bank-ledger/internal/store"
)

type Handler struct {
	coord *cluster.Coordinator
	store *store.Store
	state *cluster.State
}

// --- Public API ---

func (h *Handler) createAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		Currency string `json:"currency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}

	a, err := h.coord.CreateAccount(r.Context(), req.Name, req.Currency)
	if err != nil {
		handleDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, a)
}

func (h *Handler) getAccount(w http.ResponseWriter, r *http.Request) {
	a, err := h.store.GetAccount(r.Context(), r.PathValue("id"))
	if err != nil {
		handleDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (h *Handler) getBalance(w http.ResponseWriter, r *http.Request) {
	if !h.state.CanWrite() {
		writeError(w, http.StatusServiceUnavailable, domain.ErrMinorityPartition.Error())
		return
	}
	accountID := r.PathValue("id")
	balance, err := h.store.GetBalance(r.Context(), accountID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get balance")
		return
	}
	writeJSON(w, http.StatusOK, balance)
}

func (h *Handler) createTransfer(w http.ResponseWriter, r *http.Request) {
	var t domain.Transfer
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if t.IdempotencyKey == "" || t.FromAccountID == "" || t.ToAccountID == "" || t.AmountCents <= 0 {
		writeError(w, http.StatusBadRequest, "idempotency_key, from_account_id, to_account_id, and amount_cents > 0 are required")
		return
	}

	committed, err := h.coord.ExecuteTransfer(r.Context(), t)
	if err != nil {
		handleDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, committed)
}

func (h *Handler) getTransfer(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("id")
	t, err := h.store.GetTransactionByIdempotencyKey(r.Context(), key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get transfer")
		return
	}
	if t == nil {
		writeError(w, http.StatusNotFound, "transfer not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"can_write": h.state.CanWrite(),
	})
}

// --- Internal node-to-node API ---

func (h *Handler) ping(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) internalBalance(w http.ResponseWriter, r *http.Request) {
	accountID := r.PathValue("accountID")
	balance, err := h.store.GetBalance(r.Context(), accountID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, balance)
}

func (h *Handler) internalIdempotency(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	t, err := h.store.GetTransactionByIdempotencyKey(r.Context(), key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if t == nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *Handler) internalPrepare(w http.ResponseWriter, r *http.Request) {
	var p domain.PendingTransaction
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if err := h.store.WritePending(r.Context(), p); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) internalCommit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TransactionID string `json:"transaction_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if _, err := h.store.CommitPending(r.Context(), req.TransactionID); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "pending transaction not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "committed"})
}

func (h *Handler) internalRollback(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TransactionID string `json:"transaction_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if err := h.store.RollbackPending(r.Context(), req.TransactionID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rolled_back"})
}

func (h *Handler) internalAccount(w http.ResponseWriter, r *http.Request) {
	var a domain.Account
	if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if err := h.store.CreateAccount(r.Context(), a); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) internalSync(w http.ResponseWriter, r *http.Request) {
	afterStr := r.URL.Query().Get("after")
	after, err := time.Parse(time.RFC3339Nano, afterStr)
	if err != nil {
		after = time.Time{}
	}

	accounts, err := h.store.GetAccountsAfter(r.Context(), after)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	txns, err := h.store.GetTransactionsAfter(r.Context(), after)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, domain.SyncPayload{
		Accounts:     accounts,
		Transactions: txns,
	})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func handleDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrMinorityPartition):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, domain.ErrInsufficientFunds):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, domain.ErrInsufficientQuorum):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}
