package domain

import (
	"errors"
	"time"
)

type EntryType string

const (
	Debit  EntryType = "DEBIT"
	Credit EntryType = "CREDIT"
)

var (
	ErrInsufficientFunds  = errors.New("insufficient funds")
	ErrMinorityPartition  = errors.New("node is in minority partition — write rejected")
	ErrInsufficientQuorum = errors.New("failed to reach write quorum")
	ErrNotFound           = errors.New("not found")
	ErrAlreadyExists      = errors.New("already exists")
)

type Account struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Currency  string    `json:"currency"`
	CreatedAt time.Time `json:"created_at"`
}

type Entry struct {
	ID            string    `json:"id"`
	TransactionID string    `json:"transaction_id"`
	AccountID     string    `json:"account_id"`
	Type          EntryType `json:"type"`
	AmountCents   int64     `json:"amount_cents"`
	Currency      string    `json:"currency"`
}

type Transaction struct {
	ID             string    `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Seq            int64     `json:"seq"`
	Entries        []Entry   `json:"entries"`
	CommittedAt    time.Time `json:"committed_at"`
}

type Transfer struct {
	IdempotencyKey string `json:"idempotency_key"`
	FromAccountID  string `json:"from_account_id"`
	ToAccountID    string `json:"to_account_id"`
	AmountCents    int64  `json:"amount_cents"`
	Currency       string `json:"currency"`
}

type PendingTransaction struct {
	ID             string    `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	FromAccountID  string    `json:"from_account_id"`
	ToAccountID    string    `json:"to_account_id"`
	AmountCents    int64     `json:"amount_cents"`
	Currency       string    `json:"currency"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type BalanceResult struct {
	BalanceCents    int64     `json:"balance_cents"`
	Currency        string    `json:"currency"`
	LastCommittedAt time.Time `json:"last_committed_at"`
}

type SyncPayload struct {
	Accounts     []Account     `json:"accounts"`
	Transactions []Transaction `json:"transactions"`
}
