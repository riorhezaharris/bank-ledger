package store

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riorhezaharris/bank-ledger/internal/domain"
)

//go:embed schema.sql
var schema string

// Pool is re-exported so callers don't need a direct pgx dependency.
type Pool = pgxpool.Pool

type Store struct {
	db *pgxpool.Pool
}

func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

func Migrate(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, schema)
	return err
}

func New(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) CreateAccount(ctx context.Context, a domain.Account) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO accounts (id, name, currency, created_at) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (id) DO NOTHING`,
		a.ID, a.Name, a.Currency, a.CreatedAt)
	return err
}

func (s *Store) GetAccount(ctx context.Context, id string) (domain.Account, error) {
	var a domain.Account
	err := s.db.QueryRow(ctx,
		`SELECT id, name, currency, created_at FROM accounts WHERE id = $1`, id).
		Scan(&a.ID, &a.Name, &a.Currency, &a.CreatedAt)
	if err == pgx.ErrNoRows {
		return domain.Account{}, domain.ErrNotFound
	}
	return a, err
}

func (s *Store) GetAccountsAfter(ctx context.Context, after time.Time) ([]domain.Account, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, name, currency, created_at FROM accounts WHERE created_at > $1 ORDER BY created_at`,
		after.Add(-5*time.Second))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []domain.Account
	for rows.Next() {
		var a domain.Account
		if err := rows.Scan(&a.ID, &a.Name, &a.Currency, &a.CreatedAt); err != nil {
			return nil, err
		}
		accounts = append(accounts, a)
	}
	return accounts, rows.Err()
}

// GetBalance returns the current balance and the committed_at of the latest transaction
// touching this account. If no transactions exist, LastCommittedAt is zero.
func (s *Store) GetBalance(ctx context.Context, accountID string) (domain.BalanceResult, error) {
	var res domain.BalanceResult
	err := s.db.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN e.type = 'CREDIT' THEN e.amount_cents ELSE -e.amount_cents END), 0),
			COALESCE((
				SELECT a.currency FROM accounts a WHERE a.id = $1
			), 'USD'),
			COALESCE(MAX(t.committed_at), '1970-01-01'::timestamptz)
		FROM entries e
		JOIN transactions t ON t.id = e.transaction_id
		WHERE e.account_id = $1
	`, accountID).Scan(&res.BalanceCents, &res.Currency, &res.LastCommittedAt)
	return res, err
}

func (s *Store) GetTransactionByIdempotencyKey(ctx context.Context, key string) (*domain.Transaction, error) {
	var t domain.Transaction
	err := s.db.QueryRow(ctx,
		`SELECT id, idempotency_key, seq, committed_at FROM transactions WHERE idempotency_key = $1`, key).
		Scan(&t.ID, &t.IdempotencyKey, &t.Seq, &t.CommittedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := s.getEntries(ctx, t.ID)
	if err != nil {
		return nil, err
	}
	t.Entries = entries
	return &t, nil
}

func (s *Store) GetLastCommittedAt(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := s.db.QueryRow(ctx,
		`SELECT COALESCE(MAX(committed_at), '1970-01-01'::timestamptz) FROM transactions`).Scan(&t)
	return t, err
}

func (s *Store) GetTransactionsAfter(ctx context.Context, after time.Time) ([]domain.Transaction, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, idempotency_key, seq, committed_at FROM transactions
		 WHERE committed_at > $1 ORDER BY committed_at, seq`,
		after.Add(-5*time.Second))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txns []domain.Transaction
	for rows.Next() {
		var t domain.Transaction
		if err := rows.Scan(&t.ID, &t.IdempotencyKey, &t.Seq, &t.CommittedAt); err != nil {
			return nil, err
		}
		txns = append(txns, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range txns {
		txns[i].Entries, err = s.getEntries(ctx, txns[i].ID)
		if err != nil {
			return nil, err
		}
	}
	return txns, nil
}

func (s *Store) WritePending(ctx context.Context, p domain.PendingTransaction) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO pending_transactions (id, idempotency_key, from_account_id, to_account_id, amount_cents, currency, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (id) DO NOTHING`,
		p.ID, p.IdempotencyKey, p.FromAccountID, p.ToAccountID, p.AmountCents, p.Currency, p.ExpiresAt)
	return err
}

// CommitPending promotes a pending transaction to the live ledger within a single
// serialized DB transaction. It re-validates the balance under a FOR UPDATE lock
// to prevent concurrent debits from overdrafting the account.
func (s *Store) CommitPending(ctx context.Context, txID string) (*domain.Transaction, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var p domain.PendingTransaction
	err = tx.QueryRow(ctx,
		`SELECT id, idempotency_key, from_account_id, to_account_id, amount_cents, currency
		 FROM pending_transactions WHERE id = $1`, txID).
		Scan(&p.ID, &p.IdempotencyKey, &p.FromAccountID, &p.ToAccountID, &p.AmountCents, &p.Currency)
	if err == pgx.ErrNoRows {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get pending: %w", err)
	}

	// Serialize concurrent debits on the same source account.
	if _, err := tx.Exec(ctx, `SELECT id FROM accounts WHERE id = $1 FOR UPDATE`, p.FromAccountID); err != nil {
		return nil, fmt.Errorf("lock account: %w", err)
	}

	var balance int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN e.type = 'CREDIT' THEN e.amount_cents ELSE -e.amount_cents END), 0)
		FROM entries e
		JOIN transactions t ON t.id = e.transaction_id
		WHERE e.account_id = $1
	`, p.FromAccountID).Scan(&balance); err != nil {
		return nil, fmt.Errorf("get balance: %w", err)
	}

	if balance < p.AmountCents {
		return nil, domain.ErrInsufficientFunds
	}

	// Idempotency guard: return existing transaction if already committed.
	var existingID string
	err = tx.QueryRow(ctx,
		`SELECT id FROM transactions WHERE idempotency_key = $1`, p.IdempotencyKey).Scan(&existingID)
	if err == nil {
		_ = tx.Commit(ctx)
		return s.GetTransaction(ctx, existingID)
	}

	var committed domain.Transaction
	if err := tx.QueryRow(ctx,
		`INSERT INTO transactions (id, idempotency_key) VALUES ($1, $2) RETURNING seq, committed_at`,
		p.ID, p.IdempotencyKey).Scan(&committed.Seq, &committed.CommittedAt); err != nil {
		return nil, fmt.Errorf("insert tx: %w", err)
	}
	committed.ID = p.ID
	committed.IdempotencyKey = p.IdempotencyKey

	debitID := uuid.New().String()
	creditID := uuid.New().String()

	if _, err := tx.Exec(ctx,
		`INSERT INTO entries (id, transaction_id, account_id, type, amount_cents, currency)
		 VALUES ($1, $2, $3, 'DEBIT', $4, $5)`,
		debitID, p.ID, p.FromAccountID, p.AmountCents, p.Currency); err != nil {
		return nil, fmt.Errorf("insert debit: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO entries (id, transaction_id, account_id, type, amount_cents, currency)
		 VALUES ($1, $2, $3, 'CREDIT', $4, $5)`,
		creditID, p.ID, p.ToAccountID, p.AmountCents, p.Currency); err != nil {
		return nil, fmt.Errorf("insert credit: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM pending_transactions WHERE id = $1`, txID); err != nil {
		return nil, fmt.Errorf("delete pending: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	committed.Entries = []domain.Entry{
		{ID: debitID, TransactionID: p.ID, AccountID: p.FromAccountID, Type: domain.Debit, AmountCents: p.AmountCents, Currency: p.Currency},
		{ID: creditID, TransactionID: p.ID, AccountID: p.ToAccountID, Type: domain.Credit, AmountCents: p.AmountCents, Currency: p.Currency},
	}
	return &committed, nil
}

func (s *Store) RollbackPending(ctx context.Context, txID string) error {
	_, err := s.db.Exec(ctx, `DELETE FROM pending_transactions WHERE id = $1`, txID)
	return err
}

func (s *Store) CleanupExpiredPending(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `DELETE FROM pending_transactions WHERE expires_at < NOW()`)
	return err
}

// ApplyTransaction inserts a transaction received during resync. Idempotent by transaction ID.
func (s *Store) ApplyTransaction(ctx context.Context, t domain.Transaction) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var exists bool
	_ = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM transactions WHERE id = $1)`, t.ID).Scan(&exists)
	if exists {
		return tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO transactions (id, idempotency_key, committed_at) VALUES ($1, $2, $3)`,
		t.ID, t.IdempotencyKey, t.CommittedAt); err != nil {
		return fmt.Errorf("insert tx: %w", err)
	}

	for _, e := range t.Entries {
		if _, err := tx.Exec(ctx,
			`INSERT INTO entries (id, transaction_id, account_id, type, amount_cents, currency)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (id) DO NOTHING`,
			e.ID, e.TransactionID, e.AccountID, e.Type, e.AmountCents, e.Currency); err != nil {
			return fmt.Errorf("insert entry: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) GetTransaction(ctx context.Context, id string) (*domain.Transaction, error) {
	var t domain.Transaction
	err := s.db.QueryRow(ctx,
		`SELECT id, idempotency_key, seq, committed_at FROM transactions WHERE id = $1`, id).
		Scan(&t.ID, &t.IdempotencyKey, &t.Seq, &t.CommittedAt)
	if err == pgx.ErrNoRows {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t.Entries, err = s.getEntries(ctx, id)
	return &t, err
}

func (s *Store) getEntries(ctx context.Context, txID string) ([]domain.Entry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, transaction_id, account_id, type, amount_cents, currency FROM entries WHERE transaction_id = $1`, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []domain.Entry
	for rows.Next() {
		var e domain.Entry
		if err := rows.Scan(&e.ID, &e.TransactionID, &e.AccountID, &e.Type, &e.AmountCents, &e.Currency); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}
