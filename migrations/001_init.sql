CREATE TABLE IF NOT EXISTS accounts (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'USD',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT NOT NULL,
    seq              BIGSERIAL,
    committed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS transactions_idempotency_key_idx ON transactions(idempotency_key);
CREATE INDEX IF NOT EXISTS transactions_committed_at_idx ON transactions(committed_at);
CREATE INDEX IF NOT EXISTS transactions_seq_idx ON transactions(seq);

-- No FK constraints: application layer enforces referential integrity.
-- This simplifies cross-node resync (accounts may arrive after entries during replay).
CREATE TABLE IF NOT EXISTS entries (
    id              TEXT PRIMARY KEY,
    transaction_id  TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('DEBIT', 'CREDIT')),
    amount_cents    BIGINT NOT NULL CHECK (amount_cents > 0),
    currency        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS entries_account_id_idx ON entries(account_id);
CREATE INDEX IF NOT EXISTS entries_transaction_id_idx ON entries(transaction_id);

-- Staged records for 2PC Phase 1. Promoted to transactions+entries on COMMIT.
-- Expired records are rolled back by a background goroutine.
CREATE TABLE IF NOT EXISTS pending_transactions (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT NOT NULL,
    from_account_id  TEXT NOT NULL,
    to_account_id    TEXT NOT NULL,
    amount_cents     BIGINT NOT NULL,
    currency         TEXT NOT NULL,
    expires_at       TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
