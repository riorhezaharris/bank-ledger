# ADR-0001: Two-Phase Commit for Cross-Node Replication

## Status
Accepted

## Context
The coordinator node fans out writes to peers before acknowledging a client. With single-phase replication, a coordinator crash after writing locally but before peers ACK leaves nodes diverged — one node has the Transaction, others do not. For a bank ledger, this is unacceptable: money cannot appear or disappear due to a crash.

## Decision
Use two-phase commit (2PC) for all writes:
- **Phase 1 (PREPARE):** coordinator sends the Transaction payload to peers; each peer writes it to a `pending_transactions` staging table and ACKs "ready."
- **Phase 2 (COMMIT):** once W=2 "ready" ACKs are received, coordinator broadcasts `COMMIT`; peers promote the pending record to the live ledger.
- If the coordinator crashes before Phase 2, peers time out and roll back the pending record.

## Consequences
- Each node requires a `pending_transactions` staging table in addition to the live ledger tables.
- Write latency increases by one network round-trip (PREPARE → COMMIT).
- The coordinator is a single point of failure within a single request — if it crashes mid-2PC, in-flight Transactions are rolled back (not committed). This is the safe failure mode for a ledger.
- Simplification accepted: we do not implement a recovery coordinator for crashed mid-2PC scenarios. A timed-out PREPARE is always rolled back.
