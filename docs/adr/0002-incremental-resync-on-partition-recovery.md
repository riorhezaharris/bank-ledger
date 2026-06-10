# ADR-0002: Incremental Resync via Sequence Number on Partition Recovery

## Status
Accepted

## Context
When an isolated node rejoins the cluster after a partition, it has missed committed Transactions. Serving reads from a stale node violates the CP contract. The node must catch up before it can rejoin the quorum.

## Decision
Every committed Transaction carries a monotonic `seq int64` column, assigned by the coordinator at commit time. On heartbeat recovery (node detects it can again reach a quorum of peers), the rejoining node:
1. Queries a reachable peer: `GET /internal/transactions?after_seq=N` where N is its own last committed seq.
2. Replays the returned Transactions locally in seq order.
3. Once caught up, flips `canWrite = true` and rejoins the quorum.

An internal resync endpoint (`GET /internal/transactions?after_seq=N`) is exposed on each node for this purpose. It is not part of the public API.

## Consequences
- Every Transaction table requires a `seq int64` column with a unique index.
- The coordinator must assign `seq` atomically at commit time (Postgres `SERIAL` or a dedicated sequence).
- A node that is behind does not serve reads until resync completes — it remains in `canWrite = false` state throughout.
- Simplification accepted: seq is assigned per-coordinator, not globally ordered across concurrent coordinators. In practice with N=3 and infrequent concurrent writes this is acceptable for a portfolio scope.
