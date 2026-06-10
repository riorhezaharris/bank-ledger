# Bank Ledger — Domain Glossary

## Ledger
A double-entry accounting system that records money movements. Every movement produces exactly two entries: one debit and one credit. The invariant `Σ debits == Σ credits` must hold at all times. A node that cannot guarantee this invariant must reject writes.

## Account
A named entity that holds a balance. Balance is always derived from entries — never stored directly.

## Entry
A single line in the ledger: `(account, type, amount)` where `type` is either DEBIT or CREDIT. Entries are never created alone; they always come in pairs.

## Transaction
The atomic unit of a money movement. Contains exactly two entries: one DEBIT and one CREDIT. Either both entries are persisted or neither is.

## Transfer
The API-level request to move money between two accounts. A Transfer produces a Transaction when committed. A Transfer carries an idempotency_key — if a Transaction with that key already exists, the original result is returned without re-executing. A Transfer has a lifecycle (e.g. PENDING → COMMITTED or REJECTED); a Transaction, once written, is immutable.

## Node
A single instance of the ledger service. The cluster runs exactly 3 nodes. Each node has its own independent storage, can coordinate writes, and participates in heartbeat-based quorum health checks. Any node can act as coordinator for any request.

## Coordinator
The node that receives a client write request and is responsible for fanning it out to peers, collecting W=2 acknowledgments, and committing or rejecting the Transaction. The coordinator role is per-request, not a fixed designation.

## Quorum
The minimum number of nodes that must agree for an operation to succeed. This cluster uses N=3, W=2, R=2. Since R+W > N (4 > 3), every read overlaps with every write, guaranteeing strong consistency. A node that cannot reach a quorum of W=2 peers must immediately reject writes.

## Pending Transaction
A Transaction staged on a peer node during Phase 1 of 2PC. Stored in a `pending_transactions` table. Promoted to the live ledger on COMMIT, or rolled back when its TTL expires (5 seconds). A Pending Transaction is never visible to clients.

## Sequence Number (seq)
A monotonic `int64` assigned to every committed Transaction by the coordinator. Used for incremental resync: a rejoining node fetches all Transactions with `seq > N` from a reachable peer.

## Heartbeat
A periodic ping (every 500ms) sent from each node to its peers. A node that fails to receive 3 consecutive heartbeats from a peer marks that peer as unreachable. If a node cannot confirm reachability of W=2 total nodes (including itself), it sets `canWrite = false` and rejects all writes immediately.
