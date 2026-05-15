# Grill Session Q&A: Local-First Relational DB with Deterministic Offline Replication

Date: 2026-05-16  
Participants: AI interviewer, Human project owner  

---

## Overview

This document is a complete record of a grill-session interview used to resolve ambiguities, dependencies, and hidden conflicts in the project plan for a local-first relational database with deterministic offline replication. Decisions captured here supersede any general PRD language that conflicts with specifics stated herein.

---

## Q1: Is the developer experience raw SQL with manual metadata columns, or is there a transparent middleware?

**GUESS:** The PRD exposed internal replication storage too directly. The intended developer experience is probably normal SQLite SQL with metadata injected automatically by a thin middleware layer.

**ANSWER:** Correct. The developer executes normal SQL:

```sql
UPDATE users SET name='Alicia' WHERE id='u1';
```

The middleware expands this internally to:

```sql
UPDATE users
SET name='Alicia', name_ts=next_logical_clock(), name_peer=current_peer()
WHERE id='u1';
```

Metadata columns (`*_ts`, `*_peer`, `tombstone`, `delete_ts`, `conflicted`) are implementation details.

**✅ DECISION:** Metadata injection is transparent. Public API is raw SQL.

---

## Q2: If the system does NOT implement a SQL parser (per PRD), how does the middleware intercept and rewrite arbitrary SQL write statements?

**GUESS:** The public API is a method like `db.execute("UPDATE ...")` that does a lightweight rewrite of known statement types (INSERT, UPDATE, DELETE) via simple regexes.

**ANSWER:** Correct. A lightweight middleware layer intercepts supported write statements and injects replication metadata automatically. Unsupported statement patterns are not guaranteed.

**✅ DECISION:** Lightweight regex/template rewriting for known patterns (INSERT with explicit columns, single-column UPDATE SET, DELETE WHERE id).

---

## Q3: `SELECT *` on the internal table leaks metadata columns. How is this handled?

**GUESS:** Accept the leakage for now and later provide a Python row factory or dict wrapper that strips metadata columns.

**ANSWER:** Accept the leakage for now.

**✅ DECISION:** Internal columns are visible on raw `SELECT *`. The snapshot/observable path will strip them deliberately for the benchmark.

---

## Q4: How does a peer know which rows to send during sync? Is it change-log-based or full-state transfer?

**GUESS:** Option (b) full state transfer because datasets are small and O(n) per sync is acceptable for a hackathon.

**ANSWER:** Option B — full state transfer.

**✅ DECISION:** Send all rows from all registered tables on every sync.

---

## Q5: How is schema replication handled? Do peers replicate DDL changes or assume pre-registered identical schemas?

**GUESS:** Schema is static and identical across peers, managed by a shared init script.

**ANSWER:** Schema is not assumed static. The sync payload includes a lightweight JSON schema manifest. Changes allowed: table creation only. Deletion of tables, and adding/removing columns, are **not** handled. Conflicting table names throw an error.

**✅ DECISION:** Lightweight schema included every sync; creation of new tables supported; table deletion and column add/remove not supported; name collision → error.

---

## Q6: If schema payload is raw `CREATE TABLE` SQL, how does the receiver inject its own metadata columns without parsing DDL?

**GUESS:** Structured JSON representation instead of raw SQL, so receiver middleware generates internal DDL transparently.

**ANSWER:** Correct — JSON in the sync payload instead of raw DDL.

**✅ DECISION:** Schema payload is JSON (table name + public columns + types), not raw `CREATE TABLE`.

---

## Q7: (After user correction) The schema is in the sync payload every time. Is this per-sync or a separate handshake?

**GUESS:** Lightweight schema manifest inside every sync payload.

**ANSWER:** Yes.

**✅ DECISION:** Every sync packet includes a schema header for validation and auto-creation of missing tables.

---

## Q8: How does the middleware distinguish application tables to replicate from internal SQLite tables or scratch tables?

**GUESS:** Must be explicitly registered with the middleware, not auto-discovered.

**ANSWER:** Correct — tables are explicitly registered.

**✅ DECISION:** `register_replicated_table()` API. Only registered tables are synced.

---

## Q9: How does `sync(peer_a, peer_b)` work mechanically? Is it in-process? Networked? Bidirectional in one round-trip?

**GUESS:** In-process; two unidirectional passes for simplicity.

**ANSWER:** Correct.

**✅ DECISION:** In-process pairwise sync. Exchange Python dicts directly. `sync(a, b)` first merges `B→A`, then `A→B`.

---

## Q10: When Peer A creates a new table and syncs, what does the schema payload look like and how is it materialized?

**GUESS:** Structured JSON representation so receiver middleware generates internal DDL.

**ANSWER:** JSON in sync payload. Error on conflicting table names.

**✅ DECISION:** See Q6 and Q7.

---

## Q11 (Critical): How does the adapter handle DDL from the benchmark when native SQLite constraints conflict with replication semantics?

**CONTEXT:** Benchmark sends `UNIQUE(email)` and `ON DELETE CASCADE` via `apply_schema`. Offline replication needs to allow duplicate emails temporarily, and tombstone deletes instead of cascading.

**GUESS:** Option A: execute raw DDL on temp SQLite, introspect with PRAGMA, regenerate internal DDL without conflicting constraints.

**ANSWER:** Option A.

**✅ DECISION:**
- Raw DDL executed on temporary connection for introspection.
- Internal DDL strips `UNIQUE` constraints (enforced later by post-sync arbitration).
- Internal DDL strips `ON DELETE CASCADE` (FK references kept but cascades removed).
- Metadata columns appended after public columns.
- Same table name used.

---

## Q12: How does the middleware extract column lists from raw SQL to inject metadata pairs and extra params?

**GUESS:** Lightweight regex/template rewriter that handles exactly the patterns the benchmark uses: INSERT with explicit columns, single-column UPDATE SET, DELETE WHERE id.

**ANSWER:** Correct.

**✅ DECISION:** Regex-based rewriter for known patterns. Since benchmark SQL is regular, this is sufficient.

---

## Q13: What is the generated internal DDL for the reference schema?

**GUESS:** Public columns first, then `_ts`/`_peer` pairs per mutable column, then `tombstone`, `delete_ts`, `conflicted`.

**ANSWER:** Yes.

**✅ DECISION:** Internal DDL: `id, email, name, email_ts, email_peer, name_ts, name_peer, tombstone, delete_ts, conflicted`.

---

## Q14: How does the middleware handle DELETE statements given tombstones?

**GUESS:** Rewritten to UPDATE that sets tombstone=1 and delete_ts.

**ANSWER:** Correct.

**✅ DECISION:** DELETE rewritten to `UPDATE ... SET tombstone=1, delete_ts=? WHERE id=?`.

---

## Q15: What happens when a user issues an UPDATE on a row that was previously tombstoned locally?

**GUESS:** Allowed; cell metadata updates but tombstone remains unless new timestamp is greater than delete_ts.

**ANSWER:** Allowed.

**✅ DECISION:** Update allowed on tombstoned rows. Cell metadata updates. If incoming/new `ts > delete_ts`, tombstone is CLEARED → row becomes visible again.

---

## Q16: How and when does the adapter detect and resolve uniqueness conflicts (`users.email` duplicates)?

**GUESS:** Post-sync scan: after merges complete, query for duplicate emails, arbitrate by `(lowest timestamp, lowest peer_id)`, mark loser `conflicted=1`.

**ANSWER:** Post-sync is fine.

**✅ DECISION:** Post-sync uniqueness scan per declared unique column. Loser marked `conflicted=1`. Local enforcement deferred if needed.

---

## Q17 (Related): What happens to rows marked `conflicted=1` in `snapshot_state()`?

**GUESS:** Hidden entirely to satisfy uniqueness assertions.

**ANSWER:** Yes.

**✅ DECISION:** `conflicted=1` rows excluded from `snapshot_state()` output. `snapshot_hash()` therefore only includes live, non-conflicted, non-tombstoned rows.

---

## Q18: Do PRIMARY KEY columns get metadata columns (`id_ts`, `id_peer`)?

**GUESS:** Exclude PK metadata for simplicity.

**ANSWER:** Exclude PK columns from metadata.

**✅ DECISION:** PK columns do not get `_ts`/`_peer` metadata. Metadata only for mutable columns. Schema introspection uses `PRAGMA table_info()` to distinguish PK columns.

---

## Q19: How does `snapshot_hash()` guarantee determinism?

**GUESS:** Explicit `SELECT <public_cols> ... ORDER BY id` per table, build dicts, `json.dumps(state, sort_keys=True, default=str)` then SHA256.

**ANSWER:** Correct.

**✅ DECISION:** `snapshot_state()` selects only public columns (stripping metadata), filters `tombstone=0 AND conflicted=0`, orders by PK. `snapshot_hash()` uses `json.dumps(state, sort_keys=True, default=str) + sha256`.

---

## Q20: Does `apply_schema` handle CREATE INDEX statements?

**GUESS:** Pass through directly.

**ANSWER:** Yes.

**✅ DECISION:** `CREATE INDEX` passes through to SQLite unmodified. Only `CREATE TABLE` is intercepted and rewritten.

---

## Q21: What is the scope of the single-column UPDATE rewriter?

**GUESS:** Single-column SET only (sufficient for benchmark); multi-column SET deferred unless needed.

**ANSWER:** Single-column is fine.

**✅ DECISION:** UPDATE rewriter handles single-column `SET col = ?` patterns generated by the benchmark. Multi-column SET is out of current scope.

---

## Q22: How does the snapshot projection know which columns are public?

**GUESS:** Store original public column list during `apply_schema` introspection.

**ANSWER:** Correct.

**✅ DECISION:** `self.public_columns[peer_id][table_name]` and `self.pk_columns[peer_id][table_name]` stored from `PRAGMA table_info()` during schema application.

---

## Q23: How is logical time incremented?

**GUESS:** `self.clocks[peer_id]` starts at 0, increments by 1 before every local metadata-tagged write. All cells in one statement get the same timestamp.

**ANSWER:** Accepted.

**✅ DECISION:** `self.clocks = {peer_id: 0}` on `open_peer`. Before every local metadata injection, `+= 1`. Single timestamp per statement.

---

## Final Summary of Design Decisions

1. **Public API:** Raw SQL strings; metadata injection transparent.
2. **SQL Rewriting:** Lightweight regex for INSERT (explicit columns), single-column UPDATE, DELETE WHERE id. SELECT passthrough.
3. **Metadata Leakage:** Accepted; raw `SELECT *` shows internal columns.
4. **Sync Model:** Full state transfer; in-process Python dict exchange; unidirectional passes.
5. **Schema Sync:** JSON table manifest per sync; table creation supported; deletions/column changes unsupported; name collision → error.
6. **Table Registration:** Explicit `register_replicated_table()`. No auto-discovery.
7. **DDL Handling:** Option A — temp SQLite introspection → internal DDL regenerated with metadata columns.
8. **Constraint Stripping:** `UNIQUE` removed for internal tables; `ON DELETE CASCADE` stripped; FK references kept.
9. **Internal DDL Pattern:** Public columns → `_ts`/`_peer` pairs per mutable column → row-level `tombstone`, `delete_ts`, `conflicted`.
10. **DELETE Semantics:** Rewritten to tombstone UPDATE.
11. **Update on Tombstoned Row:** Allowed. Cell metadata updates. If `ts > delete_ts`, tombstone is cleared and row becomes visible.
12. **Uniqueness Conflicts:** Post-sync scan per declared unique column. Arbitration: `(lowest timestamp, lowest peer_id)`. Loser marked `conflicted=1`.
13. **PK Metadata:** PK columns excluded from metadata pairs.
14. **Snapshot Projection:** Explicit public column SELECT. Filters tombstones and conflicted rows. Orders by PK.
15. **Hash Determinism:** `json.dumps(sort_keys=True, default=str)` + SHA256.
16. **Index Passthrough:** `CREATE INDEX` unmodified.
