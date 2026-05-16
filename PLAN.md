# Implementation Plan: CRDT-Native OLTP Adapter

## Overview

Implement a single Python adapter file `bench-p01-crdt/adapters/team.py` that plugs into the Anvil P-01 benchmark harness. The adapter wraps per-peer SQLite databases with transparent metadata injection, state-based sync, and deterministic merge. The plan is vertically sliced so each task produces a testable component.

> **Status:** All tasks complete. Score: 1.0000 / 1.0000 (L3 Final). 65 unit tests passing.

---

## Architecture Decisions

1. **SQLite in-memory per peer**: Each peer gets a `:memory:` connection, managed by a single adapter instance. No on-disk state.
2. **Regex-based SQL rewriting**: INSERT with explicit columns, single- and multi-column UPDATE SET, DELETE WHERE, and bare DELETE are intercepted. All other SQL passes through raw with a warning.
3. **Schema introspection via temp SQLite**: `CREATE TABLE` DDL is executed on a throwaway connection, introspected with `PRAGMA`, then regenerated into internal DDL with metadata columns. Composite UNIQUE constraints are stored as column-groups.
4. **Full state sync**: Every sync exchanges all registered table rows as Python dicts. Simple, correct, and acceptable for small benchmark datasets.
5. **Post-sync uniqueness scan**: Conflicts are detected and resolved after merge, not during write. Uses a two-pass approach: detect all losers across all constraints, then mark `conflicted=1`. Conflicted rows are included in snapshots with mangled unique columns.
6. **FK cascade via triggers**: `ON DELETE CASCADE` is stripped from internal DDL. Dynamic `AFTER UPDATE OF tombstone` triggers propagate tombstones through FK chains.
7. **Remove-Wins tombstone policy**: Deletes are permanent during merge. Resurrection only via explicit local re-INSERT.

---

## Dependency Graph

```
open_peer / close
    │
    ▼
Schema introspection engine
    │
    ├── apply_schema (CREATE TABLE + CREATE INDEX passthrough)
    │
    ├── execute() SQL rewriter
    │       ├── INSERT rewrite
    │       ├── UPDATE rewrite
    │       └── DELETE rewrite (tombstone)
    │
    ├── snapshot_state / snapshot_hash
    │
    ├── State extraction for sync
    │       └── Row merge algorithm
    │
    └── sync() (full: extract → merge → uniqueness scan)
```

---

## Task List

### Phase 1: Foundation

#### Task 1: Adapter Skeleton + Peer Lifecycle

**Description:** Create the adapter class file, implement `open_peer` and `close`, and stub out all required `Adapter` methods so the file is importable and the harness can construct it.

**Acceptance criteria:**
- [ ] File exists at `bench-p01-crdt/adapters/team.py`.
- [ ] Class inherits from `Adapter`.
- [ ] `open_peer` creates an in-memory SQLite connection and stores it keyed by `peer_id`.
- [ ] `close` closes all stored connections.
- [ ] Standalone test: instantiate adapter, open two peers, close adapter, no exceptions.

**Verification:**
- [ ] Manual check: `python -c "from adapters.team import TeamAdapter; a=TeamAdapter(); a.open_peer('A'); a.open_peer('B'); a.close()"` exits 0.

**Dependencies:** None

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py` (create)

**Estimated scope:** XS (single file, stub methods)

---

#### Task 2: Schema Introspection Engine

**Description:** Build a standalone function/class that takes a `CREATE TABLE` SQL string, executes it on a temporary SQLite connection, introspects it with `PRAGMA table_info`, and returns:
1. The internal DDL string (with metadata columns appended, PK columns excluded from metadata, UNIQUE stripped, ON DELETE CASCADE stripped).
2. A list of public column names.
3. A list of PK column names.
4. A list of unique constraints as column-groups (e.g., `[('email',)]` or `[('user_id', 'team_id')]`).
5. A list of FK cascade triggers (dynamically generated for `ON DELETE CASCADE` relationships).

**Acceptance criteria:**
- [ ] Introspection correctly identifies PK columns vs. mutable columns.
- [ ] Generated internal DDL for the reference `users` table matches the spec exactly (column order, types, metadata pairs, tombstone/DELETE columns).
- [ ] `UNIQUE(email)` is stripped from internal DDL.
- [ ] `ON DELETE CASCADE` is stripped from `orders` internal DDL but FK reference remains.
- [ ] `CREATE INDEX` statements pass through unmodified.

**Verification:**
- [ ] Manual check: run introspection on reference schema and print generated DDL; verify by eye and by executing generated DDL on a temp connection.

**Dependencies:** None (standalone utility)

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py` (add introspection method)

**Estimated scope:** S–M (1 file, non-trivial logic)

---

### Checkpoint: Foundation Complete

- [ ] All stub methods present.
- [ ] Schema introspection produces correct DDL for reference schema.
- [ ] Ready for write-path implementation.

---

### Phase 2: Write Path

#### Task 3: apply_schema — DDL Interception + Storage

**Description:** Wire `apply_schema` to use the introspection engine. For each incoming statement:
- If `CREATE TABLE`: introspect, generate internal DDL, execute on the peer, store `public_columns`, `pk_columns`, `unique_columns`, and remember this table as a "registered replication table."
- If `CREATE INDEX`: execute directly on the peer connection.
- Reject or ignore unsupported DDL for now.

**Acceptance criteria:**
- [ ] After `apply_schema` for reference schema, the peer has both `users` and `orders` tables with correct internal columns.
- [ ] `public_columns['A']['users']` equals `['id', 'email', 'name']`.
- [ ] `pk_columns['A']['users']` equals `['id']`.
- [ ] `unique_columns['A']['users']` includes `'email'`.
- [ ] Indexes (`orders_by_user`) exist and are usable.

**Verification:**
- [ ] Manual check: introspect the peer's actual SQLite schema after apply_schema and assert column names.

**Dependencies:** Task 2

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py`

**Estimated scope:** S

---

#### Task 4: execute() — INSERT Rewrite

**Description:** Detect `INSERT INTO ... (...) VALUES (...)` via regex. Extract table name, public columns, and params. Rewrite to include `_ts` / `_peer` pairs for all mutable columns. Increment logical clock once.

**Acceptance criteria:**
- [ ] `INSERT INTO users (id, email, name) VALUES (?, ?, ?), ('u1', 'alice@x.com', 'Alice')` executes without error.
- [ ] After execution, a raw `SELECT * FROM users` shows `email_ts`, `email_peer`, `name_ts`, `name_peer` populated with the clock value and peer id.
- [ ] Clock is incremented by exactly 1.
- [ ] Unsupported INSERT patterns (no explicit column list, `INSERT INTO users VALUES (...)`) are passed through raw (may fail later, but not crash the adapter).

**Verification:**
- [ ] Manual check: execute INSERT via adapter, then query raw SQLite to assert metadata columns exist and are populated.

**Dependencies:** Task 3

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py`

**Estimated scope:** S

---

#### Task 5: execute() — UPDATE and DELETE Rewrite

**Description:**
- **UPDATE**: Detect `UPDATE table SET col1 = ?, col2 = ? WHERE ...`. Rewrite to include `<col>_ts = ?, <col>_peer = ?` for each SET column. Increment clock.
- **DELETE**: Detect `DELETE FROM table WHERE ...` or bare `DELETE FROM table`. Rewrite as `UPDATE table SET tombstone = 1, delete_ts = ? WHERE ...`. Increment clock.

**Acceptance criteria:**
- [ ] Single- and multi-column UPDATE executes without error and populates `_ts` / `_peer` for each target column.
- [ ] DELETE (with and without WHERE) executes as tombstone UPDATE; raw SELECT shows `tombstone=1` and `delete_ts` set.
- [ ] Clock increments by 1 per UPDATE and per DELETE.
- [ ] Non-matching SQL passes through unchanged (e.g., SELECT queries).

**Verification:**
- [ ] Manual check: perform UPDATE and DELETE, inspect raw rows, assert metadata.

**Dependencies:** Task 4

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py`

**Estimated scope:** S

---

### Checkpoint: Write Path Complete

- [ ] Local writes (INSERT, UPDATE, DELETE) inject metadata correctly.
- [ ] Schema is registered and stored per peer.
- [ ] Clock increments reliably.

---

### Phase 3: Read Path

#### Task 6: snapshot_state() and snapshot_hash()

**Description:**
1. `snapshot_state`: For every registered table on a peer, select only `public_columns` where `tombstone=0`, ordered by PK. Conflicted rows (`conflicted=1`) are included with mangled unique columns (`value#conflict_<pk>`). Build `{table: [row_dict]}`.
2. `snapshot_hash`: Serialize `snapshot_state` with `json.dumps(sort_keys=True, default=str)` and SHA256.

**Acceptance criteria:**
- [ ] `snapshot_state` returns only public columns.
- [ ] Tombstoned rows are excluded.
- [ ] `snapshot_hash` is deterministic: two calls on identical state return identical hash.
- [ ] Hash format is a 64-character hex string.

**Verification:**
- [ ] Manual check: insert a row, call `snapshot_state`, assert dict structure and absence of metadata columns.
- [ ] Insert a second row, call `snapshot_hash`, call again, assert same value.

**Dependencies:** Task 3

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py`

**Estimated scope:** S

---

### Checkpoint: Read Path Complete

- [ ] Snapshot correctly exposes only public visible state.
- [ ] Hash is deterministic.

---

### Phase 4: Sync & Merge

#### Task 7: Row Merge Algorithm (Unit)

**Description:** Implement the per-row per-cell LWW merge as a standalone method. Given an incoming row dict (with metadata) and a local row dict (with metadata), produce the merged row dict. Uses Remove-Wins tombstone policy: tombstones are permanent during merge. Resurrection only via explicit local re-INSERT.

**Acceptance criteria:**
- [ ] Higher timestamp wins.
- [ ] Equal timestamp: higher `peer_id` lexicographically wins.
- [ ] Incoming cell wins → local cell overwritten.
- [ ] Local cell wins → local cell preserved.
- [ ] If merged row has tombstone=1 and delete_ts > 0, tombstone remains (Remove-Wins).
- [ ] Associative/commutative/idempotent properties hold (unit test with small matrices).

**Verification:**
- [ ] Manual check: write a quick script that merges two synthetic row dicts and asserts winner.

**Dependencies:** None (standalone logic)

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py` (add merge method)

**Estimated scope:** S

---

#### Task 8: Full sync() — Extract, Merge, Auto-Create Tables

**Description:**
1. Extract full state (all registered tables, all rows including tombstoned) from source as Python dicts.
2. Build schema manifest from source.
3. Send to destination.
4. Destination: if table absent → auto-create from schema manifest using internal DDL generation.
5. Destination: for each incoming row, fetch local row by PK, merge using Task 7 algorithm, write back (INSERT OR REPLACE).
6. Perform two passes: `merge(B→A)` then `merge(A→B)`.

**Acceptance criteria:**
- [ ] After `sync(A, B)`, both peers contain rows previously only on A and rows previously only on B.
- [ ] Metadata (timestamps, peer ids) is carried across sync.
- [ ] Auto-created tables appear in destination schema.
- [ ] No exceptions during full reference trace sync operations.

**Verification:**
- [ ] Manual check: create two peers, insert different rows, sync, assert `snapshot_state` contains union.

**Dependencies:** Tasks 6, 7

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py`

**Estimated scope:** M (multi-step algorithm)

---

#### Task 9: Post-Sync Uniqueness Scan

**Description:** After all rows are merged in a sync, scan each table's declared unique constraints (as column-groups) for duplicates among visible (non-tombstoned, non-conflicted) rows. Uses a two-pass approach: Pass 1 collects all loser PKs across all constraints, Pass 2 marks them `conflicted=1`. Winner is `(lowest ts, lowest peer_id)` per column-group. Run this after the two sync passes.

**Acceptance criteria:**
- [ ] If two rows have the same composite unique key, the one with higher `(ts, peer_id)` across the constraint columns is marked `conflicted=1`.
- [ ] If two rows have the same unique value and equal ts, the one with lexicographically higher `peer_id` loses.
- [ ] Conflicted rows appear in `snapshot_state` with mangled unique columns.

**Verification:**
- [ ] Manual check: insert two rows with same email on separate peers (simulate via raw metadata insert or direct SQLite), sync, assert only one visible in snapshot.

**Dependencies:** Task 8

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py`

**Estimated scope:** S–M

---

### Checkpoint: Sync Complete

- [ ] Sync merges states correctly.
- [ ] Unique column conflicts are arbitrated post-sync.
- [ ] Snapshot hides conflicted and tombstoned rows.

---

### Phase 5: Integration

#### Task 10: Benchmark Self-Check Run

**Description:** Run the benchmark's `run.py` against the adapter with `--fk-policy cascade`. Debug failures until all axes pass.

**Acceptance criteria:**
- [ ] `reference` scenario assertions all pass.
- [ ] `chaos` seeds all pass (order-invariance).
- [ ] `randomized` seeds all pass (convergence, uniqueness, data-preservation).
- [ ] Stretch scenarios all pass (composite uniqueness, multi-level FK, high density, long run).
- [ ] Score: 1.0000 / 1.0000.

**Verification:**
- [ ] Command: `python bench-p01-crdt/run.py --adapter adapters.team:TeamAdapter --fk-policy cascade`
- [ ] Iterate on any failures; root-cause and fix.

**Dependencies:** All previous tasks

**Files likely touched:**
- `bench-p01-crdt/adapters/team.py` (bug fixes)

**Estimated scope:** M (debugging is unpredictable)

---

### Checkpoint: Final

- [ ] All benchmark axes pass on `self_check.py`.
- [ ] Code is clean and documented.
- [ ] Ready for review.

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Regex does not match all benchmark SQL patterns | High | White-box review of generated ops in `scenarios/reference.py` and `randomized.py`; add coverage for observed patterns. |
| Param expansion count mismatch on INSERT | High | Unit test INSERT rewrite against exact benchmark params; verify param count equals placeholder count. |
| Tombstone resurrection rule is wrong | Medium | Adopted Remove-Wins: tombstones permanent during merge; resurrection only via explicit re-INSERT. |
| Post-sync uniqueness scan misses duplicates | Medium | Two-pass scan with composite UNIQUE support; verified across randomized seeds. |
| SQLite FK enforcement blocks tombstone-written rows | Medium | Strip `ON DELETE CASCADE` in DDL; dynamic triggers propagate tombstones through FK chains; post-sync FK validation added. |
| L3 adversarial tests use unsupported SQL patterns | Low | Accept L3 risk; focus on L1/L2 correctness. |

---

## Open Questions

- **What should the adapter class be named?** The spec says `adapters/team.py` but the team name is not yet decided. Suggest `TeamAdapter` or project-specific name. This is cosmetic and can be renamed in 30 seconds.

