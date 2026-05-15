# Specification: Local-First Relational Database with Deterministic Offline Replication

## Version

Derived from PRD + 2026-05-16 grill session. All decisions herein represent the ground-truth design for implementation.

---

## 1. Goal

Build a lightweight local-first relational database adapter that plugs into the **Anvil P-01 CRDT-Native OLTP Benchmark Harness**. The adapter must satisfy:

- Offline writes on local SQLite
- Pairwise bidirectional sync between peers
- Deterministic convergence regardless of sync order
- Cell-level conflict resolution
- Uniqueness constraint preservation (arbitration)
- Foreign-key semantics under partition

The architecture is intentionally simplified: **one SQLite instance per peer, deterministic metadata per cell, state-based sync, lightweight middleware**.

---

## 2. Out of Scope

- Network transport (sync is in-process dict exchange)
- SQL parsing (only known lightweight patterns are intercepted)
- Multi-column UPDATE SET (single-column only for this implementation)
- Operation logs or causal replay
- Advanced CRDTs (sequence CRDTs, operational transforms)
- Custom storage engines, B-trees, or query planners
- Performance optimization (not a scoring axis)
- Arbitrary DDL alterations (table deletions, column add/remove)
- Internet-scale replication or distributed consensus

---

## 3. Architecture

```text
Application / Benchmark SQL
    ↓
Replication-aware Adapter (Python 3.12)
    ├── SQL Rewrite Layer (lightweight regex / pattern matching)
    ├── Schema Introspection & DDL Regeneration
    ├── Logical Clock & Peer ID Injection
    ├── Storage Layer (SQLite, per peer)
    └── Sync & Merge Layer (state-based, in-process)
        ↓
    Deterministic JSON row exchange between peers
```

Each peer is an independent SQLite connection managed by a single adapter instance.

---

## 4. Adapter Interface

Implements `bench-p01-crdt/adapter.py::Adapter`:

| Method | Role |
|--------|------|
| `open_peer(peer_id)` | Create in-memory SQLite connection, init clock to `0`. |
| `apply_schema(peer_id, stmts)` | Intercept `CREATE TABLE`, introspect via temp SQLite, regenerate internal DDL with metadata columns. Pass `CREATE INDEX` through. Store public column list and PK column list. |
| `execute(peer_id, sql, params)` | Lightweight pattern matching for INSERT/UPDATE/DELETE. Rewrite to inject logical clock + peer id metadata. Commit. |
| `sync(peer_a, peer_b)` | Two-pass unidirectional merge: `merge(B→A)` then `merge(A→B)`. Full state transfer per registered table. Post-sync uniqueness scan. |
| `snapshot_hash(peer_id)` | Deterministic SHA256 of `snapshot_state()` serialized via `json.dumps(sort_keys=True, default=str)`. |
| `snapshot_state(peer_id)` | For every registered table, `SELECT <public_cols> WHERE tombstone=0 AND conflicted=0 ORDER BY <pk>` → dict list. |
| `close()` | Close all peer connections. |

---

## 5. Data Model

### 5.1 Logical Cell Model

Every mutable cell is logically a tuple:

```
(value, logical_timestamp, peer_id)
```

### 5.2 Physical Table Representation

Internal storage adds metadata columns directly after each public mutable column:

```sql
CREATE TABLE users (
    id    TEXT PRIMARY KEY,          -- PK: no metadata
    email TEXT NOT NULL,
    email_ts    INTEGER DEFAULT 0,
    email_peer  TEXT DEFAULT '',
    name  TEXT,
    name_ts     INTEGER DEFAULT 0,
    name_peer   TEXT DEFAULT '',
    tombstone   INTEGER DEFAULT 0,
    delete_ts   INTEGER DEFAULT 0,
    conflicted  INTEGER DEFAULT 0
);
```

**Rules:**
- Mutable columns get a `_ts` / `_peer` pair.
- PK columns **do not** get metadata pairs.
- Row-level columns appended last: `tombstone`, `delete_ts`, `conflicted`.

### 5.3 Schema Introspection

When `apply_schema` receives `CREATE TABLE`:

1. Execute on a **temporary** SQLite connection.
2. Run `PRAGMA table_info(table_name)` to get column names, types, PK flags.
3. Run `PRAGMA foreign_key_list(table_name)` to understand FK references.
4. Generate internal DDL:
   - Keep public columns in original order.
   - After each mutable (non-PK) column, add `<col>_ts` and `<col>_peer`.
   - Append `tombstone`, `delete_ts`, `conflicted`.
   - Strip `UNIQUE` constraints.
   - Strip `ON DELETE CASCADE` (keep FK reference itself).
5. Execute internal DDL on the real peer connection.
6. Store `public_columns[peer_id][table_name]` and `pk_columns[peer_id][table_name]`.

---

## 6. Write Flow

### 6.1 INSERT

**Input SQL:**
```sql
INSERT INTO users (id, email, name) VALUES (?, ?, ?)
```

**Rewrite:**
```sql
INSERT INTO users (id, email, email_ts, email_peer, name, name_ts, name_peer)
VALUES (?, ?, ?, ?, ?, ?, ?)
```

**Params expansion:**
```python
(v_id, v_email, ts, peer_id, v_name, ts, peer_id)
```

Rules:
- Extract table name and public column list from the SQL text via regex.
- Look up registered schema to know which columns get metadata pairs (all except PK).
- Append `_ts` and `_peer` for each mutable column in order.
- Append `ts` and `peer_id` values for each mutable column.
- Clock incremented once per statement and applied uniformly to all mutable cells.

### 6.2 UPDATE

**Input SQL (single-column only):**
```sql
UPDATE users SET name = ? WHERE id = ?
```

**Rewrite:**
```sql
UPDATE users
SET name = ?, name_ts = ?, name_peer = ?
WHERE id = ?
```

**Params expansion:**
```python
(v_name, ts, peer_id, row_id)
```

Rules:
- Extract table, target column, value param, and WHERE clause.
- Append `<col>_ts` and `<col>_peer` to the SET list.
- Clock incremented once.

### 6.3 DELETE

**Input SQL:**
```sql
DELETE FROM users WHERE id = ?
```

**Rewrite:**
```sql
UPDATE users
SET tombstone = 1, delete_ts = ?
WHERE id = ?
```

**Params expansion:**
```python
(ts, row_id)
```

Rules:
- DELETE is always rewritten to a tombstone UPDATE.
- `tombstone=1`, `delete_ts=<current clock>`.
- Row remains physically present.

### 6.4 Update on Tombstoned Row

Allowed. The UPDATE proceeds cell-by-cell. If the new cell's `ts > row's delete_ts`, during the merge/visibility logic the row is treated as resurrected. Specifically, if after a write any cell's latest `ts > delete_ts`, the middleware may clear the tombstone (`tombstone=0, delete_ts=0`) and the row becomes visible again.

---

## 7. Sync & Merge Algorithm

### 7.1 Sync Protocol

1. Build sync payload for source peer:
   ```python
   {
     "schema": {
       "users": ["id", "email", "name"],
       "orders": ["id", "user_id", "status", "total_cents"]
     },
     "rows": {
       "users": [row_dict, row_dict, ...],
       "orders": [...]
     }
   }
   ```
2. Send to destination peer.
3. Destination validates schema:
   - If a table in payload is unknown → auto-create using schema manifest + internal DDL generation.
   - If a table name already exists but public columns differ → **raise error**.
4. Merge each incoming row.

### 7.2 Row Merge (Per-Cell LWW)

For each incoming row (by primary key):

```
1. Lookup local row by PK.
2. For every mutable column:
   a. Compare incoming (value, incoming_ts, incoming_peer)
      with local  (local_value, local_ts, local_peer).
   b. If incoming_ts > local_ts:
        overwrite local cell.
   c. Else if incoming_ts == local_ts:
        larger_peer_id wins (lexicographic).
   d. Else: keep local cell.
3. After cell merge, evaluate tombstone:
   a. If any surviving cell has ts > row's delete_ts:
        clear tombstone (tombstone=0, delete_ts=0) → row resurrected.
   b. Else: tombstone remains.
4. Write merged row back.
```

**Properties:** deterministic, associative, commutative, idempotent.

### 7.3 Post-Sync Uniqueness Scan

After all rows are merged, for every table with a declared unique column (recorded from original DDL introspection):

```sql
SELECT email, MIN(email_ts) AS min_ts, email_peer
FROM users
WHERE tombstone = 0 AND conflicted = 0
GROUP BY email
HAVING COUNT(*) > 1
```

For each duplicate group:
- **Winner:** row with `(lowest email_ts, lowest email_peer_id)`.
- **Losers:** all other rows. Set `conflicted = 1` for each loser.

These rows become invisible to `snapshot_state()`.

**Rationale:** Offline peers may independently claim the same unique value. Deterministic arbitration resolves this post-merge rather than at write time, because pre-merge both rows must exist physically.

---

## 8. Read Flow

### 8.1 Direct SQL SELECT

`SELECT` queries pass through **unchanged** to SQLite. No rewriting. Developers who run `SELECT *` will see internal metadata columns (accepted leakage).

### 8.2 Snapshot State (Benchmark Interface)

`snapshot_state(peer_id)` builds the public-visible state:

```python
for table in registered_tables:
    public_cols = self.public_columns[peer_id][table]
    cols_str = ", ".join(public_cols)
    sql = f"SELECT {cols_str} FROM {table} WHERE tombstone=0 AND conflicted=0 ORDER BY id"
    rows = conn.execute(sql).fetchall()
    result[table] = [dict(zip(public_cols, row)) for row in rows]
```

**Guarantees:**
- Tombstoned rows hidden.
- Conflicted rows hidden.
- Metadata columns stripped from output.
- Order is deterministic (ORDER BY primary key).

### 8.3 Snapshot Hash

```python
state = self.snapshot_state(peer_id)
blob = json.dumps(state, sort_keys=True, default=str).encode()
return hashlib.sha256(blob).hexdigest()
```

---

## 9. Foreign Key Policy

**Declared policy: `tombstone`**

- Parent row is tombstoned (not physically deleted).
- Child rows (e.g., `orders.user_id`) continue referencing the tombstoned row.
- SQLite FK enforcement remains active because the parent row still physically exists.
- `ON DELETE CASCADE` is removed in internal DDL, so SQLite never auto-cascades.
- Deterministic under partition.

Benchmark assertion expects:
- `orders` row `o1` is still present.
- `o1.user_id` references `u1`, which is invisible (tombstoned) in `snapshot_state()`.

---

## 10. Deterministic Convergence Guarantees

The system converges because:

- **Cell-level merge** is deterministic total order: `(ts, peer_id)`.
- **Tombstones** preserve deletion intent explicitly.
- **Full state transfer** removes replay-order ambiguity.
- **Associativity/Commutativity/Idempotency:** merging states A then B yields the same result as B then A, and merging a state with itself is a no-op.

Therefore, any sync ordering (chaos scenario) reaches the same final state.

---

## 11. Index Strategy

- `CREATE INDEX` statements pass through directly to SQLite.
- Indexes are **not** replicated; they are deterministic derived structures.
- Since internal tables preserve public column positions, standard indexes on public columns work without modification.

---

## 12. Technology Stack

| Component | Choice |
|-----------|--------|
| Database | SQLite (in-memory via `:memory:`) |
| Language | Python 3.12 |
| SQL Interception | Lightweight regex + schema introspection |
| Serialization | Python dicts (in-process sync), JSON for hashing |
| Hashing | SHA256 |

---

## 13. Benchmark Integration Checklist

| Harness Requirement | Our Strategy |
|---------------------|--------------|
| `open_peer` | In-memory SQLite + clock init. |
| `apply_schema` | Temp-introspection + internal DDL generation. Pass-through indexes. |
| `execute` | Pattern-based rewrite for INSERT / single-col UPDATE / DELETE. |
| `sync` | Full-state dict exchange, two-pass merge, post-sync uniqueness scan. |
| `snapshot_hash` | Deterministic JSON sort + SHA256. |
| `snapshot_state` | Public-column SELECT filtering tombstones + conflicted rows. |
| `close` | Close all peer SQLite connections. |
| `fk-policy` | Declare `tombstone` on CLI. |

**Assertions mapping:**

| Assertion | How it passes |
|-----------|---------------|
| `convergence` | LWW merge is deterministic + quiescence after full sync mesh. |
| `uniqueness:users.email` | Post-sync scan marks duplicates `conflicted=1`; snapshot hides them. |
| `fk:tombstone` | Parent tombstoned but exists physically; child row visible in snapshot. |
| `cell-level:u1` | Concurrent name/email updates on different cells have independent metadata; both preserved. |
| `order-invariance` | Deterministic associative/commutative merge guarantees same hash for any sync order. |
| `randomized` | Same invariant logic covers arbitrary operation traces. |
| `idempotent-sync` | Merging a state with itself is a no-op, confirmed by `snapshot_hash` equality. |

---

## 14. Edge Cases and Decisions

| Edge Case | Decision |
|-----------|----------|
| Incoming row for unknown table? | Auto-create table from schema payload. |
| Incoming row for table with different columns? | **Error.** Unsupported scenario. |
| Local UPDATE causes uniqueness duplicate? | Deferred to post-sync scan; not solved eagerly. |
| Update on tombstoned row with `ts > delete_ts`? | Row is **resurrected** (`tombstone=0`, `delete_ts=0`). |
| Duplicate unique values with equal timestamps? | Tie-break by lexicographic `peer_id` (lower wins). |
| `SELECT *` returns metadata columns? | Accepted leakage for hackathon scope. |
| Multi-column UPDATE SET? | Out of scope for now. Single-column only. |
| Arbitrary SQL patterns? | Not supported; benchmark patterns only. |

---

## 15. Confidence Level After Resolution

95%. Remaining risk is purely implementation-level regex edge cases and param-count math. Architecture and algorithm are fully specified.

