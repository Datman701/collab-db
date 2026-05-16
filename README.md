# collab-db — CRDT-Native OLTP Adapter

A local-first relational database adapter with deterministic offline replication, built for the **Anvil P-01 CRDT-Native OLTP Benchmark**.

**Benchmark Score: 1.00 / 1.00** — all 6 axes pass across reference, chaos, and randomized scenarios.

---

## Quick Start

```bash
# Run unit tests (98 tests)
cd tests
python3 run_tests.py

# Run benchmark self-check
cd bench-p01-crdt
python3 self_check.py --adapter adapters.team:TeamAdapter --fk-policy tombstone

# Full run with custom randomized seeds
python3 run.py --adapter adapters.team:TeamAdapter --fk-policy tombstone \
  --randomized-seeds 9999 31415 27182 --rand-peers 5 --rand-ops 150
```

---

## Architecture

```
Application / Benchmark SQL
    ↓
TeamAdapter (Python 3.12, src/team_adapter.py)
    ├── SQL Rewrite Layer (regex-based)
    ├── Schema Introspection & DDL Regeneration
    ├── Logical Clock & Peer ID Injection
    ├── Storage Layer (SQLite in-memory, per peer)
    └── Sync & Merge Layer (state-based, in-process)
```

Each peer is an independent in-memory SQLite connection. Writes are transparently rewritten to inject per-cell metadata (`_ts`, `_peer`) for Last-Writer-Wins conflict resolution. Sync is full-state exchange with deterministic cell-level merge.

---

## Project Structure

```
├── src/
│   └── team_adapter.py          # Core adapter implementation
├── bench-p01-crdt/              # Benchmark harness (upstream)
│   ├── adapter.py               # Abstract base class
│   ├── harness.py               # Scenario orchestration + scoring
│   ├── self_check.py            # Quick local validation
│   ├── run.py                   # Full CLI entry point
│   ├── assertions.py            # Invariant checkers
│   ├── scenarios/               # Reference, chaos, randomized scenarios
│   └── adapters/
│       └── team.py              # Bridge that imports src/team_adapter.py
├── tests/
│   ├── test_task1.py            # Adapter skeleton + peer lifecycle
│   ├── test_task2.py            # Schema introspection engine
│   ├── test_task3.py            # apply_schema DDL interception
│   ├── test_task4.py            # INSERT rewrite
│   ├── test_task5.py            # UPDATE + DELETE rewrite
│   ├── test_task6.py            # snapshot_state / snapshot_hash
│   ├── test_task7.py            # Row merge algorithm (unit)
│   ├── test_task8.py            # Full sync (extract, merge, auto-create)
│   ├── test_task9.py            # Post-sync uniqueness scan
│   ├── test_task10.py           # Integration + gap coverage tests
│   └── test_task11_adversarial.py  # Adversarial & stress tests (33 tests)
├── SPEC.md                      # Full technical specification
├── PLAN.md                      # Implementation plan with task breakdown
├── GRILL_SESSION.md             # Design decision Q&A session
└── run_tests.py                 # Test runner
```

---

## Key Design Decisions

| Decision | Rationale |
|--|--
| **Permanent tombstones** | Spec §6.4 says middleware "may" resurrect rows; we keep tombstones permanent to satisfy FK tombstone policy (§9) where deleted parents must remain invisible |
| **Full state sync** | Simple, correct, O(n) per sync — acceptable for benchmark-scale datasets |
| **Regex-based SQL rewriting** | Avoids a SQL parser; handles the exact patterns the benchmark generates |
| **Post-sync uniqueness scan** | Offline peers may independently claim the same unique value; resolved after merge, not at write time |
| **Cell-level LWW merge** | Each column is independently versioned with `(ts, peer_id)` for deterministic conflict resolution |

---

## Benchmark Axes

| Axis | Weight | Strategy |
|------|--------|----------|
| Convergence | 0.30 | Deterministic LWW merge is associative, commutative, idempotent |
| Uniqueness (`users.email`) | 0.20 | Post-sync scan marks duplicates `conflicted=1`; snapshots hide them |
| FK policy (`tombstone`) | 0.15 | Parent tombstoned but physically present; child row survives |
| Cell-level merge (`u1`) | 0.10 | Independent `_ts`/`_peer` per column preserves concurrent updates |
| Order-invariance | 0.10 | Same merge properties guarantee identical hash for any sync order |
| Randomized | 0.15 | All invariants hold for arbitrary operation traces and seeds |

---

## Changelog

### v2 — Gap Analysis Fixes (2026-05-16)

Comprehensive robustness improvements identified through systematic gap analysis against the spec, benchmark harness, and L3 adversarial readiness.

#### Bug Fixes

- **Conflicted flag reset** — `_uniqueness_scan` now resets `conflicted=0` for all rows before re-scanning. Previously, rows whose uniqueness violation was resolved by a later mutation would remain permanently hidden. This ensures multi-round sync+mutate scenarios produce correct snapshots.

- **FK-safe sync merges** — Replaced `INSERT OR REPLACE` with explicit `UPDATE` for existing rows during sync. SQLite's `INSERT OR REPLACE` internally does `DELETE + INSERT`, which can trigger FK constraint violations when child rows reference the parent being updated. FK checks are temporarily disabled during the merge pass and re-enabled afterward for safety.

- **NULL handling in uniqueness scan** — `NULL` values in unique columns are no longer treated as duplicates. SQL NULL semantics say `NULL != NULL`, but Python dict grouping treated `None` as a single key. The scan now skips `None` values entirely.

- **Duplicate table registration guard** — `apply_schema` and `_sync_one_way` now check `if table not in registered_tables` before appending. Previously, repeated syncs could register the same table multiple times, causing duplicate rows in `snapshot_state`.

#### Enhancements

- **Multi-column UPDATE support** — The SQL rewriter now handles `UPDATE table SET col1 = ?, col2 = ? WHERE ...` in addition to single-column updates. Each column in the SET clause gets its own `_ts`/`_peer` metadata injected. All columns share a single clock increment per statement.

- **Clock synchronization during sync** — After a one-way merge, the destination peer's logical clock is bumped to `max(dst_clock, src_clock)`. This reduces unnecessary divergence windows and ensures subsequent writes on the destination get timestamps that reflect the full causal history.

- **SQL normalization** — Input SQL is now stripped of trailing semicolons and extra whitespace before regex matching. This prevents silent fallthrough to raw passthrough for otherwise-valid statements.

- **Deterministic table order in snapshots** — `snapshot_state` now iterates tables in sorted order. While `json.dumps(sort_keys=True)` already handled top-level key ordering for hash determinism, this makes the dict output consistent regardless of table registration order.

#### Code Quality

- **Top-level imports** — Moved `re`, `hashlib`, `json` from inline method imports to module-level imports.

- **Docstrings** — Added docstrings to all public methods (`open_peer`, `apply_schema`, `execute`, `sync`, `snapshot_hash`, `snapshot_state`, `close`) and improved existing ones for `_merge_row`, `_sync_one_way`, `_uniqueness_scan`.

- **Clarified tombstone policy** — Updated `_merge_row` docstring to explain why tombstones are permanent (spec says "may", not "must"; required for FK tombstone policy).

#### Test Coverage

- **11 new tests** in `test_task10.py` covering all gap fixes:
  - `TestConflictedReset` — Verifies conflicted rows become visible after email uniqueness is resolved
  - `TestNullUniqueness` — Verifies NULL emails are not treated as duplicates
  - `TestMultiColumnUpdate` — Verifies multi-column UPDATE rewrites correctly with single clock increment
  - `TestClockSync` — Verifies clock synchronization after sync
  - `TestDuplicateTableRegistration` — Verifies no duplicate registration on repeated sync
  - `TestSqlNormalization` — Verifies trailing semicolons don't break rewriting
  - `TestFKSafeDuringSync` — Verifies sync with parent-child tables doesn't raise FK errors
  - `TestBenchmarkIntegration` — Full reference scenario end-to-end assertion

- **Updated `test_task8.test_sync_merges_conflicting_rows`** — Adjusted expected behavior to account for clock synchronization (equal timestamps → peer_id tiebreak)

- **33 adversarial tests** in `test_task11_adversarial.py` covering:
  - N-peer convergence (4+ peers)
  - Sync-order invariance (exhaustive permutations)
  - Three-way uniqueness conflicts
  - FK tombstone cascading behavior
  - Stable cell winner under repeated sync
  - Sync idempotence across full mesh
  - Snapshot hash stability and hiding composites
  - SQL rewriter robustness (whitespace, case, multi-line)
  - Unicode safety
  - Randomized convergence with multiple seeds

#### Total: 98 tests, all passing. Benchmark score: 1.00 / 1.00.

### v3 — Production Hardening (2026-05-16)

Focused on real-world robustness rather than benchmark optimisation.

#### Bug Fixes

- **Re-insert after delete (tombstone resurrection)** — Inserting a row with the same PK as a tombstoned row previously crashed with `IntegrityError`. The INSERT path now detects tombstoned rows and converts to an UPDATE that clears `tombstone=0`, `delete_ts=0` and writes fresh cell metadata. This supports the common "delete then recreate" workflow.

- **Duplicate `open_peer()` leaks connection** — Calling `open_peer("A")` twice would orphan the first SQLite connection (memory leak). Now closes the old connection before creating a new one, with a warning log.

- **`close()` leaves stale metadata** — Previously only cleared `self.peers`. Now also clears `public_columns`, `pk_columns`, `unique_columns`, `registered_tables`, `clocks`, and `_table_schemas` so the adapter instance can be safely reused.

- **`_introspect_schema` temp connection leak** — If DDL execution failed, the temporary SQLite connection was never closed. Wrapped in `try/finally`.

#### Enhancements

- **DML passthrough warning** — If a write statement (INSERT/UPDATE/DELETE) doesn't match any rewrite regex and falls through to raw SQLite, a warning is logged. Data written without metadata injection will lack causality tracking and may be lost during sync. This catches typos and unexpected SQL patterns before they cause silent data corruption.

- **Structured logging** — Added `logging.getLogger(__name__)` with messages at DEBUG (resurrection events, merge decisions) and WARNING (passthrough fallthrough, duplicate peer) levels.

#### Design Decision: Remove-Wins Merge

Adopted **Remove-Wins** tombstone semantics: deletes are permanent during merge. If peer A edits a row while peer B deletes it, the delete wins after sync. Resurrection is only possible via **explicit local re-INSERT** — a deliberate user action to recreate a deleted record. This matches real collaborative tool behavior (Google Docs, Notion) and satisfies the FK tombstone policy.
