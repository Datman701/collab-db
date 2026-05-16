# collab-db — CRDT-Native OLTP Adapter

A local-first relational database adapter with deterministic offline replication, built for the **Anvil P-01 CRDT-Native OLTP Benchmark**.

**Benchmark Score: 1.0000 / 1.0000 (100%)** — L3 Final (`anvil-2026-p01-L3-final`).

---

## Quick Start

```bash
# Run unit tests (65 tests)
cd collab-db
python3 -m unittest discover tests -v

# Run L3 benchmark
cd bench-p01-crdt
python3 run.py --adapter adapters.team:TeamAdapter --fk-policy cascade
```

**Dependencies:** Python 3.9+ with standard library only (`sqlite3`, `hashlib`, `json`, `re`, `logging`). No `pip install` needed.

---

## Architecture

```
Application / Benchmark SQL
    ↓
TeamAdapter (Python, src/team_adapter.py)
    ├── SQL Rewrite Layer (regex-based)
    │     ├── INSERT with explicit columns
    │     ├── Single- and multi-column UPDATE SET
    │     ├── DELETE WHERE (tombstone rewrite)
    │     └── Bare DELETE (tombstone all rows)
    ├── Schema Introspection & DDL Regeneration
    │     ├── Composite UNIQUE detection (column-groups)
    │     └── Dynamic FK cascade trigger generation
    ├── Logical Clock & Peer ID Injection
    ├── Storage Layer (SQLite in-memory, per peer)
    └── Sync & Merge Layer (state-based, in-process)
          ├── Cell-level LWW merge (Remove-Wins tombstones)
          ├── Two-pass uniqueness scan (composite-aware)
          └── Post-sync FK integrity validation
```

Each peer is an independent in-memory SQLite connection. Writes are transparently rewritten to inject per-cell metadata (`_ts`, `_peer`) for Last-Writer-Wins conflict resolution. Sync is full-state exchange with deterministic cell-level merge.

---

## Project Structure

```
├── src/
│   └── team_adapter.py          # Core adapter implementation (~715 lines)
├── bench-p01-crdt/              # Benchmark harness (upstream)
│   ├── adapter.py               # Abstract base class
│   ├── harness.py               # Scenario orchestration + scoring
│   ├── run.py                   # Full CLI entry point
│   ├── assertions.py            # Invariant checkers
│   ├── scenarios/               # Reference, chaos, randomized, stretch
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
│   └── test_task10.py           # Integration + gap coverage tests
├── SPEC.md                      # Full technical specification
├── PLAN.md                      # Implementation plan with task breakdown
├── CHANGELOG.md                 # Detailed change history (v1–v5)
├── GRILL_SESSION.md             # Design decision Q&A session
└── requirements.txt             # Dependency manifest (stdlib only)
```

---

## Key Design Decisions

| Decision | Rationale |
|--|--|
| **Remove-Wins tombstones** | Deletes are permanent during merge. If peer A edits a row while peer B deletes it, the delete wins. Resurrection only via explicit local re-INSERT. Matches collaborative tool behavior (Google Docs, Notion). |
| **FK cascade via triggers** | `ON DELETE CASCADE` is stripped from internal DDL. Dynamic `AFTER UPDATE OF tombstone` triggers propagate tombstones through FK chains (e.g., `orgs → users → orders`). |
| **Composite UNIQUE as column-groups** | `UNIQUE(col_a, col_b)` stored as `[('col_a', 'col_b')]`, not as independent constraints. Uniqueness scan groups on the full composite key. |
| **Two-pass uniqueness scan** | Pass 1: detect all losers across all constraints. Pass 2: mark `conflicted=1`. Prevents cross-constraint interference. |
| **Conflict-preserving snapshots** | Conflicted rows included in `snapshot_state` with mangled unique columns (`value#conflict_<pk>`) to satisfy both data-preservation and uniqueness invariants. |
| **Full state sync** | Simple, correct, O(N) per sync — acceptable for benchmark-scale datasets. |
| **Regex-based SQL rewriting** | Avoids a SQL parser; handles the exact patterns the benchmark generates. Unrecognized writes emit a warning. |
| **Cell-level LWW merge** | Each column is independently versioned with `(ts, peer_id)` for deterministic conflict resolution. |

---

## Benchmark Axes (L3 Final)

| Scenario | Key Assertions | Status |
|----------|---------------|--------|
| REFERENCE | convergence, uniqueness, fk:cascade, cell-level-strict | ✅ |
| CELL-LEVEL-STRICT | convergence, cell-level (non-vacuous) | ✅ |
| CHAOS (5 seeds) | convergence, order-invariance | ✅ |
| RANDOMIZED (8 seeds) | convergence, uniqueness, data-preservation | ✅ |
| COMPOSITE UNIQUENESS | convergence, composite-uniqueness, data-preservation | ✅ |
| MULTI-LEVEL FK CHAIN | convergence, fk-chain-integrity, data-preservation | ✅ |
| HIGH-DENSITY UNIQUENESS | convergence, uniqueness, data-preservation, uniqueness-winner | ✅ |
| LONG-RUN STRESS (×2) | convergence, uniqueness, data-preservation | ✅ |

---

## Known Limitations

1. **SQL rewriter covers benchmark patterns only** — `INSERT ... SELECT`, `REPLACE INTO`, subqueries, and expression-based SET clauses fall through to raw SQLite without metadata injection.
2. **Full-state sync, not incremental** — O(N) per sync per table. Acceptable under ~50K rows.
3. **Per-row conflict flag** — A row conflicted on one unique column has all unique columns mangled in the snapshot, even columns where it holds a legitimately unique value.
4. **Clocks are not persisted** — In-memory only. Peer restart resets the clock, causing all new writes to lose LWW comparisons against previously synced state.
5. **No schema evolution** — `ALTER TABLE` is not supported. Schema changes require full reimport.

---

## Changelog Summary

| Version | Score | Key Changes |
|---------|-------|-------------|
| **v5** | 1.0000 | Composite UNIQUE as column-groups, bare DELETE → tombstone-all, two-pass uniqueness scan, FK validation after sync, dead code cleanup |
| **v4** | 1.0000 | Dynamic FK cascade triggers, conflict-preserving snapshots |
| **v3** | 1.0000 | Remove-Wins doctrine, tombstone resurrection via re-INSERT, structured logging, complete state cleanup |
| **v2** | 1.0000 | Multi-column UPDATE, clock sync, NULL handling, FK-safe merges, SQL normalization |
| **v1** | — | Initial implementation: schema introspection, LWW merge, full-state sync, post-sync uniqueness |

See [CHANGELOG.md](CHANGELOG.md) for full details.

**Total: 65 tests, all passing. Benchmark score: 1.0000 / 1.0000.**
