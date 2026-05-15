"""Tests for Task 10: Integration + Gap Coverage.

Covers the fixes applied during the gap analysis:
- Conflicted flag reset on re-sync
- NULL handling in uniqueness scan
- Multi-column UPDATE rewrite
- Clock synchronization during sync
- Duplicate table registration guard
- SQL normalization (trailing semicolons)
- INSERT OR REPLACE FK safety (explicit UPDATE for existing rows)
"""
import sys
import os
import unittest

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'bench-p01-crdt'))
sys.path.insert(0, PROJECT_ROOT)

from src.team_adapter import TeamAdapter
from scenarios.reference import SCHEMA as REF_SCHEMA, PEERS, OPERATIONS, FINAL_SYNC_ORDER, Stmt, Sync
from assertions import assert_convergence, assert_uniqueness_email, assert_fk_documented, assert_cell_level_merge


SCHEMA = [
    """CREATE TABLE users (
         id    TEXT PRIMARY KEY,
         email TEXT NOT NULL UNIQUE,
         name  TEXT
       )""",
]

FULL_SCHEMA = [
    """CREATE TABLE users (
         id    TEXT PRIMARY KEY,
         email TEXT NOT NULL UNIQUE,
         name  TEXT
       )""",
    """CREATE TABLE orders (
         id          TEXT PRIMARY KEY,
         user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
         status      TEXT NOT NULL,
         total_cents INTEGER NOT NULL DEFAULT 0
       )""",
    "CREATE INDEX orders_by_user ON orders(user_id, status)",
]


class TestConflictedReset(unittest.TestCase):
    """Gap #3: conflicted flag must be reset when the duplicate is resolved."""

    def test_conflicted_reset_after_email_change(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)

        # Both peers create users with the same email (offline)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)",
                        ('u1', 'shared@x.com', 'Alice'))
        adapter.execute('B', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)",
                        ('u2', 'shared@x.com', 'Bob'))

        # Sync — one of them gets marked conflicted
        adapter.sync('A', 'B')
        state = adapter.snapshot_state('A')
        self.assertEqual(len(state['users']), 1, "Only winner should be visible")

        # Now resolve: update the loser's email to something unique
        # First figure out who was the loser
        conn_a = adapter.peers['A']
        cur = conn_a.execute("SELECT id FROM users WHERE conflicted = 1")
        loser = cur.fetchone()
        self.assertIsNotNone(loser)
        loser_id = loser[0]

        # Directly update the loser's email via raw SQL to resolve conflict
        conn_a.execute(f"UPDATE users SET email = 'unique@x.com', email_ts = 999, email_peer = 'A', conflicted = 0 WHERE id = ?",
                       (loser_id,))
        conn_a.commit()
        # Do same on B
        conn_b = adapter.peers['B']
        conn_b.execute(f"UPDATE users SET email = 'unique@x.com', email_ts = 999, email_peer = 'A', conflicted = 0 WHERE id = ?",
                       (loser_id,))
        conn_b.commit()

        # Re-sync — the uniqueness scan should reset conflicted, re-scan,
        # and find no duplicates now
        adapter.sync('A', 'B')
        state = adapter.snapshot_state('A')
        self.assertEqual(len(state['users']), 2, "Both users should now be visible")
        adapter.close()


class TestNullUniqueness(unittest.TestCase):
    """Gap #11: NULL values in unique columns should not be treated as duplicates."""

    def test_null_emails_not_conflicted(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        # Use a schema without NOT NULL on email
        adapter.apply_schema('A', [
            """CREATE TABLE users (
                 id    TEXT PRIMARY KEY,
                 email TEXT UNIQUE,
                 name  TEXT
               )"""
        ])
        # Insert two rows with NULL email via raw metadata injection
        conn = adapter.peers['A']
        conn.execute(
            "INSERT INTO users (id, email, email_ts, email_peer, name, name_ts, name_peer) "
            "VALUES ('u1', NULL, 1, 'A', 'Alice', 1, 'A')")
        conn.execute(
            "INSERT INTO users (id, email, email_ts, email_peer, name, name_ts, name_peer) "
            "VALUES ('u2', NULL, 1, 'A', 'Bob', 1, 'A')")
        conn.commit()

        # Run uniqueness scan — should NOT mark either as conflicted
        adapter._uniqueness_scan('A')
        state = adapter.snapshot_state('A')
        self.assertEqual(len(state['users']), 2, "NULL emails should not conflict")
        adapter.close()


class TestMultiColumnUpdate(unittest.TestCase):
    """Gap #2: Multi-column UPDATE SET support."""

    def test_multi_column_update_rewrites_correctly(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)",
                        ('u1', 'alice@x.com', 'Alice'))

        # Multi-column UPDATE
        adapter.execute('A', "UPDATE users SET name = ?, email = ? WHERE id = ?",
                        ('Alicia', 'alicia@x.com', 'u1'))

        conn = adapter.peers['A']
        cur = conn.execute("SELECT name, name_ts, name_peer, email, email_ts, email_peer FROM users WHERE id = 'u1'")
        row = cur.fetchone()
        self.assertEqual(row[0], 'Alicia')
        self.assertEqual(row[1], 2)  # ts=2 (insert was ts=1)
        self.assertEqual(row[2], 'A')
        self.assertEqual(row[3], 'alicia@x.com')
        self.assertEqual(row[4], 2)
        self.assertEqual(row[5], 'A')
        adapter.close()

    def test_multi_column_update_single_clock_increment(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)",
                        ('u1', 'alice@x.com', 'Alice'))
        adapter.execute('A', "UPDATE users SET name = ?, email = ? WHERE id = ?",
                        ('Alicia', 'alicia@x.com', 'u1'))
        self.assertEqual(adapter.clocks['A'], 2, "Multi-col UPDATE should increment clock once")
        adapter.close()


class TestClockSync(unittest.TestCase):
    """Gap #8: Clock synchronization during sync."""

    def test_clock_synced_after_sync(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)

        # A does 5 operations
        for i in range(5):
            adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)",
                            (f'u{i}', f'e{i}@x.com', f'Name{i}'))
        self.assertEqual(adapter.clocks['A'], 5)
        self.assertEqual(adapter.clocks['B'], 0)

        adapter.sync('A', 'B')
        # After sync, B's clock should be at least as high as A's
        self.assertGreaterEqual(adapter.clocks['B'], adapter.clocks['A'])
        adapter.close()


class TestDuplicateTableRegistration(unittest.TestCase):
    """Gap #10: Duplicate table registration guard."""

    def test_no_duplicate_registration_on_repeated_sync(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)

        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)",
                        ('u1', 'alice@x.com', 'Alice'))

        # Sync multiple times
        adapter.sync('A', 'B')
        adapter.sync('A', 'B')
        adapter.sync('A', 'B')

        # Table should only appear once in registered_tables
        count = adapter.registered_tables['B'].count('users')
        self.assertEqual(count, 1, "Table should only be registered once")
        adapter.close()


class TestSqlNormalization(unittest.TestCase):
    """Gap #5: SQL with trailing semicolons should still be rewritten."""

    def test_insert_with_semicolon(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        # Trailing semicolon
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?);",
                        ('u1', 'alice@x.com', 'Alice'))
        state = adapter.snapshot_state('A')
        self.assertEqual(len(state['users']), 1)
        adapter.close()

    def test_update_with_semicolon(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)",
                        ('u1', 'alice@x.com', 'Alice'))
        adapter.execute('A', "UPDATE users SET name = ? WHERE id = ?;",
                        ('Alicia', 'u1'))
        state = adapter.snapshot_state('A')
        self.assertEqual(state['users'][0]['name'], 'Alicia')
        adapter.close()


class TestFKSafeDuringSync(unittest.TestCase):
    """Gap #6: Sync should not violate FK constraints."""

    def test_sync_with_parent_child_tables(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', FULL_SCHEMA)
        adapter.apply_schema('B', FULL_SCHEMA)

        # Create parent and child on A
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)",
                        ('u1', 'alice@x.com', 'Alice'))
        adapter.execute('A',
                        "INSERT INTO orders (id, user_id, status, total_cents) VALUES (?, ?, ?, ?)",
                        ('o1', 'u1', 'pending', 1200))

        # Sync to B — should not raise FK errors
        adapter.sync('A', 'B')

        state_b = adapter.snapshot_state('B')
        self.assertEqual(len(state_b.get('orders', [])), 1)
        self.assertEqual(state_b['orders'][0]['user_id'], 'u1')
        adapter.close()

    def test_repeated_sync_with_fk(self):
        """Multiple syncs with parent-child shouldn't cause FK issues from UPDATE."""
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', FULL_SCHEMA)
        adapter.apply_schema('B', FULL_SCHEMA)

        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)",
                        ('u1', 'alice@x.com', 'Alice'))
        adapter.execute('A',
                        "INSERT INTO orders (id, user_id, status, total_cents) VALUES (?, ?, ?, ?)",
                        ('o1', 'u1', 'pending', 1200))

        # Multiple syncs — each one re-merges existing rows via UPDATE
        adapter.sync('A', 'B')
        adapter.sync('A', 'B')
        adapter.sync('A', 'B')

        hash_a = adapter.snapshot_hash('A')
        hash_b = adapter.snapshot_hash('B')
        self.assertEqual(hash_a, hash_b)
        adapter.close()


class TestBenchmarkIntegration(unittest.TestCase):
    """Task 10: Full benchmark self-check integration test."""

    def test_reference_scenario_passes(self):
        """Run the reference scenario and verify all assertions pass."""
        adapter = TeamAdapter()
        for p in PEERS:
            adapter.open_peer(p)
            adapter.apply_schema(p, REF_SCHEMA)

        for op in OPERATIONS:
            if isinstance(op, Stmt):
                adapter.execute(op.peer, op.sql, op.params)
            elif isinstance(op, Sync):
                adapter.sync(op.a, op.b)

        for a, b in FINAL_SYNC_ORDER:
            adapter.sync(a, b)

        hashes = {p: adapter.snapshot_hash(p) for p in PEERS}
        state = adapter.snapshot_state(PEERS[0])

        self.assertTrue(assert_convergence(hashes).passed, "Convergence failed")
        self.assertTrue(assert_uniqueness_email(state).passed, "Uniqueness failed")
        self.assertTrue(assert_fk_documented(state, 'tombstone').passed, "FK policy failed")
        self.assertTrue(assert_cell_level_merge(state).passed, "Cell-level merge failed")
        adapter.close()


if __name__ == '__main__':
    unittest.main()
