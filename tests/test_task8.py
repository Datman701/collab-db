"""Tests for Task 8: Full sync() — Extract, Merge, Auto-Create Tables."""
import sys
import os
import unittest

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'bench-p01-crdt'))
sys.path.insert(0, PROJECT_ROOT)

from src.team_adapter import TeamAdapter


SCHEMA = [
    """CREATE TABLE users (
         id    TEXT PRIMARY KEY,
         email TEXT NOT NULL UNIQUE,
         name  TEXT
       )""",
]


class TestSync(unittest.TestCase):
    def test_sync_transfers_rows_both_ways(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        adapter.execute('B', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u2', 'bob@x.com', 'Bob'))
        adapter.sync('A', 'B')
        state_a = adapter.snapshot_state('A')
        state_b = adapter.snapshot_state('B')
        ids_a = {row['id'] for row in state_a['users']}
        ids_b = {row['id'] for row in state_b['users']}
        self.assertEqual(ids_a, {'u1', 'u2'})
        self.assertEqual(ids_b, {'u1', 'u2'})
        adapter.close()

    def test_sync_preserves_metadata(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        adapter.sync('A', 'B')
        conn = adapter.peers['B']
        cur = conn.execute("SELECT email_ts, email_peer FROM users WHERE id = 'u1'")
        row = cur.fetchone()
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], 'A')
        adapter.close()

    def test_sync_auto_creates_table(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        # Don't apply schema to B
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        adapter.sync('A', 'B')
        conn = adapter.peers['B']
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
        self.assertIsNotNone(cur.fetchone())
        adapter.close()

    def test_sync_merges_conflicting_rows(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        # Sync so B gets u1
        adapter.sync('A', 'B')
        # A updates at ts=2, B updates at ts=1 (B's clock was 0)
        adapter.execute('A', "UPDATE users SET name = ? WHERE id = ?", ('Alice Cooper', 'u1'))
        adapter.execute('B', "UPDATE users SET name = ? WHERE id = ?", ('Alicia', 'u1'))
        adapter.sync('A', 'B')
        state_a = adapter.snapshot_state('A')
        u1_a = next(r for r in state_a['users'] if r['id'] == 'u1')
        state_b = adapter.snapshot_state('B')
        u1_b = next(r for r in state_b['users'] if r['id'] == 'u1')
        # A should win because A has higher ts (2 > 1)
        self.assertEqual(u1_a['name'], 'Alice Cooper')
        self.assertEqual(u1_b['name'], 'Alice Cooper')
        adapter.close()

    def test_sync_cell_level_merge(self):
        """Concurrent updates to different columns should both be preserved."""
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        # Sync so B gets u1
        adapter.sync('A', 'B')
        # A updates name, B updates email
        adapter.execute('A', "UPDATE users SET name = ? WHERE id = ?", ('Alice Cooper', 'u1'))
        adapter.execute('B', "UPDATE users SET email = ? WHERE id = ?", ('alice@ex.org', 'u1'))
        adapter.sync('A', 'B')
        state_a = adapter.snapshot_state('A')
        u1_a = next(r for r in state_a['users'] if r['id'] == 'u1')
        state_b = adapter.snapshot_state('B')
        u1_b = next(r for r in state_b['users'] if r['id'] == 'u1')
        # Both column updates should be preserved
        self.assertEqual(u1_a['name'], 'Alice Cooper')
        self.assertEqual(u1_a['email'], 'alice@ex.org')
        self.assertEqual(u1_b['name'], 'Alice Cooper')
        self.assertEqual(u1_b['email'], 'alice@ex.org')
        adapter.close()

    def test_sync_idempotent(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        adapter.sync('A', 'B')
        hash1_a = adapter.snapshot_hash('A')
        hash1_b = adapter.snapshot_hash('B')
        adapter.sync('A', 'B')
        hash2_a = adapter.snapshot_hash('A')
        hash2_b = adapter.snapshot_hash('B')
        self.assertEqual(hash1_a, hash2_a)
        self.assertEqual(hash1_b, hash2_b)
        adapter.close()


if __name__ == '__main__':
    unittest.main()
