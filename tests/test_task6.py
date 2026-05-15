"""Tests for Task 6: snapshot_state() and snapshot_hash()."""
import sys
import os
import unittest
import re

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


class TestSnapshot(unittest.TestCase):
    def setUp(self):
        self.adapter = TeamAdapter()
        self.adapter.open_peer('A')
        self.adapter.apply_schema('A', SCHEMA)

    def tearDown(self):
        self.adapter.close()

    def test_snapshot_state_returns_public_columns_only(self):
        self.adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        state = self.adapter.snapshot_state('A')
        self.assertIn('users', state)
        self.assertEqual(len(state['users']), 1)
        row = state['users'][0]
        self.assertEqual(set(row.keys()), {'id', 'email', 'name'})
        self.assertNotIn('email_ts', row)
        self.assertNotIn('tombstone', row)

    def test_snapshot_state_excludes_tombstoned_rows(self):
        self.adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        self.adapter.execute('A', "DELETE FROM users WHERE id = ?", ('u1',))
        state = self.adapter.snapshot_state('A')
        self.assertEqual(len(state['users']), 0)

    def test_snapshot_hash_deterministic(self):
        self.adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        hash1 = self.adapter.snapshot_hash('A')
        hash2 = self.adapter.snapshot_hash('A')
        self.assertEqual(hash1, hash2)

    def test_snapshot_hash_format(self):
        self.adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        h = self.adapter.snapshot_hash('A')
        self.assertEqual(len(h), 64)
        self.assertTrue(all(c in '0123456789abcdef' for c in h))

    def test_snapshot_state_orders_by_pk(self):
        self.adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u2', 'bob@x.com', 'Bob'))
        self.adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        state = self.adapter.snapshot_state('A')
        ids = [row['id'] for row in state['users']]
        self.assertEqual(ids, ['u1', 'u2'])


if __name__ == '__main__':
    unittest.main()
