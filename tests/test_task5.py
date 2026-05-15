"""Tests for Task 5: execute() — UPDATE and DELETE Rewrite."""
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


class TestUpdateDeleteRewrite(unittest.TestCase):
    def setUp(self):
        self.adapter = TeamAdapter()
        self.adapter.open_peer('A')
        self.adapter.apply_schema('A', SCHEMA)
        self.adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))

    def tearDown(self):
        self.adapter.close()

    def test_update_populates_metadata(self):
        self.adapter.execute('A', "UPDATE users SET name = ? WHERE id = ?", ('Alicia', 'u1'))
        conn = self.adapter.peers['A']
        cur = conn.execute("SELECT name_ts, name_peer FROM users WHERE id = 'u1'")
        row = cur.fetchone()
        self.assertEqual(row[0], 2)  # clock was 1 after insert, now 2
        self.assertEqual(row[1], 'A')

    def test_update_increments_clock(self):
        self.assertEqual(self.adapter.clocks['A'], 1)
        self.adapter.execute('A', "UPDATE users SET name = ? WHERE id = ?", ('Alicia', 'u1'))
        self.assertEqual(self.adapter.clocks['A'], 2)

    def test_delete_rewrites_to_tombstone(self):
        self.adapter.execute('A', "DELETE FROM users WHERE id = ?", ('u1',))
        conn = self.adapter.peers['A']
        cur = conn.execute("SELECT tombstone, delete_ts FROM users WHERE id = 'u1'")
        row = cur.fetchone()
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], 2)  # clock was 1 after insert, now 2

    def test_delete_increments_clock(self):
        self.assertEqual(self.adapter.clocks['A'], 1)
        self.adapter.execute('A', "DELETE FROM users WHERE id = ?", ('u1',))
        self.assertEqual(self.adapter.clocks['A'], 2)

    def test_select_passes_through(self):
        conn = self.adapter.peers['A']
        cur = conn.execute("SELECT id, email FROM users WHERE id = ?", ('u1',))
        row = cur.fetchone()
        self.assertEqual(row, ('u1', 'alice@x.com'))


if __name__ == '__main__':
    unittest.main()
