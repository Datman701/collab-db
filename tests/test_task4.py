"""Tests for Task 4: execute() — INSERT Rewrite."""
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


class TestInsertRewrite(unittest.TestCase):
    def test_insert_executes_without_error(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        adapter.close()

    def test_insert_populates_metadata_columns(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        conn = adapter.peers['A']
        cur = conn.execute("SELECT * FROM users WHERE id = 'u1'")
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        row_dict = dict(zip(cols, row))
        self.assertEqual(row_dict['email_ts'], 1)
        self.assertEqual(row_dict['email_peer'], 'A')
        self.assertEqual(row_dict['name_ts'], 1)
        self.assertEqual(row_dict['name_peer'], 'A')
        adapter.close()

    def test_insert_increments_clock(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        self.assertEqual(adapter.clocks['A'], 0)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        self.assertEqual(adapter.clocks['A'], 1)
        adapter.close()

    def test_insert_two_rows_increments_clock_once(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u2', 'bob@x.com', 'Bob'))
        self.assertEqual(adapter.clocks['A'], 2)
        adapter.close()

    def test_insert_second_row_has_higher_timestamp(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u2', 'bob@x.com', 'Bob'))
        conn = adapter.peers['A']
        cur = conn.execute("SELECT email_ts FROM users WHERE id = 'u2'")
        self.assertEqual(cur.fetchone()[0], 2)
        adapter.close()


if __name__ == '__main__':
    unittest.main()
