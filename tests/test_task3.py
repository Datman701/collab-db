"""Tests for Task 3: apply_schema — DDL Interception + Storage."""
import sys
import os
import unittest
import sqlite3

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
    """CREATE TABLE orders (
         id          TEXT PRIMARY KEY,
         user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
         status      TEXT NOT NULL,
         total_cents INTEGER NOT NULL DEFAULT 0
       )""",
    "CREATE INDEX orders_by_user ON orders(user_id, status)",
]


class TestApplySchema(unittest.TestCase):
    def test_apply_schema_creates_tables(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        conn = adapter.peers['A']
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = {row[0] for row in cur.fetchall()}
        self.assertIn('users', tables)
        self.assertIn('orders', tables)
        adapter.close()

    def test_apply_schema_users_internal_columns(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        conn = adapter.peers['A']
        cur = conn.execute("PRAGMA table_info(users)")
        cols = [row[1] for row in cur.fetchall()]
        expected = [
            'id', 'email', 'email_ts', 'email_peer',
            'name', 'name_ts', 'name_peer',
            'tombstone', 'delete_ts', 'conflicted'
        ]
        self.assertEqual(cols, expected)
        adapter.close()

    def test_apply_schema_orders_internal_columns(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        conn = adapter.peers['A']
        cur = conn.execute("PRAGMA table_info(orders)")
        cols = [row[1] for row in cur.fetchall()]
        expected = [
            'id', 'user_id', 'user_id_ts', 'user_id_peer',
            'status', 'status_ts', 'status_peer',
            'total_cents', 'total_cents_ts', 'total_cents_peer',
            'tombstone', 'delete_ts', 'conflicted'
        ]
        self.assertEqual(cols, expected)
        adapter.close()

    def test_apply_schema_stores_public_columns(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        self.assertEqual(adapter.public_columns['A']['users'], ['id', 'email', 'name'])
        self.assertEqual(adapter.public_columns['A']['orders'], ['id', 'user_id', 'status', 'total_cents'])
        adapter.close()

    def test_apply_schema_stores_pk_columns(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        self.assertEqual(adapter.pk_columns['A']['users'], ['id'])
        self.assertEqual(adapter.pk_columns['A']['orders'], ['id'])
        adapter.close()

    def test_apply_schema_stores_unique_columns(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        self.assertEqual(adapter.unique_columns['A']['users'], {'email'})
        adapter.close()

    def test_apply_schema_creates_index(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.apply_schema('A', SCHEMA)
        conn = adapter.peers['A']
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='orders_by_user'")
        self.assertIsNotNone(cur.fetchone())
        adapter.close()


if __name__ == '__main__':
    unittest.main()
