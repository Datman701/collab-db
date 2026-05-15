"""Tests for Task 2: Schema Introspection Engine."""
import sys
import os
import unittest
import sqlite3

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'bench-p01-crdt'))
sys.path.insert(0, PROJECT_ROOT)

from src.team_adapter import TeamAdapter


USERS_DDL = """CREATE TABLE users (
     id    TEXT PRIMARY KEY,
     email TEXT NOT NULL UNIQUE,
     name  TEXT
   )"""

ORDERS_DDL = """CREATE TABLE orders (
     id          TEXT PRIMARY KEY,
     user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
     status      TEXT NOT NULL,
     total_cents INTEGER NOT NULL DEFAULT 0
   )"""


class TestSchemaIntrospection(unittest.TestCase):
    def test_introspect_users_public_columns(self):
        adapter = TeamAdapter()
        result = adapter._introspect_schema(USERS_DDL)
        self.assertEqual(result['public_columns'], ['id', 'email', 'name'])

    def test_introspect_users_pk_columns(self):
        adapter = TeamAdapter()
        result = adapter._introspect_schema(USERS_DDL)
        self.assertEqual(result['pk_columns'], ['id'])

    def test_introspect_users_unique_columns(self):
        adapter = TeamAdapter()
        result = adapter._introspect_schema(USERS_DDL)
        self.assertEqual(result['unique_columns'], {'email'})

    def test_introspect_orders_unique_columns(self):
        adapter = TeamAdapter()
        result = adapter._introspect_schema(ORDERS_DDL)
        # orders has no UNIQUE columns
        self.assertEqual(result['unique_columns'], set())

    def test_introspect_orders_fk_stripped(self):
        adapter = TeamAdapter()
        result = adapter._introspect_schema(ORDERS_DDL)
        # ON DELETE CASCADE should be stripped, but FK reference kept
        internal = result['internal_ddl']
        self.assertIn('REFERENCES users(id)', internal)
        self.assertNotIn('ON DELETE CASCADE', internal)

    def test_introspect_users_internal_ddl_executable(self):
        adapter = TeamAdapter()
        result = adapter._introspect_schema(USERS_DDL)
        internal = result['internal_ddl']
        # Verify it can be executed on a temp connection
        conn = sqlite3.connect(':memory:')
        conn.execute(internal)
        cur = conn.execute("PRAGMA table_info(users)")
        cols = [row[1] for row in cur.fetchall()]
        expected = [
            'id', 'email', 'email_ts', 'email_peer',
            'name', 'name_ts', 'name_peer',
            'tombstone', 'delete_ts', 'conflicted'
        ]
        self.assertEqual(cols, expected)
        conn.close()

    def test_introspect_orders_internal_ddl_executable(self):
        adapter = TeamAdapter()
        result = adapter._introspect_schema(ORDERS_DDL)
        internal = result['internal_ddl']
        conn = sqlite3.connect(':memory:')
        conn.execute(internal)
        cur = conn.execute("PRAGMA table_info(orders)")
        cols = [row[1] for row in cur.fetchall()]
        expected = [
            'id', 'user_id', 'user_id_ts', 'user_id_peer',
            'status', 'status_ts', 'status_peer',
            'total_cents', 'total_cents_ts', 'total_cents_peer',
            'tombstone', 'delete_ts', 'conflicted'
        ]
        self.assertEqual(cols, expected)
        conn.close()

    def test_introspect_users_internal_ddl_no_unique(self):
        adapter = TeamAdapter()
        result = adapter._introspect_schema(USERS_DDL)
        internal = result['internal_ddl']
        self.assertNotIn('UNIQUE', internal)


if __name__ == '__main__':
    unittest.main()
