"""Tests for Task 9: Post-Sync Uniqueness Scan."""
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


class TestUniquenessScan(unittest.TestCase):
    def test_duplicate_email_marks_loser_conflicted(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        adapter.execute('B', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u2', 'alice@x.com', 'Bob'))
        adapter.sync('A', 'B')
        state = adapter.snapshot_state('A')
        emails = [r['email'] for r in state.get('users', [])]
        self.assertEqual(len(set(emails)), len(emails))
        adapter.close()

    def test_higher_ts_loses(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)
        # A inserts at ts=1
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        # Sync so B gets it
        adapter.sync('A', 'B')
        # B updates email at ts=1 (B's clock was 0), so B has email_ts=1
        adapter.execute('B', "UPDATE users SET email = ? WHERE id = ?", ('alice@x.com', 'u2'))
        # Wait, u2 doesn't exist on B. Let me insert u2 on B first
        adapter.execute('B', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u2', 'bob@x.com', 'Bob'))
        # Then update u2's email to alice@x.com
        adapter.execute('B', "UPDATE users SET email = ? WHERE id = ?", ('alice@x.com', 'u2'))
        # Now A has u1 with email_ts=1, B has u2 with email_ts=1
        # After sync, they have the same ts. Lower peer_id wins (A < B)
        adapter.sync('A', 'B')
        state = adapter.snapshot_state('A')
        # Both rows should be visible to satisfy data-preservation
        self.assertEqual(len(state['users']), 2)
        
        # A's row should win because 'A' < 'B' lexicographically
        winner = next(u for u in state['users'] if "conflict_" not in u['email'])
        self.assertEqual(winner['id'], 'u1')
        self.assertEqual(winner['email'], 'alice@x.com')
        adapter.close()

    def test_equal_ts_lower_peer_wins(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.apply_schema('A', SCHEMA)
        adapter.apply_schema('B', SCHEMA)
        # Insert same email with same ts on both peers
        # Need to manually set metadata to force equal ts
        adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
        adapter.execute('B', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u2', 'alice@x.com', 'Bob'))
        # Manually set equal timestamps via raw SQL
        conn = adapter.peers['A']
        conn.execute("UPDATE users SET email_ts = 5, email_peer = 'X' WHERE id = 'u1'")
        conn.commit()
        conn = adapter.peers['B']
        conn.execute("UPDATE users SET email_ts = 5, email_peer = 'Y' WHERE id = 'u2'")
        conn.commit()
        adapter.sync('A', 'B')
        state = adapter.snapshot_state('A')
        self.assertEqual(len(state['users']), 2)
        # Lower peer 'X' should win over 'Y'
        winner = next(u for u in state['users'] if "conflict_" not in u['email'])
        self.assertEqual(winner['id'], 'u1')
        adapter.close()


if __name__ == '__main__':
    unittest.main()
