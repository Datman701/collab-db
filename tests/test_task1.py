"""Tests for Task 1: Adapter Skeleton + Peer Lifecycle."""
import sqlite3
import sys
import os
import unittest

# Add bench-p01-crdt to path so we can import adapters
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'bench-p01-crdt'))
sys.path.insert(0, PROJECT_ROOT)

from adapter import Adapter
from src.team_adapter import TeamAdapter


class TestAdapterSkeleton(unittest.TestCase):
    def test_import_and_inheritance(self):
        self.assertTrue(issubclass(TeamAdapter, Adapter))

    def test_open_peer_creates_connection(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        # After opening, peer should exist in internal storage
        self.assertIn('A', adapter.peers)
        # Should be a sqlite3 Connection
        self.assertIsInstance(adapter.peers['A'], sqlite3.Connection)
        adapter.close()

    def test_open_multiple_peers(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        self.assertEqual(len(adapter.peers), 2)
        adapter.close()

    def test_close_clears_peers(self):
        adapter = TeamAdapter()
        adapter.open_peer('A')
        adapter.open_peer('B')
        adapter.close()
        self.assertEqual(len(adapter.peers), 0)

    def test_close_no_exception_when_empty(self):
        adapter = TeamAdapter()
        adapter.close()  # Should not raise


if __name__ == '__main__':
    unittest.main()
