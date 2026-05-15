"""Tests for Task 7: Row Merge Algorithm (Unit)."""
import sys
import os
import unittest

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'bench-p01-crdt'))
sys.path.insert(0, PROJECT_ROOT)

from src.team_adapter import TeamAdapter


class TestRowMerge(unittest.TestCase):
    def setUp(self):
        self.adapter = TeamAdapter()

    def test_incoming_higher_ts_wins(self):
        local = {'id': 'u1', 'name': 'Alice', 'name_ts': 1, 'name_peer': 'A'}
        incoming = {'id': 'u1', 'name': 'Bob', 'name_ts': 2, 'name_peer': 'B'}
        merged = self.adapter._merge_row(incoming, local, ['name'])
        self.assertEqual(merged['name'], 'Bob')
        self.assertEqual(merged['name_ts'], 2)
        self.assertEqual(merged['name_peer'], 'B')

    def test_local_higher_ts_wins(self):
        local = {'id': 'u1', 'name': 'Alice', 'name_ts': 3, 'name_peer': 'A'}
        incoming = {'id': 'u1', 'name': 'Bob', 'name_ts': 2, 'name_peer': 'B'}
        merged = self.adapter._merge_row(incoming, local, ['name'])
        self.assertEqual(merged['name'], 'Alice')
        self.assertEqual(merged['name_ts'], 3)
        self.assertEqual(merged['name_peer'], 'A')

    def test_equal_ts_larger_peer_wins(self):
        local = {'id': 'u1', 'name': 'Alice', 'name_ts': 2, 'name_peer': 'A'}
        incoming = {'id': 'u1', 'name': 'Bob', 'name_ts': 2, 'name_peer': 'B'}
        merged = self.adapter._merge_row(incoming, local, ['name'])
        self.assertEqual(merged['name'], 'Bob')  # 'B' > 'A' lexicographically
        self.assertEqual(merged['name_peer'], 'B')

    def test_equal_ts_smaller_peer_keeps_local(self):
        local = {'id': 'u1', 'name': 'Alice', 'name_ts': 2, 'name_peer': 'B'}
        incoming = {'id': 'u1', 'name': 'Bob', 'name_ts': 2, 'name_peer': 'A'}
        merged = self.adapter._merge_row(incoming, local, ['name'])
        self.assertEqual(merged['name'], 'Alice')  # 'B' > 'A', so local wins
        self.assertEqual(merged['name_peer'], 'B')

    def test_tombstone_permanent_when_cell_ts_gt_delete_ts(self):
        """Remove-Wins: tombstone stays even if cell ts > delete_ts."""
        local = {'id': 'u1', 'name': 'Alice', 'name_ts': 5, 'name_peer': 'A', 'tombstone': 1, 'delete_ts': 3}
        incoming = {'id': 'u1', 'name': 'Bob', 'name_ts': 2, 'name_peer': 'B'}
        merged = self.adapter._merge_row(incoming, local, ['name'])
        # local cell wins (ts 5 > 2), tombstone is permanent (Remove-Wins)
        self.assertEqual(merged['tombstone'], 1)
        self.assertEqual(merged['delete_ts'], 3)

    def test_tombstone_preserved_when_all_cells_ts_lte_delete_ts(self):
        local = {'id': 'u1', 'name': 'Alice', 'name_ts': 1, 'name_peer': 'A', 'tombstone': 1, 'delete_ts': 5}
        incoming = {'id': 'u1', 'name': 'Bob', 'name_ts': 2, 'name_peer': 'B'}
        merged = self.adapter._merge_row(incoming, local, ['name'])
        # incoming wins cell (ts 2 > 1), but ts 2 <= delete_ts 5 → stays dead
        self.assertEqual(merged['tombstone'], 1)
        self.assertEqual(merged['delete_ts'], 5)

    def test_multi_column_merge(self):
        local = {'id': 'u1', 'name': 'Alice', 'name_ts': 1, 'name_peer': 'A', 'email': 'a@x.com', 'email_ts': 5, 'email_peer': 'A'}
        incoming = {'id': 'u1', 'name': 'Bob', 'name_ts': 3, 'name_peer': 'B', 'email': 'b@x.com', 'email_ts': 2, 'email_peer': 'B'}
        merged = self.adapter._merge_row(incoming, local, ['name', 'email'])
        # name: incoming wins (3 > 1)
        self.assertEqual(merged['name'], 'Bob')
        # email: local wins (5 > 2)
        self.assertEqual(merged['email'], 'a@x.com')

    def test_associative_property(self):
        a = {'id': 'u1', 'name': 'A', 'name_ts': 1, 'name_peer': 'A'}
        b = {'id': 'u1', 'name': 'B', 'name_ts': 2, 'name_peer': 'B'}
        c = {'id': 'u1', 'name': 'C', 'name_ts': 3, 'name_peer': 'C'}
        ab = self.adapter._merge_row(a, b, ['name'])
        abc = self.adapter._merge_row(ab, c, ['name'])
        bc = self.adapter._merge_row(b, c, ['name'])
        abc2 = self.adapter._merge_row(a, bc, ['name'])
        self.assertEqual(abc, abc2)

    def test_commutative_property(self):
        a = {'id': 'u1', 'name': 'A', 'name_ts': 1, 'name_peer': 'A'}
        b = {'id': 'u1', 'name': 'B', 'name_ts': 2, 'name_peer': 'B'}
        ab = self.adapter._merge_row(a, b, ['name'])
        ba = self.adapter._merge_row(b, a, ['name'])
        self.assertEqual(ab, ba)

    def test_idempotent_property(self):
        a = {'id': 'u1', 'name': 'A', 'name_ts': 1, 'name_peer': 'A'}
        aa = self.adapter._merge_row(a, a, ['name'])
        self.assertEqual(aa, a)


if __name__ == '__main__':
    unittest.main()
