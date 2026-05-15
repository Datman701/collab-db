"""Trace the reference scenario to debug."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from adapters.team import TeamAdapter
from scenarios.reference import SCHEMA, PEERS, OPERATIONS, FINAL_SYNC_ORDER, Stmt, Sync

adapter = TeamAdapter()
for p in PEERS:
    adapter.open_peer(p)
    adapter.apply_schema(p, SCHEMA)

for op in OPERATIONS:
    if isinstance(op, Stmt):
        adapter.execute(op.peer, op.sql, op.params)
    else:
        adapter.sync(op.a, op.b)

for a, b in FINAL_SYNC_ORDER:
    adapter.sync(a, b)

for p in PEERS:
    print(f"=== Peer {p} ===")
    state = adapter.snapshot_state(p)
    for table, rows in state.items():
        print(f"  {table}:")
        for row in rows:
            print(f"    {row}")
    
    # Also show tombstoned rows
    conn = adapter.peers[p]
    cur = conn.execute("SELECT * FROM users")
    cols = [d[0] for d in cur.description]
    print(f"  Raw users:")
    for row in cur.fetchall():
        print(f"    {dict(zip(cols, row))}")

adapter.close()
