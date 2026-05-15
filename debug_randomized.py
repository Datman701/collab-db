"""Debug randomized scenario."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bench-p01-crdt'))

from adapters.team import TeamAdapter
from scenarios.randomized import RandomizedConfig, generate
from scenarios.reference import SCHEMA

SEED = 101

adapter = TeamAdapter()
cfg = RandomizedConfig(seed=SEED, n_peers=4, n_ops=80)
peers, ops, tail = generate(cfg)
scoped = {p: f"R{SEED}:{p}" for p in peers}

for p in peers:
    adapter.open_peer(scoped[p])
    adapter.apply_schema(scoped[p], SCHEMA)

for op in ops:
    if hasattr(op, 'peer'):
        adapter.execute(scoped[op.peer], op.sql, op.params)
    else:
        adapter.sync(scoped[op.a], scoped[op.b])

for a, b in tail:
    adapter.sync(scoped[a], scoped[b])

print("Hashes after quiescence:")
for p in peers:
    h = adapter.snapshot_hash(scoped[p])
    print(f"  {p}: {h}")

# Show diff between P0 and P2
s0 = adapter.snapshot_state(scoped['P0'])
s2 = adapter.snapshot_state(scoped['P2'])
print("\nP0 state:", s0)
print("P2 state:", s2)

# Show raw tables
for p in ['P0', 'P2']:
    print(f"\n=== Raw {p} ===")
    conn = adapter.peers[scoped[p]]
    cur = conn.execute("SELECT * FROM users")
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        print(f"  {dict(zip(cols, row))}")

adapter.close()
