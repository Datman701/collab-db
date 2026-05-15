"""Debug script for sync merge."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bench-p01-crdt'))

from adapters.team import TeamAdapter

SCHEMA = [
    """CREATE TABLE users (
         id    TEXT PRIMARY KEY,
         email TEXT NOT NULL UNIQUE,
         name  TEXT
       )""",
]

adapter = TeamAdapter()
adapter.open_peer('A')
adapter.open_peer('B')
adapter.apply_schema('A', SCHEMA)
adapter.apply_schema('B', SCHEMA)

adapter.execute('A', "INSERT INTO users (id, email, name) VALUES (?, ?, ?)", ('u1', 'alice@x.com', 'Alice'))
adapter.sync('A', 'B')

adapter.execute('A', "UPDATE users SET name = ? WHERE id = ?", ('Alice Cooper', 'u1'))
adapter.execute('B', "UPDATE users SET name = ? WHERE id = ?", ('Alicia', 'u1'))

print("Before sync:")
conn = adapter.peers['A']
cur = conn.execute("SELECT * FROM users WHERE id = 'u1'")
print("A:", dict(zip([d[0] for d in cur.description], cur.fetchone())))
conn = adapter.peers['B']
cur = conn.execute("SELECT * FROM users WHERE id = 'u1'")
print("B:", dict(zip([d[0] for d in cur.description], cur.fetchone())))

adapter.sync('A', 'B')

print("After sync:")
conn = adapter.peers['A']
cur = conn.execute("SELECT * FROM users WHERE id = 'u1'")
print("A:", dict(zip([d[0] for d in cur.description], cur.fetchone())))
conn = adapter.peers['B']
cur = conn.execute("SELECT * FROM users WHERE id = 'u1'")
print("B:", dict(zip([d[0] for d in cur.description], cur.fetchone())))

adapter.close()
