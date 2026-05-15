"""
Team adapter for P-01 (CRDT-Native OLTP).

Implements a local-first relational database adapter with deterministic
offline replication using per-cell metadata in SQLite.
"""
from __future__ import annotations

import sqlite3
from typing import Any

import sys
import os

# When this module is imported from within bench-p01-crdt/adapters/team.py,
# we need to ensure the bench-p01-crdt directory is on sys.path so that
# `from adapter import Adapter` works.
_BENCH_DIR = os.path.join(os.path.dirname(__file__), '..', 'bench-p01-crdt')
if _BENCH_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(_BENCH_DIR))

from adapter import Adapter


class TeamAdapter(Adapter):
    def __init__(self) -> None:
        self.peers: dict[str, sqlite3.Connection] = {}
        self.public_columns: dict[str, dict[str, list[str]]] = {}
        self.pk_columns: dict[str, dict[str, list[str]]] = {}
        self.unique_columns: dict[str, dict[str, set[str]]] = {}
        self.registered_tables: dict[str, list[str]] = {}
        self.clocks: dict[str, int] = {}
        self._table_schemas: dict[str, dict] = {}

    def open_peer(self, peer_id: str) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys = ON")
        self.peers[peer_id] = conn
        self.public_columns[peer_id] = {}
        self.pk_columns[peer_id] = {}
        self.unique_columns[peer_id] = {}
        self.registered_tables[peer_id] = []
        self.clocks[peer_id] = 0

    def _introspect_schema(self, ddl: str) -> dict:
        """Introspect a CREATE TABLE DDL and return internal schema info."""
        import re

        # Extract table name
        match = re.search(r'CREATE\s+TABLE\s+(\w+)', ddl, re.IGNORECASE)
        if not match:
            raise ValueError(f"Could not extract table name from DDL: {ddl}")
        table_name = match.group(1)

        # Execute on temp connection
        temp_conn = sqlite3.connect(':memory:')
        temp_conn.execute(ddl)

        # Get column info: (cid, name, type, notnull, dflt_value, pk)
        cur = temp_conn.execute(f"PRAGMA table_info({table_name})")
        cols = cur.fetchall()

        # Get unique columns from indexes with origin='u'
        unique_cols = set()
        cur = temp_conn.execute(f"PRAGMA index_list({table_name})")
        indexes = cur.fetchall()  # (seq, name, unique, origin, partial)
        for idx in indexes:
            if idx[2] == 1 and idx[3] == 'u':  # unique and origin is constraint
                cur2 = temp_conn.execute(f"PRAGMA index_info({idx[1]})")
                for info in cur2.fetchall():
                    unique_cols.add(info[2])  # column name

        # Get FK info: (id, seq, table, from, to, on_update, on_delete, match)
        cur = temp_conn.execute(f"PRAGMA foreign_key_list({table_name})")
        fks = cur.fetchall()

        temp_conn.close()

        public_columns = []
        pk_columns = []
        col_defs = []

        for col in cols:
            cid, name, ctype, notnull, dflt, pk = col
            public_columns.append(name)
            if pk:
                pk_columns.append(name)
                pk_flag = " PRIMARY KEY" if len([c for c in cols if c[5]]) == 1 else ""
                notnull_flag = " NOT NULL" if notnull else ""
                dflt_flag = f" DEFAULT {dflt}" if dflt is not None else ""
                col_defs.append(f"    {name} {ctype}{notnull_flag}{dflt_flag}{pk_flag}")
            else:
                notnull_flag = " NOT NULL" if notnull else ""
                dflt_flag = f" DEFAULT {dflt}" if dflt is not None else ""
                col_defs.append(f"    {name} {ctype}{notnull_flag}{dflt_flag}")
                col_defs.append(f"    {name}_ts INTEGER DEFAULT 0")
                col_defs.append(f"    {name}_peer TEXT DEFAULT ''")

        # Append row-level metadata columns
        col_defs.append("    tombstone INTEGER DEFAULT 0")
        col_defs.append("    delete_ts INTEGER DEFAULT 0")
        col_defs.append("    conflicted INTEGER DEFAULT 0")

        # Composite PK clause
        if len(pk_columns) > 1:
            col_defs.append(f"    PRIMARY KEY ({', '.join(pk_columns)})")

        # FK clauses (strip ON DELETE CASCADE, keep reference)
        fk_groups = {}
        for fk in fks:
            fk_id, seq, ref_table, from_col, to_col, on_update, on_delete, match = fk
            if fk_id not in fk_groups:
                fk_groups[fk_id] = {'table': ref_table, 'cols': []}
            fk_groups[fk_id]['cols'].append((from_col, to_col))

        for group in fk_groups.values():
            ref_table = group['table']
            col_pairs = group['cols']
            from_cols = ', '.join(p[0] for p in col_pairs)
            to_cols = ', '.join(p[1] for p in col_pairs)
            col_defs.append(f"    FOREIGN KEY ({from_cols}) REFERENCES {ref_table}({to_cols})")

        internal_ddl = f"CREATE TABLE {table_name} (\n" + ",\n".join(col_defs) + "\n)"

        return {
            'internal_ddl': internal_ddl,
            'public_columns': public_columns,
            'pk_columns': pk_columns,
            'unique_columns': unique_cols,
        }

    def apply_schema(self, peer_id: str, stmts: list[str]) -> None:
        import re
        conn = self.peers[peer_id]
        for stmt in stmts:
            stmt_stripped = stmt.strip()
            if re.match(r'CREATE\s+TABLE', stmt_stripped, re.IGNORECASE):
                info = self._introspect_schema(stmt_stripped)
                table_name = re.search(r'CREATE\s+TABLE\s+(\w+)', stmt_stripped, re.IGNORECASE).group(1)
                conn.execute(info['internal_ddl'])
                self.public_columns[peer_id][table_name] = info['public_columns']
                self.pk_columns[peer_id][table_name] = info['pk_columns']
                self.unique_columns[peer_id][table_name] = info['unique_columns']
                self.registered_tables[peer_id].append(table_name)
                # Store globally for auto-creation during sync
                self._table_schemas[table_name] = info
            elif re.match(r'CREATE\s+INDEX', stmt_stripped, re.IGNORECASE):
                conn.execute(stmt_stripped)
            else:
                # Unsupported DDL — pass through for now
                conn.execute(stmt_stripped)
        conn.commit()

    def execute(self, peer_id: str, sql: str, params: tuple[Any, ...] = ()) -> None:
        import re
        conn = self.peers[peer_id]
        peer = peer_id
        
        # Try INSERT rewrite
        insert_match = re.match(
            r"INSERT\s+INTO\s+(\w+)\s+\(([^)]+)\)\s+VALUES\s+\(([^)]+)\)",
            sql.strip(),
            re.IGNORECASE,
        )
        if insert_match:
            table_name = insert_match.group(1)
            public_cols = [c.strip() for c in insert_match.group(2).split(",")]
            placeholders = [p.strip() for p in insert_match.group(3).split(",")]
            
            pk_cols = self.pk_columns[peer_id].get(table_name, [])
            
            new_cols = []
            new_placeholders = []
            new_params = []
            
            self.clocks[peer_id] += 1
            ts = self.clocks[peer_id]
            
            for i, col in enumerate(public_cols):
                new_cols.append(col)
                new_placeholders.append(placeholders[i])
                new_params.append(params[i])
                if col not in pk_cols:
                    new_cols.append(f"{col}_ts")
                    new_cols.append(f"{col}_peer")
                    new_placeholders.append("?")
                    new_placeholders.append("?")
                    new_params.append(ts)
                    new_params.append(peer)
            
            rewritten_sql = f"INSERT INTO {table_name} ({', '.join(new_cols)}) VALUES ({', '.join(new_placeholders)})"
            conn.execute(rewritten_sql, tuple(new_params))
            conn.commit()
            return
        
        # Try single-column UPDATE rewrite
        update_match = re.match(
            r"UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*\?\s+WHERE\s+(.+)",
            sql.strip(),
            re.IGNORECASE,
        )
        if update_match:
            table_name = update_match.group(1)
            col = update_match.group(2)
            where_clause = update_match.group(3)
            
            self.clocks[peer_id] += 1
            ts = self.clocks[peer_id]
            
            rewritten_sql = f"UPDATE {table_name} SET {col} = ?, {col}_ts = ?, {col}_peer = ? WHERE {where_clause}"
            new_params = (params[0], ts, peer) + params[1:]
            conn.execute(rewritten_sql, new_params)
            conn.commit()
            return
        
        # Try DELETE rewrite (tombstone UPDATE)
        delete_match = re.match(
            r"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(.+)",
            sql.strip(),
            re.IGNORECASE,
        )
        if delete_match:
            table_name = delete_match.group(1)
            where_clause = delete_match.group(2)
            
            self.clocks[peer_id] += 1
            ts = self.clocks[peer_id]
            
            rewritten_sql = f"UPDATE {table_name} SET tombstone = 1, delete_ts = ? WHERE {where_clause}"
            new_params = (ts,) + params
            conn.execute(rewritten_sql, new_params)
            conn.commit()
            return
        
        # Pass through raw for everything else (SELECT, unsupported patterns)
        conn.execute(sql, params)
        conn.commit()

    def _merge_row(self, incoming: dict, local: dict, mutable_cols: list[str]) -> dict:
        """Merge two row dicts using per-cell LWW. Returns merged row dict.
        
        Tombstones are permanent: once a row is deleted, it stays deleted.
        Updates on tombstoned rows update cell metadata but do not resurrect the row.
        This matches the benchmark's expected behavior for FK policy tests.
        """
        merged = dict(local)
        for col in mutable_cols:
            incoming_ts = incoming.get(f"{col}_ts", 0)
            local_ts = local.get(f"{col}_ts", 0)
            incoming_peer = incoming.get(f"{col}_peer", "")
            local_peer = local.get(f"{col}_peer", "")
            
            if incoming_ts > local_ts:
                merged[col] = incoming[col]
                merged[f"{col}_ts"] = incoming_ts
                merged[f"{col}_peer"] = incoming_peer
            elif incoming_ts == local_ts and incoming_peer > local_peer:
                merged[col] = incoming[col]
                merged[f"{col}_ts"] = incoming_ts
                merged[f"{col}_peer"] = incoming_peer
        
        # Merge tombstone metadata explicitly
        incoming_delete_ts = incoming.get("delete_ts", 0)
        local_delete_ts = local.get("delete_ts", 0)
        if incoming_delete_ts > local_delete_ts:
            merged["tombstone"] = incoming.get("tombstone", 0)
            merged["delete_ts"] = incoming_delete_ts
        elif incoming_delete_ts == local_delete_ts and incoming_delete_ts > 0:
            # If timestamps are equal and non-zero, tombstone wins if either side is tombstoned
            merged["tombstone"] = max(merged.get("tombstone", 0), incoming.get("tombstone", 0))
        
        return merged

    def _sync_one_way(self, src_peer: str, dst_peer: str) -> None:
        """Merge all state from src_peer into dst_peer."""
        src_conn = self.peers[src_peer]
        dst_conn = self.peers[dst_peer]
        
        # Build sync payload from source
        schema_manifest = {}
        rows_payload = {}
        for table in self.registered_tables.get(src_peer, []):
            schema_manifest[table] = self.public_columns[src_peer][table]
            # Select ALL columns (including metadata)
            cur = src_conn.execute(f"SELECT * FROM {table}")
            cols = [d[0] for d in cur.description]
            rows_payload[table] = [dict(zip(cols, row)) for row in cur.fetchall()]
        
        # Apply to destination
        for table, rows in rows_payload.items():
            # Auto-create table if absent
            if table not in self.registered_tables.get(dst_peer, []):
                cur = dst_conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,)
                )
                if cur.fetchone() is None:
                    # Create from global schema cache
                    if table in self._table_schemas:
                        info = self._table_schemas[table]
                        dst_conn.execute(info['internal_ddl'])
                        self.public_columns[dst_peer][table] = info['public_columns']
                        self.pk_columns[dst_peer][table] = info['pk_columns']
                        self.unique_columns[dst_peer][table] = info['unique_columns']
                        self.registered_tables[dst_peer].append(table)
                else:
                    # Table exists physically but not registered — register it
                    if table in self._table_schemas:
                        info = self._table_schemas[table]
                        self.public_columns[dst_peer][table] = info['public_columns']
                        self.pk_columns[dst_peer][table] = info['pk_columns']
                        self.unique_columns[dst_peer][table] = info['unique_columns']
                        self.registered_tables[dst_peer].append(table)
            
            pk_cols = self.pk_columns[src_peer].get(table, [])
            mutable_cols = [c for c in self.public_columns[src_peer].get(table, []) if c not in pk_cols]
            
            for incoming_row in rows:
                # Build PK where clause
                pk_values = [incoming_row[pk] for pk in pk_cols]
                if pk_cols:
                    where = " AND ".join(f"{pk} = ?" for pk in pk_cols)
                    cur = dst_conn.execute(f"SELECT * FROM {table} WHERE {where}", pk_values)
                else:
                    cur = dst_conn.execute(f"SELECT * FROM {table} WHERE rowid = ?", (incoming_row.get('rowid'),))
                
                local_row = cur.fetchone()
                if local_row is None:
                    # Insert incoming row as-is
                    all_cols = list(incoming_row.keys())
                    placeholders = ",".join("?" * len(all_cols))
                    dst_conn.execute(
                        f"INSERT OR REPLACE INTO {table} ({','.join(all_cols)}) VALUES ({placeholders})",
                        tuple(incoming_row.values()),
                    )
                else:
                    # Merge
                    cols = [d[0] for d in cur.description]
                    local_dict = dict(zip(cols, local_row))
                    merged = self._merge_row(incoming_row, local_dict, mutable_cols)
                    all_cols = list(merged.keys())
                    placeholders = ",".join("?" * len(all_cols))
                    dst_conn.execute(
                        f"INSERT OR REPLACE INTO {table} ({','.join(all_cols)}) VALUES ({placeholders})",
                        tuple(merged.values()),
                    )
        
        dst_conn.commit()

    def sync(self, peer_a: str, peer_b: str) -> None:
        self._sync_one_way(peer_b, peer_a)
        self._sync_one_way(peer_a, peer_b)
        self._uniqueness_scan(peer_a)
        self._uniqueness_scan(peer_b)

    def _uniqueness_scan(self, peer_id: str) -> None:
        """After sync, scan for duplicate unique values and mark losers conflicted."""
        conn = self.peers[peer_id]
        for table in self.registered_tables.get(peer_id, []):
            unique_cols = self.unique_columns[peer_id].get(table, set())
            pk_cols = self.pk_columns[peer_id].get(table, [])
            if not unique_cols or not pk_cols:
                continue
            pk_col = pk_cols[0]  # Assume single-column PK
            
            for ucol in unique_cols:
                ts_col = f"{ucol}_ts"
                peer_col = f"{ucol}_peer"
                # Fetch all visible rows with this unique column
                cur = conn.execute(
                    f"SELECT {pk_col}, {ucol}, {ts_col}, {peer_col} FROM {table} WHERE tombstone = 0 AND conflicted = 0"
                )
                rows = cur.fetchall()
                
                # Group by unique column value
                groups: dict[str, list[tuple]] = {}
                for row in rows:
                    pk_val, uval, ts_val, peer_val = row
                    groups.setdefault(uval, []).append((pk_val, ts_val, peer_val))
                
                # For each group with duplicates, mark losers
                for uval, group_rows in groups.items():
                    if len(group_rows) <= 1:
                        continue
                    # Winner: lowest (ts, peer_id)
                    winner = min(group_rows, key=lambda r: (r[1], r[2]))
                    winner_pk = winner[0]
                    for pk_val, ts_val, peer_val in group_rows:
                        if pk_val != winner_pk:
                            conn.execute(
                                f"UPDATE {table} SET conflicted = 1 WHERE {pk_col} = ?",
                                (pk_val,),
                            )
        conn.commit()

    def snapshot_hash(self, peer_id: str) -> str:
        import hashlib, json
        state = self.snapshot_state(peer_id)
        blob = json.dumps(state, sort_keys=True, default=str).encode()
        return hashlib.sha256(blob).hexdigest()

    def snapshot_state(self, peer_id: str) -> dict[str, list[dict[str, Any]]]:
        conn = self.peers[peer_id]
        result: dict[str, list[dict[str, Any]]] = {}
        for table in self.registered_tables.get(peer_id, []):
            public_cols = self.public_columns[peer_id][table]
            pk_cols = self.pk_columns[peer_id][table]
            cols_str = ", ".join(public_cols)
            order_by = ", ".join(pk_cols) if pk_cols else "rowid"
            sql = f"SELECT {cols_str} FROM {table} WHERE tombstone = 0 AND conflicted = 0 ORDER BY {order_by}"
            cur = conn.execute(sql)
            rows = cur.fetchall()
            result[table] = [dict(zip(public_cols, row)) for row in rows]
        return result

    def close(self) -> None:
        for c in self.peers.values():
            c.close()
        self.peers.clear()
