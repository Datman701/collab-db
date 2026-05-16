#!/usr/bin/env python3
"""CLI mode for the TeamAdapter engine with local HTTP sync and offline storage."""
from __future__ import annotations

import argparse
import http.server
import json
import socketserver
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from src.team_adapter import TeamAdapter


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class CLIHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length == 0:
            return {}
        raw = self.rfile.read(content_length)
        return json.loads(raw.decode("utf-8"))

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path == "/status":
            server = self.server
            self._send_json({
                "peer_id": server.peer_id,
                "remote_peers": server.remote_peers,
                "status": "ok",
            })
            return
        self.send_error(404, "Not found")

    def do_POST(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path == "/execute":
            self._handle_execute()
        elif path == "/query":
            self._handle_query()
        elif path == "/sync":
            self._handle_sync_request()
        else:
            self.send_error(404, "Not found")

    def _handle_execute(self) -> None:
        body = self._read_json()
        peer = body.get("peer", self.server.peer_id)
        sql = body.get("sql", "")
        params = tuple(body.get("params", []))
        try:
            self.server.adapter.execute(peer, sql, params)
            self._send_json({"status": "ok"})
        except Exception as exc:
            self._send_json({"status": "error", "error": str(exc)}, status=500)

    def _handle_query(self) -> None:
        body = self._read_json()
        peer = body.get("peer", self.server.peer_id)
        sql = body.get("sql", "")
        params = tuple(body.get("params", []))
        try:
            columns, rows = self.server.adapter.query(peer, sql, params)
            rows_json = [
                [row if row is None or isinstance(row, (str, int, float, bool)) else str(row) for row in row_tuple]
                for row_tuple in rows
            ]
            self._send_json({"columns": columns, "rows": rows_json})
        except Exception as exc:
            self._send_json({"status": "error", "error": str(exc)}, status=500)

    def _handle_sync_request(self) -> None:
        body = self._read_json()
        peer_id = body.get("peer_id", self.server.peer_id)
        state = body.get("state")
        if state is None:
            self._send_json({"status": "error", "error": "Missing state payload"}, status=400)
            return
        try:
            self.server.adapter.import_peer_state(peer_id, state)
            # Notify locally that we received a sync from a remote peer
            try:
                addr = f"{self.client_address[0]}:{self.client_address[1]}"
            except Exception:
                addr = "unknown"
            print(f"[sync] received sync from {addr} for peer {peer_id}")
            response_state = self.server.adapter.export_peer_state(peer_id)
            self._send_json({"peer_state": response_state})
        except Exception as exc:
            self._send_json({"status": "error", "error": str(exc)}, status=500)

    def log_message(self, format: str, *args: Any) -> None:
        # Silence the default logging; CLI prints its own progress.
        return


def load_schema_file(schema_file: str) -> list[str]:
    text = Path(schema_file).read_text(encoding="utf-8")
    stmts = [stmt.strip() for stmt in text.split(";") if stmt.strip()]
    return stmts


def pretty_print_query(columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    if not columns:
        print("(no columns returned)")
        return
    widths = [len(col) for col in columns]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(str(value)))
    separator = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header = "| " + " | ".join(col.ljust(widths[idx]) for idx, col in enumerate(columns)) + " |"
    print(separator)
    print(header)
    print(separator)
    for row in rows:
        line = "| " + " | ".join(str(value).ljust(widths[idx]) for idx, value in enumerate(row)) + " |"
        print(line)
    print(separator)
    print(f"{len(rows)} row(s)")


def perform_remote_sync(adapter: TeamAdapter, peer_id: str, remote_url: str) -> bool:
    sync_url = remote_url.rstrip("/") + "/sync"
    payload = {
        "peer_id": peer_id,
        "state": adapter.export_peer_state(peer_id),
    }
    data = json.dumps(payload, default=str).encode("utf-8")
    request = urllib.request.Request(
        sync_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            response_text = response.read().decode("utf-8")
            body = json.loads(response_text)
            remote_state = body.get("peer_state")
            if remote_state is None:
                print(f"[sync] remote did not return peer_state: {body}")
                return False
            adapter.import_peer_state(peer_id, remote_state)
            print(f"[sync] synced with {remote_url}")
            return True
    except urllib.error.URLError as exc:
        print(f"[sync] could not reach {remote_url}: {exc}")
        return False
    except Exception as exc:
        print(f"[sync] failed syncing with {remote_url}: {exc}")
        return False


def sync_worker(adapter: TeamAdapter, peer_id: str, remote_peers: list[str], interval: int, stop_event: threading.Event) -> None:
    while not stop_event.wait(interval):
        for remote in remote_peers:
            if perform_remote_sync(adapter, peer_id, remote):
                print(f"[sync] synced with {remote}")


def repl(adapter: TeamAdapter, peer_id: str, remote_peers: list[str]) -> None:
    print("Entering SQL CLI mode. Type :help for commands.")
    while True:
        try:
            line = input(f"{peer_id}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        if line.startswith(":"):
            parts = line[1:].split()
            cmd = parts[0].lower()
            if cmd in ("exit", "quit"):
                break
            if cmd == "help":
                print("Commands:")
                print("  :help            Show this help")
                print("  :exit            Quit")
                print("  :status          Show CLI status")
                print("  :sync [url]      Trigger manual sync")
                print("  :remotes         Show configured remote peers")
                continue
            if cmd == "status":
                print(f"peer_id: {peer_id}")
                print(f"remote peers: {', '.join(remote_peers) or '(none)'}")
                continue
            if cmd == "remotes":
                for remote in remote_peers:
                    print(remote)
                continue
            if cmd == "sync":
                if len(parts) == 1:
                    if not remote_peers:
                        print("No remote peers configured.")
                        continue
                    for remote in remote_peers:
                        perform_remote_sync(adapter, peer_id, remote)
                else:
                    perform_remote_sync(adapter, peer_id, parts[1])
                continue
            print(f"Unknown command: {line}. Type :help.")
            continue

        try:
            if line.strip().upper().startswith("SELECT"):
                columns, rows = adapter.query(peer_id, line)
                pretty_print_query(columns, rows)
            else:
                adapter.execute(peer_id, line)
                print("OK")
        except Exception as exc:
            print(f"ERROR: {exc}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Local SQL CLI with peer sync and offline storage")
    ap.add_argument("--host", default="0.0.0.0", help="Host to bind HTTP server")
    ap.add_argument("--port", type=int, default=8080, help="Port for HTTP server")
    ap.add_argument("--peer", default="cli", help="Local peer id")
    ap.add_argument("--storage-path", default="cli.db", help="SQLite path for local peer storage")
    ap.add_argument("--remote-peer", action="append", default=[], help="Remote peer HTTP URL for periodic sync")
    ap.add_argument("--sync-interval", type=int, default=10, help="Seconds between automatic sync attempts")
    ap.add_argument("--schema-file", help="Optional SQL schema file to apply at startup")
    ap.add_argument("--no-server", action="store_true", help="Do not start the HTTP server")
    args = ap.parse_args(argv)

    adapter = TeamAdapter(peer_db_paths={args.peer: args.storage_path})
    adapter.open_peer(args.peer)

    if args.schema_file:
        stmts = load_schema_file(args.schema_file)
        if stmts:
            adapter.apply_schema(args.peer, stmts)
            print(f"Applied schema from {args.schema_file}")

    server: ThreadedHTTPServer | None = None
    sync_stop = threading.Event()
    sync_thread: threading.Thread | None = None

    if not args.no_server:
        server = ThreadedHTTPServer((args.host, args.port), CLIHTTPRequestHandler)
        server.adapter = adapter
        server.peer_id = args.peer
        server.remote_peers = args.remote_peer
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        print(f"HTTP server listening at http://{args.host}:{args.port}/")

    if args.remote_peer:
        sync_thread = threading.Thread(
            target=sync_worker,
            args=(adapter, args.peer, args.remote_peer, args.sync_interval, sync_stop),
            daemon=True,
        )
        sync_thread.start()
        print(f"Automatic sync enabled for: {', '.join(args.remote_peer)}")

    try:
        repl(adapter, args.peer, args.remote_peer)
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        if sync_thread is not None:
            sync_stop.set()
        adapter.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
