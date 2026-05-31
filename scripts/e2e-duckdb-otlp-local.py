#!/usr/bin/env python3
"""Local duckdb-otlp -> DuckLake -> canardstack query smoke.

This is intentionally a developer harness, not a unit test. It expects a
neighbor checkout of duckdb-otlp with a local release build:

    ../duckdb-otlp/build/release/duckdb
    ../duckdb-otlp/build/release/extension/otlp/otlp.duckdb_extension
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path


OTLP_TOKEN = "dev-token-123456"
CANARDSTACK_TOKEN = "dev-canardstack-key"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_port(port: int, proc: subprocess.Popen[str], log_path: Path) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"duckdb exited before OTLP listener was ready; see {log_path}"
            )
        try:
            with socket.create_connection(("localhost", port), timeout=0.2):
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for localhost:{port}; see {log_path}")


def request(
    method: str,
    url: str,
    token: str,
    data: bytes | None = None,
    content_type: str | None = None,
) -> tuple[int, bytes]:
    headers = {"Authorization": f"Bearer {token}"}
    if content_type is not None:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, resp.read()


def wait_for_health(base_url: str, proc: subprocess.Popen[str], log_path: Path) -> None:
    deadline = time.monotonic() + 20
    url = f"{base_url}/healthz"
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"canardstack exited before /healthz became ready; see {log_path}"
            )
        try:
            status, body = request("GET", url, CANARDSTACK_TOKEN)
            if status == 200 and json.loads(body).get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for {url}; see {log_path}")


def write_sql(proc: subprocess.Popen[str], sql: str) -> None:
    assert proc.stdin is not None
    proc.stdin.write(sql)
    proc.stdin.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb-otlp-root",
        default="../duckdb-otlp",
        help="Path to a duckdb-otlp checkout with build/release artifacts.",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep the temporary DuckLake directory for inspection.",
    )
    args = parser.parse_args()

    repo_root = Path.cwd().resolve()
    otlp_root = (repo_root / args.duckdb_otlp_root).resolve()
    duckdb = otlp_root / "build/release/duckdb"
    extension = otlp_root / "build/release/extension/otlp/otlp.duckdb_extension"
    sample_logs = otlp_root / "test/data/logs_simple.jsonl"

    for path in (duckdb, extension, sample_logs):
        if not path.exists():
            raise FileNotFoundError(path)

    temp_root = Path(tempfile.mkdtemp(prefix="canardstack-duckdb-otlp-"))

    otlp_port = free_port()
    canardstack_port = free_port()
    catalog = temp_root / "metadata.ducklake"
    data_dir = temp_root / "data"
    writer_db = temp_root / "writer.duckdb"
    duckdb_log = temp_root / "duckdb.log"
    canardstack_log = temp_root / "canardstack.log"

    duckdb_proc: subprocess.Popen[str] | None = None
    canardstack_proc: subprocess.Popen[str] | None = None
    try:
        with duckdb_log.open("w") as duckdb_out:
            duckdb_proc = subprocess.Popen(
                [str(duckdb), "-interactive", "-unsigned", str(writer_db)],
                cwd=otlp_root,
                stdin=subprocess.PIPE,
                stdout=duckdb_out,
                stderr=subprocess.STDOUT,
                text=True,
            )
            write_sql(
                duckdb_proc,
                f"""
.bail on
LOAD '{extension}';
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_dir}/');
SELECT * FROM otlp_serve('otlp:localhost:{otlp_port}', catalog := 'lake', token := '{OTLP_TOKEN}');
""",
            )
            wait_for_port(otlp_port, duckdb_proc, duckdb_log)

            status, body = request(
                "POST",
                f"http://localhost:{otlp_port}/v1/logs",
                OTLP_TOKEN,
                data=sample_logs.read_bytes(),
                content_type="application/x-ndjson",
            )
            if status != 202:
                raise RuntimeError(f"OTLP POST returned {status}: {body.decode()}")

            write_sql(
                duckdb_proc,
                f"""
SELECT * FROM otlp_flush('otlp:localhost:{otlp_port}');
SELECT * FROM otlp_stop('otlp:localhost:{otlp_port}');
.quit
""",
            )
            duckdb_proc.wait(timeout=15)
            if duckdb_proc.returncode != 0:
                raise RuntimeError(f"duckdb failed; see {duckdb_log}")

        env = os.environ.copy()
        env.update(
            {
                "CANARDSTACK_DATA_DIR": str(temp_root / "canardstack"),
                "CANARDSTACK_DUCKLAKE_ATTACH_URI": f"ducklake:{catalog}",
                "CANARDSTACK_DUCKLAKE_DATA_PATH": f"{data_dir}/",
                "CANARDSTACK_API_KEY": CANARDSTACK_TOKEN,
                "CANARDSTACK_ADMIN_API_KEY": "dev-canardstack-admin-key",
            }
        )
        with canardstack_log.open("w") as canardstack_out:
            canardstack_proc = subprocess.Popen(
                [
                    "cargo",
                    "run",
                    "--quiet",
                    "--",
                    "serve",
                    "--listen",
                    f"localhost:{canardstack_port}",
                ],
                cwd=repo_root,
                env=env,
                stdout=canardstack_out,
                stderr=subprocess.STDOUT,
                text=True,
            )
            base_url = f"http://localhost:{canardstack_port}"
            wait_for_health(base_url, canardstack_proc, canardstack_log)

            query = urllib.parse.urlencode(
                {
                    "query": '{service_name="test-service"}',
                    "start": "1640000000",
                    "end": "1640000100",
                    "limit": "10",
                }
            )
            _, body = request(
                "GET",
                f"{base_url}/loki/api/v1/query_range?{query}",
                CANARDSTACK_TOKEN,
            )
            payload = json.loads(body)
            bodies = [
                value[1]
                for stream in payload["data"]["result"]
                for value in stream["values"]
            ]
            expected = {
                "Application started",
                "High memory usage detected",
                "Database connection failed",
            }
            missing = expected.difference(bodies)
            if missing:
                raise RuntimeError(
                    f"Loki response missed expected log bodies {sorted(missing)}: {payload}"
                )

        print(
            "ok: duckdb-otlp wrote 3 OTLP log rows to DuckLake; "
            "canardstack queried them through Loki query_range"
        )
        if args.keep_temp:
            print(f"temp_root={temp_root}")
        return 0
    finally:
        if canardstack_proc is not None and canardstack_proc.poll() is None:
            canardstack_proc.send_signal(signal.SIGINT)
            try:
                canardstack_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                canardstack_proc.kill()
        if duckdb_proc is not None and duckdb_proc.poll() is None:
            try:
                write_sql(
                    duckdb_proc,
                    f"SELECT * FROM otlp_stop('otlp:localhost:{otlp_port}');\n.quit\n",
                )
                duckdb_proc.wait(timeout=5)
            except Exception:
                duckdb_proc.kill()
        if not args.keep_temp:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
