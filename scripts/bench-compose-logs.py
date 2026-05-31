#!/usr/bin/env python3
"""Compose-local OTLP logs ingest benchmark.

This is a small developer harness inspired by the OpenTelemetry Collector
benchmark dimensions: log datapoints/second and average CPU percentage. It is
not the Collector benchmark harness and should not be compared as an equivalent
result without matching hardware, image tags, and payloads.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Iterable


DEFAULT_COMPOSE_FILES = ("compose.yaml", "compose.build.yaml")
DEFAULT_SERVICES = ("ingest", "ducklake-catalog", "canardstack")
DEFAULT_TOKEN = "dev-otlp-token-123456"


@dataclass
class Counters:
    accepted_202: int = 0
    other_status: int = 0
    errors: int = 0


@dataclass
class CpuSample:
    service: str
    cpu_percent: float


def compose_base(args: argparse.Namespace) -> list[str]:
    cmd = ["docker", "compose", "-p", args.project]
    for compose_file in args.compose_file:
        cmd.extend(["-f", compose_file])
    return cmd


def run(cmd: list[str], *, capture: bool = True) -> str:
    proc = subprocess.run(
        cmd,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    return proc.stdout.strip() if capture else ""


def service_containers(args: argparse.Namespace) -> dict[str, str]:
    containers: dict[str, str] = {}
    base = compose_base(args)
    for service in args.cpu_service:
        cid = run([*base, "ps", "-q", service])
        if cid:
            containers[service] = cid.splitlines()[0]
    if not containers:
        raise RuntimeError(
            "no compose service containers found; start the stack before benchmarking"
        )
    return containers


def parse_cpu(value: str) -> float:
    value = value.strip().removesuffix("%")
    return float(value) if value else 0.0


def sample_cpu(
    containers: dict[str, str],
    stop: threading.Event,
    samples: list[CpuSample],
    interval: float,
) -> None:
    id_to_service = {cid[:12]: service for service, cid in containers.items()}
    ids = list(containers.values())
    while not stop.wait(interval):
        try:
            output = run(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids]
            )
        except Exception:
            continue
        for line in output.splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            cid = str(row.get("Container", ""))
            service = id_to_service.get(cid[:12])
            if service is None:
                continue
            try:
                samples.append(
                    CpuSample(service, parse_cpu(str(row.get("CPUPerc", "0"))))
                )
            except ValueError:
                continue


def make_payload(batch_size: int) -> bytes:
    now = time.time_ns()
    records = []
    for idx in range(batch_size):
        records.append(
            {
                "timeUnixNano": str(now + idx),
                "severityNumber": 9,
                "severityText": "INFO",
                "body": {"stringValue": f"compose benchmark log {idx}"},
                "attributes": [
                    {"key": "bench.batch_index", "value": {"intValue": str(idx)}}
                ],
            }
        )
    payload = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "compose-bench"},
                        }
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": {"name": "bench-compose-logs"},
                        "logRecords": records,
                    }
                ],
            }
        ]
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def worker(
    url: str,
    token: str,
    payload: bytes,
    duration: float,
    counters: Counters,
    lock: threading.Lock,
    per_thread_rps: float | None,
) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    deadline = time.monotonic() + duration
    next_at = time.monotonic()
    while time.monotonic() < deadline:
        if per_thread_rps:
            now = time.monotonic()
            if now < next_at:
                time.sleep(next_at - now)
            next_at += 1.0 / per_thread_rps
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
                resp.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
        except Exception:
            with lock:
                counters.errors += 1
            continue
        with lock:
            if status == 202:
                counters.accepted_202 += 1
            else:
                counters.other_status += 1


def summarize_cpu(samples: Iterable[CpuSample]) -> tuple[float, dict[str, float]]:
    by_service: dict[str, list[float]] = {}
    for sample in samples:
        by_service.setdefault(sample.service, []).append(sample.cpu_percent)
    service_avg = {
        service: sum(values) / len(values)
        for service, values in sorted(by_service.items())
        if values
    }
    total_avg = sum(service_avg.values())
    return total_avg, service_avg


def run_case(
    args: argparse.Namespace,
    target_dps: int | None,
    containers: dict[str, str],
) -> dict[str, object]:
    payload = make_payload(args.batch_size)
    counters = Counters()
    lock = threading.Lock()
    stop_cpu = threading.Event()
    cpu_samples: list[CpuSample] = []
    req_rate = (target_dps / args.batch_size) if target_dps else None
    per_thread_rps = (req_rate / args.concurrency) if req_rate else None

    sampler = threading.Thread(
        target=sample_cpu,
        args=(containers, stop_cpu, cpu_samples, args.cpu_interval),
        daemon=True,
    )
    sampler.start()

    threads = [
        threading.Thread(
            target=worker,
            args=(
                args.url,
                args.token,
                payload,
                args.duration,
                counters,
                lock,
                per_thread_rps,
            ),
            daemon=True,
        )
        for _ in range(args.concurrency)
    ]
    started = time.monotonic()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.monotonic() - started
    stop_cpu.set()
    sampler.join(timeout=args.cpu_interval + 1)

    total_cpu, service_cpu = summarize_cpu(cpu_samples)
    accepted_logs = counters.accepted_202 * args.batch_size
    total_responses = counters.accepted_202 + counters.other_status + counters.errors
    success_ratio = counters.accepted_202 / total_responses if total_responses else 0.0
    return {
        "target_dps": target_dps,
        "duration_s": elapsed,
        "batch_size": args.batch_size,
        "concurrency": args.concurrency,
        "accepted_202": counters.accepted_202,
        "other_status": counters.other_status,
        "errors": counters.errors,
        "accepted_202_per_s": counters.accepted_202 / elapsed,
        "accepted_log_dps": accepted_logs / elapsed,
        "success_ratio": success_ratio,
        "cpu_percentage_avg": total_cpu,
        "service_cpu_percentage_avg": service_cpu,
    }


def flush_ingest(args: argparse.Namespace) -> None:
    sql = "SELECT * FROM otlp_flush('otlp:0.0.0.0:4319');"
    run(
        [
            *compose_base(args),
            "exec",
            "-T",
            "ingest",
            "sh",
            "-c",
            f"printf '%s\\n' \"{sql}\" > /tmp/duckdb-otlp-ingest.sql",
        ],
        capture=True,
    )


def print_table(results: list[dict[str, object]]) -> None:
    print(
        "| target_dps | accepted_log_dps | accepted_202/s | 202s | success | cpu_percentage_avg | service_cpu_percentage_avg |"
    )
    print("| ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    for row in results:
        service_cpu = ", ".join(
            f"{name}={value:.1f}%"
            for name, value in row["service_cpu_percentage_avg"].items()  # type: ignore[union-attr]
        )
        target = row["target_dps"] if row["target_dps"] is not None else "max"
        print(
            f"| {target} | {row['accepted_log_dps']:.0f} | "
            f"{row['accepted_202_per_s']:.1f} | {row['accepted_202']} | "
            f"{row['success_ratio']:.3f} | {row['cpu_percentage_avg']:.1f}% | "
            f"{service_cpu} |"
        )


def parse_targets(value: str | None) -> list[int | None]:
    if not value:
        return [10_000]
    targets: list[int | None] = []
    for part in value.split(","):
        part = part.strip().lower()
        if part == "max":
            targets.append(None)
        elif part:
            targets.append(int(part))
    return targets


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-f",
        "--compose-file",
        action="append",
        default=None,
        help="Compose file to pass to docker compose. Repeat to add files.",
    )
    parser.add_argument("-p", "--project", default="canardstack")
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:4319/v1/logs",
        help="OTLP/HTTP logs endpoint.",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN)
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument(
        "--targets",
        default="10000",
        help="Comma-separated target log datapoints/sec values, or 'max' for unpaced saturation.",
    )
    parser.add_argument(
        "--cpu-service",
        action="append",
        default=None,
        help="Compose service to include in cpu_percentage_avg. Repeat to add services.",
    )
    parser.add_argument("--cpu-interval", type=float, default=1.0)
    parser.add_argument(
        "--flush",
        action="store_true",
        help="Flush the duckdb-otlp writer after each case.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of a markdown table.",
    )
    args = parser.parse_args()
    if args.compose_file is None:
        args.compose_file = list(DEFAULT_COMPOSE_FILES)
    if args.cpu_service is None:
        args.cpu_service = list(DEFAULT_SERVICES)

    if args.duration <= 0:
        raise SystemExit("--duration must be positive")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency must be positive")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")

    containers = service_containers(args)
    results = []
    for target in parse_targets(args.targets):
        result = run_case(args, target, containers)
        results.append(result)
        if args.flush:
            flush_ingest(args)

    if args.json:
        print(json.dumps(results, indent=2, sort_keys=True))
    else:
        print_table(results)
        best = max(results, key=lambda row: float(row["accepted_log_dps"]))
        print(
            "\nmax accepted log DPS: "
            f"{best['accepted_log_dps']:.0f} at "
            f"{best['cpu_percentage_avg']:.1f}% average CPU"
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
