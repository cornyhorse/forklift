"""A stand-in for ``forklift run-job`` that the worker's tests drive through the spec (tests only).

    python fake_engine.py run-job SPEC --base-dir DIR --result RESULT [--allow-url-host HOST]...
                          [--progress-jsonl]

It follows the run-job contract (progress as JSON lines on stdout, logs on stderr, the result in
--result, exit 0 / 1 / 2, SIGTERM cancels) and does what ``spec["options"]["fake_engine"]``
says:

- ``{"mode": "succeed"}`` (default): reads the input (a staged file, or a presigned URL whose host
  was allowed), writes ``out/data.parquet`` (the input upper-cased), ``out/bad_rows.parquet`` and
  ``out/manifest.json``, reports progress and succeeds.
- ``{"mode": "fail"}``: a threshold failure that keeps ``out/bad_rows.parquet``.
- ``{"mode": "exit", "code": N, "stderr": [...]}``: writes the lines to stderr, exits N, no result.
- ``{"mode": "signal", "signal": N}``: kills itself with signal N.
- ``{"mode": "hang"}``: reports progress, then waits; SIGTERM writes a cancelled result.
- ``{"mode": "stubborn"}``: reports progress and ignores SIGTERM.
- ``{"mode": "result", "result": {...} | "text": "...", "files": {path: text},
  "symlinks": {path: target}, "exit": N}``: writes exactly that.
- ``{"mode": "probe", "read": [paths], "write": [paths], "connect": [[host, port]],
  "signal_parent": bool}``: reports its environment, limits and what it could reach as the
  ``out/report.json`` artifact.
- ``{"mode": "flood"}``: noisy stdout (non-JSON, over-long and odd lines), then succeeds.
- ``{"mode": "escape"}``: leaves a process running in a session of its own (its pid is in
  ``out/report.json``), then succeeds.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import signal
import socket
import sys
import time
import urllib.request
from urllib.parse import urlsplit


def write_result(path, spec, status, artifacts=(), error=None):
    result = {
        "spec_version": 1,
        "job_id": spec["job_id"],
        "status": status,
        "counts": {"total_rows": 3, "valid_rows": 2, "invalid_rows": 1, "truncated_rows": 0},
        "schema_extensions": [],
        "validation_summary": {"NOT_NULL:id": 1} if status != "cancelled" else {},
        "warnings": ["fake engine"],
        "artifacts": list(artifacts),
        "error": error,
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(result, handle)


def progress(enabled, **counts):
    if enabled:
        print(json.dumps(counts), flush=True)


def read_input(spec, base_dir, allowed_hosts):
    location = spec["input"]["location"]
    if location["type"] == "file":
        with open(os.path.join(base_dir, location["path"]), "rb") as handle:
            return handle.read()
    if location["type"] == "presigned_url":
        host = urlsplit(location["url"]).netloc
        if host not in allowed_hosts:
            raise PermissionError(f"host {host} is not allowed")
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(location["url"], timeout=30) as response:
            return response.read()
    return b""


def write(base_dir, path, data):
    full = os.path.join(base_dir, path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "wb") as handle:
        handle.write(data if isinstance(data, bytes) else data.encode("utf-8"))


def succeed(args, spec, extra_artifacts=()):
    try:
        data = read_input(spec, args.base_dir, args.allow_url_host)
    except (OSError, PermissionError) as error:
        print(f"cannot read the input: {error}", file=sys.stderr)
        write_result(
            args.result,
            spec,
            "failed",
            error={"code": "INPUT_UNREADABLE", "message": str(error), "retryable": False},
        )
        return 1
    progress(args.progress_jsonl, rows_read=1, rows_rejected=0, bytes_read=len(data) // 2)
    progress(args.progress_jsonl, rows_read=3, rows_rejected=1, bytes_read=len(data))
    write(args.base_dir, "out/data.parquet", data.upper())
    write(args.base_dir, "out/bad_rows.parquet", b"bad rows")
    write(args.base_dir, "out/manifest.json", json.dumps({"files": ["data.parquet"]}))
    artifacts = [
        {"kind": "data", "path": "out/data.parquet", "rows": 2, "bytes": 1, "sha256": "x"},
        {"kind": "manifest", "path": "out/manifest.json"},
        {"kind": "bad_rows", "path": "out/bad_rows.parquet", "rows": 1},
        *extra_artifacts,
    ]
    write_result(args.result, spec, "succeeded", artifacts)
    print("fake engine: done", file=sys.stderr)
    return 0


def wait_for_sigterm(args, spec, stubborn):
    def on_term(number, frame):
        write_result(
            args.result,
            spec,
            "cancelled",
            error={"code": "CANCELLED", "message": "cancelled by SIGTERM", "retryable": False},
        )
        sys.exit(1)

    signal.signal(signal.SIGTERM, signal.SIG_IGN if stubborn else on_term)
    progress(args.progress_jsonl, rows_read=5, rows_rejected=0, bytes_read=50)
    while True:
        time.sleep(0.05)


def probe(args, spec, behaviour):
    report = {
        "env": dict(os.environ),
        "cwd": os.getcwd(),
        "argv": sys.argv,
        "rlimits": {
            name: list(resource.getrlimit(getattr(resource, f"RLIMIT_{name.upper()}")))
            for name in ("as", "cpu", "fsize", "nofile", "core")
        },
        "interfaces": sorted(name for _, name in socket.if_nameindex()),
        "read": {},
        "write": {},
        "connect": {},
    }
    for path in behaviour.get("read", []):
        try:
            with open(path, "rb") as handle:
                handle.read(1)
            report["read"][path] = "ok"
        except OSError as error:
            report["read"][path] = type(error).__name__
    for path in behaviour.get("write", []):
        try:
            with open(path, "wb") as handle:
                handle.write(b"x")
            report["write"][path] = "ok"
        except OSError as error:
            report["write"][path] = type(error).__name__
    for host, port in behaviour.get("connect", []):
        try:
            socket.create_connection((host, port), timeout=5).close()
            report["connect"][f"{host}:{port}"] = "ok"
        except OSError as error:
            report["connect"][f"{host}:{port}"] = type(error).__name__
    if behaviour.get("signal_parent"):
        try:
            os.kill(os.getppid(), 0)
            report["signal_parent"] = "ok"
        except OSError as error:
            report["signal_parent"] = type(error).__name__
    write(args.base_dir, "out/report.json", json.dumps(report))
    return succeed(args, spec, [{"kind": "report", "path": "out/report.json"}])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run-job"])
    parser.add_argument("spec")
    parser.add_argument("--base-dir", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--allow-url-host", action="append", default=[])
    parser.add_argument("--progress-jsonl", action="store_true")
    args = parser.parse_args()
    with open(args.spec, encoding="utf-8") as handle:
        spec = json.load(handle)
    behaviour = (spec.get("options") or {}).get("fake_engine") or {}
    mode = behaviour.get("mode", "succeed")
    if mode == "succeed":
        return succeed(args, spec)
    if mode == "fail":
        write(args.base_dir, "out/bad_rows.parquet", b"bad rows")
        write_result(
            args.result,
            spec,
            "failed",
            [{"kind": "bad_rows", "path": "out/bad_rows.parquet", "rows": 1}],
            {"code": "BAD_ROWS_THRESHOLD_EXCEEDED", "message": "1 of 3 rows", "retryable": False},
        )
        return 1
    if mode == "exit":
        for line in behaviour.get("stderr", []):
            print(line, file=sys.stderr)
        return behaviour["code"]
    if mode == "signal":
        sys.stderr.flush()
        os.kill(os.getpid(), behaviour["signal"])
        time.sleep(5)
        return 99
    if mode in ("hang", "stubborn"):
        wait_for_sigterm(args, spec, stubborn=mode == "stubborn")
    if mode == "result":
        for path, text in (behaviour.get("files") or {}).items():
            write(args.base_dir, path, text)
        for path, target in (behaviour.get("symlinks") or {}).items():
            os.makedirs(os.path.dirname(os.path.join(args.base_dir, path)), exist_ok=True)
            os.symlink(target, os.path.join(args.base_dir, path))
        with open(args.result, "w", encoding="utf-8") as handle:
            if "text" in behaviour:
                handle.write(behaviour["text"])
            else:
                json.dump(behaviour["result"], handle)
        return behaviour.get("exit", 0)
    if mode == "probe":
        return probe(args, spec, behaviour)
    if mode == "escape":
        child = os.fork()
        if child == 0:
            os.setsid()
            null = os.open(os.devnull, os.O_RDWR)
            for descriptor in (0, 1, 2):
                os.dup2(null, descriptor)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(600)
            os._exit(0)
        write(args.base_dir, "out/report.json", json.dumps({"escaped": child}))
        return succeed(args, spec, [{"kind": "report", "path": "out/report.json"}])
    if mode == "flood":
        print("not json", flush=True)
        print("[1, 2, 3]", flush=True)
        print("x" * (200 * 1024), flush=True)
        print(json.dumps({"rows_read": True, "Bad Key": 1, "rows_rejected": -1}), flush=True)
        print(json.dumps({"rows_written": 7}), flush=True)
        return succeed(args, spec)
    raise SystemExit(f"unknown fake engine mode {mode!r}")


if __name__ == "__main__":
    sys.exit(main())
