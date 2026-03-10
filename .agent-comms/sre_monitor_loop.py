#!/usr/bin/env python3
"""Persistent SRE/frontend monitor — runs indefinitely, cycles every 60s."""

import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

LOG_FILE = Path("/tmp/sre_monitor_loop.log")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIR = PROJECT_ROOT / "frontend"
CYCLE_SLEEP = 60


def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def check_port(port: int) -> bool:
    """Return True if something is listening on the port."""
    result = subprocess.run(
        ["lsof", "-i", f":{port}", "-t"],
        capture_output=True, text=True
    )
    return bool(result.stdout.strip())


def check_http(url: str, timeout: int = 5) -> tuple[bool, str]:
    """Return (ok, first_bytes) for a URL."""
    try:
        result = subprocess.run(
            ["curl", "-s", "--max-time", str(timeout), url],
            capture_output=True, text=True, timeout=timeout + 2
        )
        text = result.stdout[:100]
        ok = len(result.stdout) > 0
        return ok, text
    except Exception as e:
        return False, str(e)


def check_sse(url: str, timeout: int = 3) -> bool:
    """Return True if SSE endpoint returns event data."""
    try:
        result = subprocess.run(
            ["curl", "-sN", "--max-time", str(timeout), url],
            capture_output=True, text=True, timeout=timeout + 2
        )
        return "event:" in result.stdout or "data:" in result.stdout
    except Exception:
        return False


def restart_backend():
    log("RESTARTING BACKEND on port 8000")
    subprocess.run(["bash", "-c", "lsof -i :8000 -t 2>/dev/null | xargs kill 2>/dev/null"], capture_output=True)
    time.sleep(1)
    subprocess.Popen(
        ["/opt/homebrew/bin/python3.11", "-m", "litestar", "--app", "web.app:app",
         "run", "--host", "0.0.0.0", "--port", "8000"],
        cwd=str(PROJECT_ROOT),
        stdout=open("/tmp/backend.log", "a"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    time.sleep(3)


def restart_frontend():
    log("RESTARTING FRONTEND on port 3000")
    subprocess.run(["bash", "-c", "lsof -i :3000 -t 2>/dev/null | xargs kill 2>/dev/null"], capture_output=True)
    time.sleep(1)
    subprocess.Popen(
        ["npx", "next", "dev", "-p", "3000"],
        cwd=str(FRONTEND_DIR),
        stdout=open("/tmp/frontend.log", "a"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    time.sleep(4)


def run_cycle(cycle_num: int):
    issues = []

    # Check backend
    backend_port = check_port(8000)
    backend_http, backend_sample = check_http("http://localhost:8000/api/agents")
    if not backend_port or not backend_http:
        issues.append("Backend DOWN")
        restart_backend()
        # Re-check
        backend_http, backend_sample = check_http("http://localhost:8000/api/agents")
        if backend_http:
            issues.append("Backend RECOVERED after restart")
        else:
            issues.append("Backend STILL DOWN after restart attempt")

    # Check frontend
    frontend_port = check_port(3000)
    frontend_http, frontend_sample = check_http("http://localhost:3000/")
    if not frontend_port or not frontend_http:
        issues.append("Frontend DOWN")
        restart_frontend()
        frontend_http, frontend_sample = check_http("http://localhost:3000/")
        if frontend_http:
            issues.append("Frontend RECOVERED after restart")
        else:
            issues.append("Frontend STILL DOWN after restart attempt")

    # Check SSE
    sse_ok = check_sse("http://localhost:8000/api/sse/agents")
    if not sse_ok:
        issues.append("SSE agents stream not responding")

    status = "ALL GREEN" if not issues else " | ".join(issues)
    log(f"Cycle {cycle_num}: {status}")

    if issues:
        log(f"  Issues: {issues}")


def main():
    log("SRE monitor starting (persistent loop)")
    cycle = 0
    while True:
        cycle += 1
        try:
            run_cycle(cycle)
        except Exception as e:
            log(f"Cycle {cycle} error: {e}")
        time.sleep(CYCLE_SLEEP)


if __name__ == "__main__":
    main()
