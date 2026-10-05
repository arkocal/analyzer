#!/usr/bin/env python3
"""Run goblint once (one bench config x one solver), streaming its log to disk.

Writes into --outdir:
  goblint.log   combined stdout/stderr
  result.json   when finished
and appends the result as one row to --results-csv, so results show up as each run terminates.

Always exits 0: timeouts, OOMs and crashes are recorded in the result, not reported to snakemake.
"""
import argparse
import csv
import fcntl
import json
import os
import re
import resource
import signal
import subprocess
import time
from datetime import datetime
from pathlib import Path

FIELDS = ["config", "solver", "status", "exit_code", "wall_s", "solver_s", "max_rss_mb",
          "td3_vars", "td3_evals", "wbu_rhs", "started", "finished", "goblint_version", "input"]


def tail(path, nbytes=200_000):
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        f.seek(max(0, f.tell() - nbytes))
        return f.read().decode("utf-8", errors="replace")


def parse_log(text):
    def last(pattern):
        m = re.findall(pattern, text)
        return m[-1] if m else None
    r = {}
    vars_evals = re.findall(r"vars: (\d+), evals: (\d+)", text) or re.findall(r"vars = (\d+)\s+evals = (\d+)", text)
    if vars_evals:
        r["td3_vars"], r["td3_evals"] = vars_evals[-1]
    r["wbu_rhs"] = last(r"\[Info\] RHS: (\d+)")
    start, end = last(r"Solver start: (\d+)"), last(r"Solver end: (\d+)")
    if start and end:
        r["solver_s"] = f"{(int(end) - int(start)) / 1000:.3f}"
    r["oom"] = "Out of memory" in text or "Out_of_memory" in text
    return r


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--goblint", required=True)
    p.add_argument("--conf", action="append", required=True, help="goblint --conf, in order")
    p.add_argument("--set", nargs=2, action="append", default=[], metavar=("OPT", "VAL"))
    p.add_argument("--input", required=True)
    p.add_argument("--config-name", required=True)
    p.add_argument("--solver", required=True)
    p.add_argument("--timeout", type=int, default=0, help="seconds, 0 = no limit")
    p.add_argument("--memory-limit-mb", type=int, default=0)
    p.add_argument("--outdir", required=True)
    p.add_argument("--results-csv", required=True)
    a = p.parse_args()

    out = Path(a.outdir)
    out.mkdir(parents=True, exist_ok=True)
    log_path = out / "goblint.log"
    version = subprocess.run([a.goblint, "--version"], capture_output=True, text=True).stdout.splitlines()
    version = version[0].split(":", 1)[-1].strip() if version else "?"

    cmd = [a.goblint]
    for c in a.conf:
        cmd += ["--conf", c]
    for k, v in a.set:
        cmd += ["--set", k, v]
    cmd += ["--set", "goblint-dir", str(out / ".goblint"), "-v", a.input]

    def limit():
        os.setsid()  # own process group, so a timeout kill takes everything with it
        if a.memory_limit_mb > 0:
            b = a.memory_limit_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (b, b))

    started = datetime.now().isoformat(timespec="seconds")
    t0 = time.monotonic()
    with open(log_path, "w") as log:
        log.write("$ " + " ".join(cmd) + "\n")
        log.flush()
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, preexec_fn=limit)
        timed_out = False
        while True:
            pid, wstatus, ru = os.wait4(proc.pid, os.WNOHANG)
            if pid:
                break
            if a.timeout > 0 and time.monotonic() - t0 > a.timeout:
                timed_out = True
                os.killpg(proc.pid, signal.SIGKILL)
                pid, wstatus, ru = os.wait4(proc.pid, 0)
                break
            time.sleep(0.2)
    wall = time.monotonic() - t0
    code = os.waitstatus_to_exitcode(wstatus)

    parsed = parse_log(tail(log_path))
    if a.solver != "td3":
        parsed.pop("td3_vars", None); parsed.pop("td3_evals", None)
    if timed_out:
        status = "timeout"
    elif code == 0:
        status = "done"
    elif parsed["oom"]:
        status = "oom"
    elif code < 0:
        status = f"signal {-code}"
    else:
        status = "error"

    row = {k: None for k in FIELDS}
    row.update({k: v for k, v in parsed.items() if k in FIELDS})
    row.update(config=a.config_name, solver=a.solver, status=status, exit_code=code, wall_s=f"{wall:.3f}",
               max_rss_mb=f"{ru.ru_maxrss / 1024:.0f}", started=started,
               finished=datetime.now().isoformat(timespec="seconds"), goblint_version=version, input=a.input)

    (out / "result.json").write_text(json.dumps(row, indent=2))

    results = Path(a.results_csv)
    results.parent.mkdir(parents=True, exist_ok=True)
    with open(results, "a", newline="") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        new = f.tell() == 0
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)
    print(f"{a.config_name} / {a.solver}: {status} in {wall:.1f}s")


if __name__ == "__main__":
    main()
