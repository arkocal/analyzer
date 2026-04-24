#!/usr/bin/env python3
"""Run goblint on an SV-COMP task description for all properties with an expected verdict."""

import argparse
import csv
import re
import subprocess
import sys
import time
from pathlib import Path

import yaml

# Prevent yaml from coercing bare true/false to Python bools
class _Loader(yaml.SafeLoader):
    pass
_Loader.add_constructor(
    "tag:yaml.org,2002:bool",
    lambda loader, node: loader.construct_scalar(node),
)

GOBLINT = Path(__file__).parent / "goblint"
SVCOMP_RESULT_RE = re.compile(r"SV-COMP result: (.+)")


def run_task(task_file: Path, config: Path, timeout: int, writer: csv.DictWriter) -> None:
    task_dir = task_file.parent
    with task_file.open() as f:
        task = yaml.load(f, Loader=_Loader)

    input_file = task_dir / task["input_files"]
    properties = task.get("properties", [])

    verdicted = [(p["property_file"], p["expected_verdict"])
                 for p in properties if "expected_verdict" in p]

    if not verdicted:
        return

    for prop_rel, expected in verdicted:
        prop_file = (task_dir / prop_rel).resolve()
        cmd = [
            str(GOBLINT),
            "--conf", str(config),
            "--set", "ana.specification", str(prop_file),
            str(input_file),
        ]
        timed_out = False
        returned = "unknown"
        t0 = time.monotonic()
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            output = result.stdout + result.stderr
            m = SVCOMP_RESULT_RE.search(output)
            if m:
                returned = m.group(1).strip()
        except subprocess.TimeoutExpired:
            timed_out = True
            returned = "unknown"
        runtime = round(time.monotonic() - t0, 2)

        parts = task_file.parts
        try:
            idx = next(i for i, p in enumerate(parts) if p == "c" and parts[i-1] == "sv-benchmarks")
            task_key = str(Path(*parts[idx + 1:]))
        except StopIteration:
            task_key = str(task_file)

        writer.writerow({
            "config": config.name,
            "task": task_key,
            "property": Path(prop_rel).stem,
            "expected": expected,
            "returned": returned,
            "timeout": timed_out,
            "runtime": runtime,
        })


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="Path to goblint config JSON")
    parser.add_argument("tasks", type=Path, nargs="+", help="SV-COMP task YAML file(s)")
    parser.add_argument("--timeout", type=int, default=60,
                        help="Timeout in seconds per property (default: 60)")
    args = parser.parse_args()

    if not args.config.exists():
        sys.exit(f"Config not found: {args.config}")

    fieldnames = ["config", "task", "property", "expected", "returned", "timeout", "runtime"]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()

    for task_file in args.tasks:
        if not task_file.exists():
            print(f"# skipping missing task: {task_file}", file=sys.stderr)
            continue
        run_task(task_file, args.config.resolve(), args.timeout, writer)


if __name__ == "__main__":
    main()
