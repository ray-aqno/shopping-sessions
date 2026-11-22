#!/usr/bin/env python3
"""Alternate pinned native binaries; retain process wall time and phase logs."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--cpus", default="0-7")
    parser.add_argument("binaries", nargs="+", type=Path)
    args = parser.parse_args()
    records = []
    args.output.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    for round_no in range(args.rounds):
        binaries = args.binaries if round_no % 2 == 0 else args.binaries[::-1]
        for binary in binaries:
            command = ["/usr/bin/time", "-f", "METRICS %e %M %U %S", "taskset", "-c", args.cpus, str(binary.resolve())]
            env = dict(os.environ, DATA_DIR=str(args.data.resolve()), OUTPUT_DIR=str(args.output.resolve()/binary.name), NCPU="8")
            result = subprocess.run(command, env=env, capture_output=True, text=True, check=True)
            lines = result.stderr.splitlines()
            metric = next(line for line in lines if line.startswith("METRICS ")).split()
            record = dict(binary=binary.name, round=round_no+1, wall_seconds=float(metric[1]), peak_rss_kib=int(metric[2]), user_seconds=float(metric[3]), system_seconds=float(metric[4]), phases=[line for line in lines if not line.startswith("METRICS ")])
            records.append(record)
            # Preserve completed measurements across a shell/VM interruption.
            args.report.write_text(json.dumps(dict(data=args.data.name, cpus=args.cpus, runs=records, complete=False), indent=2) + "\n")
            print(json.dumps(record), flush=True)
    summary = {}
    for binary in args.binaries:
        subset = [r for r in records if r["binary"] == binary.name]
        summary[binary.name] = dict(median_seconds=statistics.median(r["wall_seconds"] for r in subset), max_peak_rss_kib=max(r["peak_rss_kib"] for r in subset))
    report = dict(data=args.data.name, cpus=args.cpus, ncpu=8, rounds=args.rounds, runs=records, summary=summary)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
