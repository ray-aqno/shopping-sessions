#!/usr/bin/env python3
"""Measure fresh containers using Docker process timestamps, excluding CLI setup."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import statistics
import subprocess
import time


def capture(args):
    return subprocess.check_output(args, text=True).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--image", default="shopping-sessions:latest")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--cpuset", default="0-7")
    args = parser.parse_args()
    image = json.loads(capture(["docker", "image", "inspect", args.image]))[0]
    assert image["Architecture"] == "amd64" and image["Os"] == "linux"
    assert image["Size"] < 8 * 1024**3
    report = dict(image=args.image, image_id=image["Id"], data=args.data.name,
                  cpuset=args.cpuset, ncpu=8, memory_gb=24, runs=[],
                  note="Local Docker State.StartedAt-to-FinishedAt; not an official scorer measurement")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    for number in range(args.rounds):
        out = args.output.resolve() / f"run-{number+1}"
        scratch = args.output.resolve() / f"scratch-{number+1}"
        out.mkdir(parents=True, exist_ok=True)
        scratch.mkdir(parents=True, exist_ok=True)
        command = ["docker", "create", "--platform", "linux/amd64", "--network", "none",
                   "--read-only", "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
                   "--security-opt", "no-new-privileges", "--pids-limit", "4096", "--cpus", "8",
                   "--cpuset-cpus", args.cpuset, "--memory", "24g", "--memory-swap", "24g",
                   "--user", "1000:1000", "-v", f"{args.data.resolve()}:/data:ro",
                   "-v", f"{out}:/output", "-v", f"{scratch}:/scratch",
                   "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output", "-e", "SCRATCH_DIR=/scratch",
                   "-e", "HOME=/scratch", "-e", "NCPU=8", "-e", "MEM_GB=24", args.image]
        container = capture(command)
        try:
            start = time.monotonic()
            result = subprocess.run(["docker", "start", "--attach", container], capture_output=True, text=True, timeout=190)
            cli_seconds = time.monotonic() - start
            state = json.loads(capture(["docker", "inspect", "--format", "{{json .State}}", container]))
            parse = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00"))
            seconds = (parse(state["FinishedAt"]) - parse(state["StartedAt"])).total_seconds()
            run = dict(round=number+1, container_seconds=seconds, cli_seconds=cli_seconds,
                       exit_code=state["ExitCode"], oom_killed=state["OOMKilled"],
                       phases=result.stderr.splitlines())
            report["runs"].append(run)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(run), flush=True)
            if result.returncode or state["ExitCode"] or state["OOMKilled"]:
                raise RuntimeError("container failed; see report")
        finally:
            subprocess.run(["docker", "rm", "--force", container], capture_output=True, check=True)
    report["median_seconds"] = statistics.median(r["container_seconds"] for r in report["runs"])
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Median container runtime: {report['median_seconds']:.6f}s")


if __name__ == "__main__":
    main()
