#!/usr/bin/env python3
"""Interleave two binaries on the same input; measure process start through exit."""
import argparse
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--cpus", type=int, default=8)
    args = parser.parse_args()
    if min(args.runs, args.cpus) < 1:
        parser.error("--runs and --cpus must be positive")

    binaries = [p.resolve(strict=True) for p in (args.baseline, args.candidate)]
    results = [[], []]
    for round_index in range(args.runs + 1):
        for index in ((0, 1) if round_index % 2 == 0 else (1, 0)):
            with tempfile.TemporaryDirectory() as output:
                env = dict(os.environ, NCPU=str(args.cpus), DATA_DIR=str(args.input.resolve()),
                           OUTPUT_DIR=output, SS_TIMING="0")
                start = time.perf_counter_ns()
                subprocess.run([binaries[index]], env=env, check=True)
                seconds = (time.perf_counter_ns() - start) / 1e9
            if round_index:
                results[index].append(seconds)
    for label, samples in zip(("baseline", "candidate"), results):
        print(label, "seconds:", " ".join(f"{s:.4f}" for s in samples),
              f"(median {statistics.median(samples):.4f})")
    print(f"Median speedup: {statistics.median(results[0]) / statistics.median(results[1]):.2f}x")


if __name__ == "__main__":
    main()
