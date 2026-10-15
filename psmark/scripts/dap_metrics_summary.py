#!/usr/bin/env python3
"""Mean and standard deviation of each cell's MQTT-DAP metrics over the matrix passes.

Reads <results>/pass*/<cell>/dap_metrics.json, as dap_metrics.py writes them, and
prints one CSV row per cell. A metric a cell does not have is left empty.

Usage: dap_metrics_summary.py <results dir>
"""
import argparse
import csv
import glob
import json
import os
import statistics
import sys
from collections import defaultdict

METRICS = (
    ("residual_error", "EAR_pct"),
    ("residual_error", "ERR_pct"),
    ("delivery_protection", "DPR_pct"),
    ("operations", "coverage_pct"),
    ("operations", "leakage_pct"),
    ("operations", "completion_pct"),
)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("results")
    args = ap.parse_args()

    runs = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(args.results, "pass*", "*", "dap_metrics.json"))):
        with open(path) as fh:
            runs[os.path.basename(os.path.dirname(path))].append(json.load(fh))

    out = csv.writer(sys.stdout)
    out.writerow(["cell", "passes"] + [f"{key}_{stat}" for _, key in METRICS for stat in ("mean", "sd")])
    for cell in sorted(runs):
        row = [cell, len(runs[cell])]
        for group, key in METRICS:
            values = [r[group][key] for r in runs[cell] if r.get(group) and r[group].get(key) is not None]
            row.append(f"{statistics.mean(values):.4f}" if values else "")
            row.append(f"{statistics.stdev(values):.4f}" if len(values) > 1 else "")
        out.writerow(row)
    return 0


if __name__ == "__main__":
    sys.exit(main())
