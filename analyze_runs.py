#!/usr/bin/env python3
"""Cross-run comparison over a sweep's output directories.

Takes one config (stem or .yaml path), plus ';'-separated mode, sMax, and
seed lists - the same axes run_all_cloudlab_sweep.sh sweeps - and reads
runs/<config>_<mode>_<sMax>_<seed>/ for every combination. For each
(mode, sMax) it prints one overall-run-summary row per seed plus the average
across the seeds that exist, so seed-to-seed spread and the seed-averaged
comparison across modes and sMax values are visible in one report.

Latency percentiles are pooled over the whole run (exact from the raw
histogram buckets when the ledger has them, a merged-sketch estimate marked
with "~" otherwise); the avg row averages the per-seed percentiles. Missing
run directories are reported as MISSING and excluded from the average.

Usage:
  python3 analyze_runs.py config_local \\
      --modes "chameleon;pileus" --smax "2250;4500" --seeds "7;42;100"
"""

import argparse
import os
import sys

import warnings

import pandas as pd

# The Run loader's column additions trip pandas' fragmentation performance
# hint once per loaded run; it is advisory, and this report loads many runs.
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

from analyze_run import Run, pooled_quantiles

# (ledger column, report label); columns absent from older ledgers are skipped.
COUNTER_COLS = [
    ("Rejected", "Rejected"),
    ("ShedAtClient", "Shed"),
    ("DeadlineExceeded", "Deadline"),
    ("Lost", "Lost"),
    ("Fallbacks", "Fallbacks"),
    ("Redirects", "Redirects"),
    ("Violations", "Violations"),
    ("PredictedProfitSum", "PredProfit"),
    ("RealizedProfitSum", "Profit"),
]
PCT_LABELS = ["p50ms", "p95ms", "p99ms"]


def split_list(raw, what):
    values = [v.strip() for v in raw.split(";") if v.strip()]
    if not values:
        sys.exit(f"ERROR: --{what} contained no values: '{raw}'")
    return values


def run_metrics(run_dir):
    """One run's overall summary as {label: number}, plus percentile exactness."""
    run = Run(run_dir)
    led = run.ledger
    duration_s = max((led["Timestamp"].max() - run.t0) / 1000.0, 1.0)
    m = {"Served": led["Count"].sum()}
    m["Served/s"] = m["Served"] / duration_s
    for col, label in COUNTER_COLS:
        if col in led.columns:
            m[label] = led[col].sum()
    (p50, p95, p99), exact = pooled_quantiles(led, (0.50, 0.95, 0.99))
    m.update(dict(zip(PCT_LABELS, (p50, p95, p99))))
    return m, exact


def fmt(label, value, exact=True):
    tag = "" if exact else "~"
    if label in PCT_LABELS:
        return f"{tag}{value:.1f}"
    if label == "Served/s":
        return f"{value:,.0f}"
    return f"{value:,.0f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config", help="config stem or path (config_local, config_local.yaml, ...)")
    parser.add_argument("--modes", required=True, help="';'-separated mode list")
    parser.add_argument("--smax", required=True, help="';'-separated sMax list")
    parser.add_argument("--seeds", required=True, help="';'-separated seed list")
    parser.add_argument("--runs-dir", default="runs", help="directory holding the run folders (default: runs)")
    args = parser.parse_args()

    stem = os.path.splitext(os.path.basename(args.config))[0]
    modes = split_list(args.modes, "modes")
    smaxes = split_list(args.smax, "smax")
    seeds = split_list(args.seeds, "seeds")

    missing_dirs = []
    for mode in modes:
        for smax in smaxes:
            rows = []
            per_seed = []  # (metrics, exact) for the avg row
            labels_seen = []
            for seed in seeds:
                name = f"{stem}_{mode}_{smax}_{seed}"
                run_dir = os.path.join(args.runs_dir, name)
                if not os.path.isdir(run_dir):
                    rows.append({"seed": seed, "Served": "MISSING"})
                    missing_dirs.append(run_dir)
                    continue
                metrics, exact = run_metrics(run_dir)
                per_seed.append((metrics, exact))
                for label in metrics:
                    if label not in labels_seen:
                        labels_seen.append(label)
                rows.append({"seed": seed,
                             **{k: fmt(k, v, exact if k in PCT_LABELS else True)
                                for k, v in metrics.items()}})
            print("\n" + "=" * 72)
            print(f"{stem}  mode={mode}  sMax={smax}  ({len(per_seed)}/{len(seeds)} seeds found)")
            print("=" * 72)
            if not per_seed:
                print("no runs found for this combination")
                continue
            if len(per_seed) > 1:
                avg = {label: sum(m[label] for m, _ in per_seed) / len(per_seed)
                       for label in labels_seen}
                all_exact = all(exact for _, exact in per_seed)
                rows.append({"seed": "avg",
                             **{k: fmt(k, v, all_exact if k in PCT_LABELS else True)
                                for k, v in avg.items()}})
            table = pd.DataFrame(rows).fillna("")
            print(table.to_string(index=False))
    if missing_dirs:
        print("\nnote: MISSING rows are excluded from the averages; not found:", file=sys.stderr)
        for d in missing_dirs:
            print(f"  {d}", file=sys.stderr)


if __name__ == "__main__":
    main()
