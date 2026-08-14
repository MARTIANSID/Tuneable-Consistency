#!/usr/bin/env python3
"""Analyze one experiment run directory: the same analysis as graphs.ipynb.
Reads the CSVs a run writes into its own directory, renders the six figures,
and prints the text reports to stdout.

Data sources (per run directory):
  client_metrics_global.csv  client-side ledger, one row per (node, chosen,
                             executed) cell per ~1 s interval; counters reset
                             at every flush, so rows are per-interval activity
                             and the P50/P95/P99 columns are that interval's
                             percentiles - all cross-arm comparison numbers
                             come from here
  occupancy_<n>.csv          per node, per control interval: utilization U,
                             average in-flight, shadow price lambda
  config.yaml                the exact config of this run (mode, phases)

By default the figures open in interactive windows; pass --save (or --out) to
write them as PNGs into <run_dir>/graphs/ instead.

Usage:  python3 analyze_run.py runs/<label>_<stamp> [--save] [--out DIR]
Needs:  pandas, matplotlib
"""

import argparse
import glob
import json
import os
import sys

import matplotlib
import pandas as pd

plt = None  # bound in main() once the backend is chosen (Agg only when saving)

# Fixed color per entity (never cycled): the same level or node keeps the same
# hue across runs and modes, so cross-run figures stay comparable.
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7"]
LEVEL_COLORS = {
    "R:EVENTUAL_LOCAL": PALETTE[0],
    "R:EVENTUAL_MAJORITY": PALETTE[1],
    "R:CAUSAL_LOCAL": PALETTE[2],
    "R:CAUSAL_MAJORITY": PALETTE[3],
    "R:LINEARIZABLE": PALETTE[4],
    "W:1": PALETTE[5],
    "W:2": PALETTE[6],
    "W:3": "#7a6a52",
}
NODE_COLORS = {n: PALETTE[n % len(PALETTE)] for n in range(8)}

CELL_KEY = ["NodeId", "ChosenLevel", "ExecutedLevel"]
LINE = dict(linewidth=1.8)


class Run:
    def __init__(self, run_dir):
        self.dir = run_dir
        self.config = self._load_config(run_dir)
        self.mode = self.config.get("mode", "?")
        # phases moved under workload; fall back to top-level for old runs.
        self.phases = self.config.get("workload", {}).get("phases", self.config.get("phases", []))

        self.ledger = pd.read_csv(os.path.join(run_dir, "client_metrics_global.csv"))
        if "CountTotal" in self.ledger.columns:
            sys.exit("ERROR: this run's ledger uses the old cumulative schema "
                     "(CountTotal columns); analyze it with analyze_run.py from "
                     "an older git revision")
        self.t0 = self.ledger["Timestamp"].min()
        self.ledger["Time_s"] = (self.ledger["Timestamp"] - self.t0) / 1000.0

        self.occupancy = {}
        for path in sorted(glob.glob(os.path.join(run_dir, "occupancy_*.csv"))):
            node = int(path.split("_")[-1].split(".")[0])
            df = pd.read_csv(path)
            df["Time_s"] = (df["Timestamp"] - self.t0) / 1000.0
            self.occupancy[node] = df

    @staticmethod
    def _load_config(run_dir):
        yaml_path = os.path.join(run_dir, "config.yaml")
        json_path = os.path.join(run_dir, "config.json")
        if os.path.exists(yaml_path):
            try:
                import yaml
            except ImportError:
                sys.exit("ERROR: this run's config is YAML; install PyYAML (pip install pyyaml)")
            with open(yaml_path) as f:
                return yaml.safe_load(f)
        if os.path.exists(json_path):  # older runs archived JSON
            with open(json_path) as f:
                return json.load(f)
        sys.exit(f"ERROR: no config.yaml or config.json in {run_dir}")

    def interval_rates(self, columns):
        """Ledger rows are already per-interval: sum the cells per flush
        timestamp for system-wide interval activity."""
        out = self.ledger.groupby("Timestamp")[columns].sum().reset_index()
        out["Time_s"] = (out["Timestamp"] - self.t0) / 1000.0
        out["dt_s"] = out["Timestamp"].diff().fillna(1000) / 1000.0
        return out

    def whole_run(self):
        """Whole-run totals per cell: the sum of every interval row."""
        sum_cols = [c for c in self.ledger.columns
                    if c not in CELL_KEY + ["Timestamp", "Time_s", "AvgLatencyMs",
                                            "P50Ms", "P95Ms", "P99Ms"]]
        df = self.ledger.copy()
        df["LatencySum"] = df["AvgLatencyMs"] * df["Count"]
        agg = df.groupby(CELL_KEY)[sum_cols + ["LatencySum"]].sum().reset_index()
        agg["AvgLatencyMs"] = agg["LatencySum"] / agg["Count"].clip(lower=1)
        # Percentiles cannot be merged across intervals; report the worst
        # (count-weighted percentiles would need the raw buckets).
        worst = df.groupby(CELL_KEY)[["P95Ms", "P99Ms"]].max().reset_index()
        return agg.merge(worst, on=CELL_KEY)

    def phase_lines(self, ax):
        if not self.phases:
            return
        t = 0
        for p in self.phases[:-1]:
            t += p["durationSeconds"]
            ax.axvline(t, color="gray", linestyle=":", linewidth=0.8)


def style(ax, xlabel, ylabel, title):
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(alpha=0.25)


def plot_throughput(run, out):
    rates = run.interval_rates(["Count", "Rejected", "Lost", "Fallbacks"])
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(rates["Time_s"], rates["Count"] / rates["dt_s"], label="served/s",
            color=PALETTE[0], **LINE)
    ax.plot(rates["Time_s"], rates["Rejected"] / rates["dt_s"], label="rejected/s",
            color=PALETTE[1], **LINE)
    ax.plot(rates["Time_s"], rates["Fallbacks"] / rates["dt_s"], label="fallbacks/s",
            color=PALETTE[3], **LINE)
    run.phase_lines(ax)
    style(ax, "time [s]", "requests/s", f"Throughput and shedding ({run.mode})")
    ax.legend()
    fig.tight_layout()
    if out:
        fig.savefig(out("throughput.png"), dpi=120)
        plt.close(fig)


def plot_profit(run, out):
    profit = run.interval_rates(["PredictedProfitSum", "RealizedProfitSum"])
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(profit["Time_s"], profit["PredictedProfitSum"] / profit["dt_s"],
            label="predicted/s", color="#52514e", **LINE)
    ax.plot(profit["Time_s"], profit["RealizedProfitSum"] / profit["dt_s"],
            label="realized/s", color=PALETTE[2], **LINE)
    run.phase_lines(ax)
    style(ax, "time [s]", "profit/s",
          f"Predicted vs realized profit ({run.mode}) - the gap is the misprediction stream")
    ax.legend()
    fig.tight_layout()
    if out:
        fig.savefig(out("profit.png"), dpi=120)
        plt.close(fig)


def plot_occupancy(run, out):
    fig, (ax_u, ax_l) = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    for node, df in run.occupancy.items():
        color = NODE_COLORS[node % 8]
        ax_u.plot(df["Time_s"], df["U"].rolling(10, min_periods=1).mean(),
                  label=f"node {node}", color=color, linewidth=1.2)
        ax_l.plot(df["Time_s"], df["Lambda"], label=f"node {node}", color=color, linewidth=1.2)
    ax_u.axhline(run.config["chameleon"]["uTarget"], color="black", linestyle="--",
                 linewidth=0.8, label="u_target")
    ax_u.legend(ncol=3, fontsize=8)
    ax_l.set_yscale("log")
    run.phase_lines(ax_u)
    run.phase_lines(ax_l)
    style(ax_u, "", "utilization U",
          f"Occupancy utilization (1 s smoothed) and shadow price ({run.mode})")
    style(ax_l, "time [s]", "lambda (log)", "")
    fig.tight_layout()
    if out:
        fig.savefig(out("occupancy.png"), dpi=120)
        plt.close(fig)


def plot_level_mix(run, out):
    df = run.ledger
    mix = (df[df["ExecutedLevel"] != "-"]
           .groupby(["Timestamp", "ExecutedLevel"])["Count"].sum().unstack(fill_value=0))
    mix.index = (mix.index - run.t0) / 1000.0
    dt = pd.Series(mix.index).diff().fillna(1.0).values
    fig, ax = plt.subplots(figsize=(12, 4))
    for level in sorted(mix.columns):
        ax.plot(mix.index, mix[level] / dt, label=level,
                color=LEVEL_COLORS.get(level, "#888"), linewidth=1.2)
    run.phase_lines(ax)
    style(ax, "time [s]", "requests/s",
          f"Executed level mix ({run.mode}) - the mix is emergent, nothing configures it")
    ax.legend(ncol=3, fontsize=8)
    fig.tight_layout()
    if out:
        fig.savefig(out("level_mix.png"), dpi=120)
        plt.close(fig)


def plot_latency(run, out):
    lat = run.ledger.copy()
    lat["LatencySum"] = lat["AvgLatencyMs"] * lat["Count"]
    by_t = lat.groupby("Timestamp")[["LatencySum", "Count"]].sum()
    by_t["avg_ms"] = by_t["LatencySum"] / by_t["Count"].clip(lower=1)
    by_t.index = (by_t.index - run.t0) / 1000.0
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(by_t.index, by_t["avg_ms"], color=PALETTE[6], **LINE)
    run.phase_lines(ax)
    style(ax, "time [s]", "avg latency [ms]",
          f"Client-observed average latency per interval ({run.mode})")
    fig.tight_layout()
    if out:
        fig.savefig(out("latency.png"), dpi=120)
        plt.close(fig)


def plot_upgrades(run, out):
    up = run.interval_rates(["UpgradesFree", "UpgradesWaiting"])
    total = (up["UpgradesFree"] + up["UpgradesWaiting"]).clip(lower=1)
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(up["Time_s"], up["UpgradesFree"] / total, label="free fraction",
            color=PALETTE[2], **LINE)
    ax.plot(up["Time_s"], up["UpgradesWaiting"] / total, label="waiting fraction",
            color=PALETTE[3], **LINE)
    run.phase_lines(ax)
    ax.set_ylim(0, 1)
    style(ax, "time [s]", "fraction of upgrades", f"Free vs waiting upgrades ({run.mode})")
    ax.legend()
    fig.tight_layout()
    if out:
        fig.savefig(out("upgrades.png"), dpi=120)
        plt.close(fig)


def print_tables(run):
    totals = run.whole_run()

    print("\n--- Latency per busy cell (whole run; P95/P99 = worst 1 s interval) ---")
    busy = totals[totals["Count"] > 0.01 * totals["Count"].sum()]
    cols = ["NodeId", "ChosenLevel", "ExecutedLevel", "Count",
            "AvgLatencyMs", "P95Ms", "P99Ms"]
    print(busy.sort_values("Count", ascending=False)[cols].to_string(index=False))

    print("\n--- Satisfied rung per chosen SLA (the Pileus head-to-head metric) ---")
    rungs = totals.groupby("ChosenLevel")[["SatisfiedRung0", "SatisfiedRung1", "SatisfiedRung2",
                                           "SatisfiedRung3", "SatisfiedNone"]].sum()
    print(rungs[rungs.sum(axis=1) > 0].to_string())

    duration_s = (run.ledger["Timestamp"].max() - run.t0) / 1000.0
    served = totals["Count"].sum()
    up_free = totals["UpgradesFree"].sum()
    up_wait = totals["UpgradesWaiting"].sum()
    print("\n" + "=" * 64)
    print(f"RUN SUMMARY  mode={run.mode}  duration={duration_s:.0f}s  dir={run.dir}")
    print("=" * 64)
    print(f"served:            {served:,.0f}  ({served / max(duration_s, 1):,.0f}/s avg)")
    print(f"rejected:          {totals['Rejected'].sum():,.0f}")
    print(f"lost:              {totals['Lost'].sum():,.0f}")
    print(f"fallbacks:         {totals['Fallbacks'].sum():,.0f}")
    print(f"redirects:         {totals['Redirects'].sum():,.0f}")
    print(f"violations:        {totals['SessionViolations'].sum():,.0f}")
    print(f"predicted profit:  {totals['PredictedProfitSum'].sum():,.0f}")
    print(f"realized profit:   {totals['RealizedProfitSum'].sum():,.0f}")
    print(f"upgrades:          {up_free + up_wait:,.0f}  "
          f"(free {up_free / max(up_free + up_wait, 1):.1%})")
    for node, df in sorted(run.occupancy.items()):
        print(f"node {node}: meanU={df['U'].mean():.3f}  maxU={df['U'].max():.2f}  "
              f"maxLambda={df['Lambda'].max():.4f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir", help="run directory (contains client_metrics_global.csv)")
    parser.add_argument("--save", action="store_true",
                        help="save the figures as PNGs instead of showing them interactively")
    parser.add_argument("--out", help="output directory for PNGs (implies --save; default: <run_dir>/graphs)")
    args = parser.parse_args()
    save = args.save or args.out is not None

    # The backend must be chosen before pyplot is imported: Agg (headless)
    # when saving, the platform's interactive backend when showing.
    if save:
        matplotlib.use("Agg")
    global plt
    import matplotlib.pyplot as plt

    ledger_path = os.path.join(args.run_dir, "client_metrics_global.csv")
    if not os.path.exists(ledger_path):
        sys.exit(f"ERROR: {ledger_path} not found - is this a run directory?")

    run = Run(args.run_dir)
    if save:
        out_dir = args.out or os.path.join(args.run_dir, "graphs")
        os.makedirs(out_dir, exist_ok=True)
        out = lambda name: os.path.join(out_dir, name)  # noqa: E731
    else:
        out = None

    plot_throughput(run, out)
    plot_profit(run, out)
    plot_occupancy(run, out)
    plot_level_mix(run, out)
    plot_latency(run, out)
    plot_upgrades(run, out)
    if save:
        print(f"wrote 6 figures to {out_dir}/")

    print_tables(run)

    if not save:
        plt.show()


if __name__ == "__main__":
    main()
