#!/usr/bin/env python3
"""Analyze one experiment run directory: the same analysis as graphs.ipynb,
headless. Reads the CSVs a run writes into its own directory, saves the plots
as PNGs into <run_dir>/graphs/, and prints the text reports to stdout.

Data sources (per run directory):
  client_metrics_global.csv  client-side ledger, cumulative per (node, chosen,
                             executed) cell, flushed every second - all
                             cross-arm comparison numbers come from here
  occupancy_<n>.csv          per node, per control interval: utilization U,
                             average in-flight, shadow price lambda
  config.json                the exact config of this run (mode, phases)

Usage:  python3 analyze_run.py runs/<label>_<stamp> [--out DIR]
Needs:  pandas, matplotlib
"""

import argparse
import glob
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

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
        self.phases = self.config.get("phases", [])
        self.single_phase = self.config.get("experiment", {}).get("runSinglePhase", False)

        self.ledger = pd.read_csv(os.path.join(run_dir, "client_metrics_global.csv"))
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

    def interval_deltas(self, columns):
        """Ledger rows are cumulative per cell: diff consecutive flushes per
        cell, then sum per flush timestamp for system-wide interval deltas."""
        df = self.ledger.sort_values("Timestamp").copy()
        for c in columns:
            df[c] = df.groupby(CELL_KEY)[c].diff().fillna(df[c])
        out = df.groupby("Timestamp")[columns].sum().reset_index()
        out["Time_s"] = (out["Timestamp"] - self.t0) / 1000.0
        out["dt_s"] = out["Timestamp"].diff().fillna(1000) / 1000.0
        return out

    def final_flush(self):
        return self.ledger[self.ledger["Timestamp"] == self.ledger["Timestamp"].max()]

    def phase_lines(self, ax):
        if self.single_phase or not self.phases:
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
    rates = run.interval_deltas(["CountTotal", "RejectedTotal", "LostTotal", "FallbacksTotal"])
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(rates["Time_s"], rates["CountTotal"] / rates["dt_s"], label="served/s",
            color=PALETTE[0], **LINE)
    ax.plot(rates["Time_s"], rates["RejectedTotal"] / rates["dt_s"], label="rejected/s",
            color=PALETTE[1], **LINE)
    ax.plot(rates["Time_s"], rates["FallbacksTotal"] / rates["dt_s"], label="fallbacks/s",
            color=PALETTE[3], **LINE)
    run.phase_lines(ax)
    style(ax, "time [s]", "requests/s", f"Throughput and shedding ({run.mode})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out("throughput.png"), dpi=120)
    plt.close(fig)


def plot_profit(run, out):
    profit = run.interval_deltas(["PredictedProfitSum", "RealizedProfitSum"])
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
    fig.savefig(out("occupancy.png"), dpi=120)
    plt.close(fig)


def plot_level_mix(run, out):
    df = run.ledger.sort_values("Timestamp").copy()
    df["Count_d"] = df.groupby(CELL_KEY)["CountTotal"].diff().fillna(df["CountTotal"])
    mix = (df[df["ExecutedLevel"] != "-"]
           .groupby(["Timestamp", "ExecutedLevel"])["Count_d"].sum().unstack(fill_value=0))
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
    fig.savefig(out("level_mix.png"), dpi=120)
    plt.close(fig)


def plot_latency(run, out):
    lat = run.ledger.sort_values("Timestamp").copy()
    lat["LatencySum"] = lat["AvgLatencyMs"] * lat["CountTotal"]
    lat["LatencySum_d"] = lat.groupby(CELL_KEY)["LatencySum"].diff().fillna(lat["LatencySum"])
    lat["Count_d"] = lat.groupby(CELL_KEY)["CountTotal"].diff().fillna(lat["CountTotal"])
    by_t = lat.groupby("Timestamp")[["LatencySum_d", "Count_d"]].sum()
    by_t["avg_ms"] = by_t["LatencySum_d"] / by_t["Count_d"].clip(lower=1)
    by_t.index = (by_t.index - run.t0) / 1000.0
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(by_t.index, by_t["avg_ms"], color=PALETTE[6], **LINE)
    run.phase_lines(ax)
    style(ax, "time [s]", "avg latency [ms]",
          f"Client-observed average latency per interval ({run.mode})")
    fig.tight_layout()
    fig.savefig(out("latency.png"), dpi=120)
    plt.close(fig)


def plot_upgrades(run, out):
    up = run.interval_deltas(["UpgradesFreeTotal", "UpgradesWaitingTotal"])
    total = (up["UpgradesFreeTotal"] + up["UpgradesWaitingTotal"]).clip(lower=1)
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(up["Time_s"], up["UpgradesFreeTotal"] / total, label="free fraction",
            color=PALETTE[2], **LINE)
    ax.plot(up["Time_s"], up["UpgradesWaitingTotal"] / total, label="waiting fraction",
            color=PALETTE[3], **LINE)
    run.phase_lines(ax)
    ax.set_ylim(0, 1)
    style(ax, "time [s]", "fraction of upgrades", f"Free vs waiting upgrades ({run.mode})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out("upgrades.png"), dpi=120)
    plt.close(fig)


def print_tables(run):
    final = run.final_flush()

    print("\n--- Latency tails per busy cell (cumulative, final flush) ---")
    busy = final[final["CountTotal"] > 0.01 * final["CountTotal"].sum()]
    cols = ["NodeId", "ChosenLevel", "ExecutedLevel", "CountTotal",
            "AvgLatencyMs", "P50Ms", "P95Ms", "P99Ms"]
    print(busy.sort_values("CountTotal", ascending=False)[cols].to_string(index=False))

    print("\n--- Satisfied rung per chosen SLA (the Pileus head-to-head metric) ---")
    rungs = final.groupby("ChosenLevel")[["SatisfiedRung0", "SatisfiedRung1", "SatisfiedRung2",
                                          "SatisfiedRung3", "SatisfiedNone"]].sum()
    print(rungs[rungs.sum(axis=1) > 0].to_string())

    duration_s = (run.ledger["Timestamp"].max() - run.t0) / 1000.0
    served = final["CountTotal"].sum()
    up_free = final["UpgradesFreeTotal"].sum()
    up_wait = final["UpgradesWaitingTotal"].sum()
    print("\n" + "=" * 64)
    print(f"RUN SUMMARY  mode={run.mode}  duration={duration_s:.0f}s  dir={run.dir}")
    print("=" * 64)
    print(f"served:            {served:,.0f}  ({served / max(duration_s, 1):,.0f}/s avg)")
    print(f"rejected:          {final['RejectedTotal'].sum():,.0f}")
    print(f"lost:              {final['LostTotal'].sum():,.0f}")
    print(f"fallbacks:         {final['FallbacksTotal'].sum():,.0f}")
    print(f"redirects:         {final['RedirectsTotal'].sum():,.0f}")
    print(f"violations:        {final['SessionViolationsTotal'].sum():,.0f}")
    print(f"predicted profit:  {final['PredictedProfitSum'].sum():,.0f}")
    print(f"realized profit:   {final['RealizedProfitSum'].sum():,.0f}")
    print(f"upgrades:          {up_free + up_wait:,.0f}  "
          f"(free {up_free / max(up_free + up_wait, 1):.1%})")
    for node, df in sorted(run.occupancy.items()):
        print(f"node {node}: meanU={df['U'].mean():.3f}  maxU={df['U'].max():.2f}  "
              f"maxLambda={df['Lambda'].max():.4f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir", help="run directory (contains client_metrics_global.csv)")
    parser.add_argument("--out", help="output directory for PNGs (default: <run_dir>/graphs)")
    args = parser.parse_args()

    ledger_path = os.path.join(args.run_dir, "client_metrics_global.csv")
    if not os.path.exists(ledger_path):
        sys.exit(f"ERROR: {ledger_path} not found - is this a run directory?")

    run = Run(args.run_dir)
    out_dir = args.out or os.path.join(args.run_dir, "graphs")
    os.makedirs(out_dir, exist_ok=True)
    out = lambda name: os.path.join(out_dir, name)  # noqa: E731

    plot_throughput(run, out)
    plot_profit(run, out)
    plot_occupancy(run, out)
    plot_level_mix(run, out)
    plot_latency(run, out)
    plot_upgrades(run, out)
    print(f"wrote 6 figures to {out_dir}/")

    print_tables(run)


if __name__ == "__main__":
    main()
