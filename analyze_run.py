#!/usr/bin/env python3
"""Analyze one experiment run directory: the same analysis as graphs.ipynb.
Reads the CSVs a run writes into its own directory, renders the six figures,
and prints the text reports to stdout.

Data sources (per run directory):
  client_metrics_global.csv  client-side ledger, one row per (node, chosen,
                             executed) cell per ~1 s interval; counters reset
                             at every flush, so rows are per-interval activity
                             and the P50/P90/P95/P99 columns are that
                             interval's percentiles - all cross-arm comparison
                             numbers come from here
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

    def percentile_cols(self):
        """The typical-latency percentile columns this ledger has: P90Ms only
        exists in runs recorded after it was added to the tracker."""
        return ["P50Ms"] + (["P90Ms"] if "P90Ms" in self.ledger.columns else [])

    def whole_run(self):
        """Whole-run totals per cell: the sum of every interval row."""
        sum_cols = [c for c in self.ledger.columns
                    if c not in CELL_KEY + ["Timestamp", "Time_s", "AvgLatencyMs",
                                            "P50Ms", "P90Ms", "P95Ms", "P99Ms"]]
        df = self.ledger.copy()
        df["LatencySum"] = df["AvgLatencyMs"] * df["Count"]
        agg = df.groupby(CELL_KEY)[sum_cols + ["LatencySum"]].sum().reset_index()
        agg["AvgLatencyMs"] = agg["LatencySum"] / agg["Count"].clip(lower=1)
        # True percentiles cannot be merged across intervals (that would need
        # the raw buckets): P50/P90 are reported as the count-weighted mean of
        # the per-interval percentiles (a "typical interval" figure), P95/P99
        # as the worst interval.
        for col in self.percentile_cols():
            df[col + "_w"] = df[col] * df["Count"]
            weighted = df.groupby(CELL_KEY)[col + "_w"].sum().reset_index()
            agg = agg.merge(weighted, on=CELL_KEY)
            agg[col] = agg[col + "_w"] / agg["Count"].clip(lower=1)
            agg = agg.drop(columns=[col + "_w"])
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
    cols = ["Count", "Rejected", "Lost", "Fallbacks"]
    has_shed = "ShedAtClient" in run.ledger.columns
    if has_shed:
        cols.append("ShedAtClient")
    rates = run.interval_rates(cols)
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(rates["Time_s"], rates["Count"] / rates["dt_s"], label="served/s",
            color=PALETTE[0], **LINE)
    ax.plot(rates["Time_s"], rates["Rejected"] / rates["dt_s"], label="rejected/s",
            color=PALETTE[1], **LINE)
    if has_shed:
        ax.plot(rates["Time_s"], rates["ShedAtClient"] / rates["dt_s"], label="shed at client/s",
                color=PALETTE[2], **LINE)
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
    # Count-weighted P50/P90 across the interval's cells (an approximation of
    # the pooled percentile; exact pooling would need the raw buckets).
    cols = run.percentile_cols()
    lat = run.ledger.copy()
    for col in cols:
        lat[col + "_w"] = lat[col] * lat["Count"]
    by_t = lat.groupby("Timestamp")[[c + "_w" for c in cols] + ["Count"]].sum()
    for col in cols:
        by_t[col] = by_t[col + "_w"] / by_t["Count"].clip(lower=1)
    by_t.index = (by_t.index - run.t0) / 1000.0
    fig, ax = plt.subplots(figsize=(12, 4))
    for col, color in zip(cols, [PALETTE[0], PALETTE[6]]):
        ax.plot(by_t.index, by_t[col],
                label=col.replace("Ms", "").lower(), color=color, **LINE)
    run.phase_lines(ax)
    style(ax, "time [s]", "latency [ms]",
          f"Client-observed latency per interval ({run.mode})")
    ax.legend()
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


def plot_leadership(run, out):
    """Each node's self-reported Raft role per control interval: small dots
    while a node believes it is the leader, a red circle at every leader
    change (the first one is the initial election). During a partition a
    deposed leader that still believes it leads shows up as two rows at once,
    which is deliberate - the column records each node's own belief."""
    frames = []
    for node, df in run.occupancy.items():
        if "Role" not in df.columns:
            print("note: occupancy CSVs have no Role column (older run); skipping leadership plot")
            return
        frames.append((node, df[df["Role"] == "LEADER"]))
    fig, ax = plt.subplots(figsize=(12, 2.8))
    for node, led in frames:
        ax.plot(led["Time_s"], [node] * len(led), ".", markersize=3,
                color=NODE_COLORS[node % 8], label=f"node {node}")
    merged = pd.concat(
        [led.assign(Node=node)[["Time_s", "Node"]] for node, led in frames],
        ignore_index=True).sort_values("Time_s")
    if not merged.empty:
        changes = merged[merged["Node"].ne(merged["Node"].shift())]
        ax.plot(changes["Time_s"], changes["Node"], "o", markersize=11,
                markerfacecolor="none", markeredgewidth=1.8, color="#c22f2f",
                label="leader change")
    run.phase_lines(ax)
    ax.set_yticks(sorted(run.occupancy.keys()))
    ax.set_ylim(min(run.occupancy.keys()) - 0.5, max(run.occupancy.keys()) + 0.5)
    style(ax, "time [s]", "node", f"Leadership timeline ({run.mode}) - each node's own belief")
    ax.legend(ncol=6, fontsize=8)
    fig.tight_layout()
    if out:
        fig.savefig(out("leadership.png"), dpi=120)
        plt.close(fig)


def print_tables(run):
    totals = run.whole_run()

    print("\n--- Latency per busy cell (whole run; P50/P90 = count-weighted "
          "interval percentiles, P95/P99 = worst 1 s interval) ---")
    busy = totals[totals["Count"] > 0.01 * totals["Count"].sum()]
    cols = (["NodeId", "ChosenLevel", "ExecutedLevel", "Count"]
            + run.percentile_cols() + ["P95Ms", "P99Ms"])
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
    if "ShedAtClient" in totals.columns:
        print(f"shed at client:    {totals['ShedAtClient'].sum():,.0f}")
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


def print_phase_tables(run):
    """Per-phase, per-subSLA accounting. For every phase and every subSLA
    (ChosenLevel, e.g. R:A1S1): the outcome counts, average served/s over the
    phase, the execution rate (served / its own attempts), its share of the
    phase's served requests, upgrades and their free fraction, predicted and
    realized profit, and its share of the phase's realized profit."""
    if not run.phases:
        return
    # Terminal outcomes (the ExecRate denominator); redirects/fallbacks/
    # violations describe served requests, upgrades split by waiting.
    outcomes = ["Count", "Rejected", "Lost"]
    has_shed = "ShedAtClient" in run.ledger.columns
    if has_shed:
        outcomes.insert(1, "ShedAtClient")
    extras = ["Fallbacks", "Redirects", "SessionViolations",
              "UpgradesFree", "UpgradesWaiting", "PredictedProfitSum"]

    names, edges = [], [0.0]
    for i, p in enumerate(run.phases):
        names.append(p.get("name", f"phase{i}"))
        edges.append(edges[-1] + p["durationSeconds"])
    # Interval rows are stamped at flush time; the drain tail after the last
    # phase's configured end counts into the last phase.
    bins = edges[:-1] + [float("inf")]
    phase_idx = pd.cut(run.ledger["Time_s"], bins=bins, labels=False, right=False)
    t_max = run.ledger["Time_s"].max()

    for i, name in enumerate(names):
        sub = run.ledger[phase_idx == i]
        if sub.empty:
            continue
        g = sub.groupby("ChosenLevel")[outcomes + extras + ["RealizedProfitSum"]].sum()
        g = g[(g[outcomes].sum(axis=1) > 0) | (g["RealizedProfitSum"] > 0)]
        if g.empty:
            continue
        attempted = g[outcomes].sum(axis=1).clip(lower=1)
        phase_served = max(g["Count"].sum(), 1)
        phase_profit = g["RealizedProfitSum"].sum()
        # Rates use the phase's observed span: a cut-short run ends early, and
        # the last phase includes its drain tail.
        duration = max((t_max if i == len(names) - 1 else min(edges[i + 1], t_max)) - edges[i], 1.0)
        upgrades = g["UpgradesFree"] + g["UpgradesWaiting"]

        columns = {"Served": g["Count"].astype(int)}
        columns["Served/s"] = (g["Count"] / duration).round(0).astype(int)
        columns["Rejected"] = g["Rejected"].astype(int)
        if has_shed:
            columns["Shed"] = g["ShedAtClient"].astype(int)
        columns["Lost"] = g["Lost"].astype(int)
        columns["Fallbacks"] = g["Fallbacks"].astype(int)
        columns["Redirects"] = g["Redirects"].astype(int)
        columns["Viol"] = g["SessionViolations"].astype(int)
        columns["ExecRate%"] = (100.0 * g["Count"] / attempted).round(1)
        columns["ExecShare%"] = (100.0 * g["Count"] / phase_served).round(1)
        columns["Upgrades"] = upgrades.astype(int)
        columns["Free%"] = (100.0 * g["UpgradesFree"] / upgrades.clip(lower=1)).round(1)
        columns["PredProfit"] = g["PredictedProfitSum"].round(0).astype(int)
        columns["Profit"] = g["RealizedProfitSum"].round(0).astype(int)
        columns["ProfitShare%"] = (100.0 * g["RealizedProfitSum"]
                                   / (phase_profit if phase_profit > 0 else 1)).round(1)
        table = pd.DataFrame(columns)
        table.index.name = "SubSLA"

        end = "end" if bins[i + 1] == float("inf") else f"{edges[i + 1]:.0f}s"
        print(f"\n--- Phase {name} (t={edges[i]:.0f}s-{end}): "
              f"served={g['Count'].sum():,.0f} ({phase_served / duration:,.0f}/s)  "
              f"profit={phase_profit:,.0f} ---")
        print(table.sort_values("Profit", ascending=False).to_string())


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
    plot_leadership(run, out)
    if save:
        print(f"wrote 7 figures to {out_dir}/")

    print_tables(run)
    print_phase_tables(run)

    if not save:
        plt.show()


if __name__ == "__main__":
    main()
