#!/usr/bin/env bash
#
# run_all.sh - all-in-one local experiment runner.
#
#   1. Preflight checks (java 17+, maven, redis, tmux) - fails fast, no silent fallbacks
#   2. Builds the project (foreground, so build errors stop everything immediately)
#   3. Runs the experiment inside a detached tmux session "tuneable":
#        redis      - redis-server (only if one is not already running on 6379)
#        experiment - the run itself
#        metrics    - live tail of system_tps_global.csv
#   4. The experiment executes with runs/<label>_<timestamp>/ as its working
#      directory, so every CSV plus run.log and run_info.txt is written there
#      directly and the repo root stays clean.
#
# The calling terminal is never attached to tmux. The script blocks until the
# run completes, then prints a summary. On success the tmux session is killed
# automatically; on failure it is kept alive for inspection.
#
# Geo latency simulation (simulate_geo_latency.sh) is intentionally not part of
# this script - it requires Linux tc/netem and ENABLE_GEO_SETTINGS is off anyway.
#
# Usage:
#   ./run_all.sh [label] [config.json]     e.g. ./run_all.sh upgrades-on
#                                               ./run_all.sh pressure my-config.json
# The config file (default: repo config.json) is passed to the experiment and
# archived alongside the results for provenance.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$REPO_DIR/run_all.sh"
SESSION="tuneable"
REDIS_PORT=6379

die() { echo "ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Internal mode: this is what runs inside the tmux "experiment" window.
# Working directory: the run directory (set by tmux -c). All CSVs land here.
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "_experiment" ]]; then
    LABEL="$2"
    CONFIG_PATH="$3"
    RUN_DIR="$(pwd)"

    # Archive the exact config used before the run, for provenance.
    cp "$CONFIG_PATH" config.json

    echo "=== Experiment starting (label: $LABEL) ==="
    echo "=== Working directory: $RUN_DIR ==="
    echo "=== Config: $CONFIG_PATH ==="
    echo

    set +e
    mvn -f "$REPO_DIR/pom.xml" -Dbuild.dir=target-script exec:java \
        -Dexec.mainClass="org.example.Server.Servers" \
        -Dexec.args="$CONFIG_PATH" 2>&1 | tee run.log
    RUN_EXIT=${PIPESTATUS[0]}
    set -e

    # Copy (not move) the analysis notebook next to the data, matching how
    # previous result folders (Baseline_*, Chamaleon_*) were laid out.
    [[ -f "$REPO_DIR/graphs.ipynb" ]] && cp "$REPO_DIR/graphs.ipynb" .

    # Record what was run so results stay comparable across config edits.
    # Written last: the orchestrator polls for this file to detect completion.
    {
        echo "label:        $LABEL"
        echo "date:         $(date '+%Y-%m-%d %H:%M:%S')"
        echo "exit_code:    $RUN_EXIT"
        echo "git revision: $(git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
        echo "git status:   $(git -C "$REPO_DIR" status --porcelain 2>/dev/null | wc -l | tr -d ' ') modified/untracked files"
        echo "java:         $(java -version 2>&1 | head -1)"
        echo "config:       $CONFIG_PATH (archived as config.json in this directory)"
    } > run_info.txt

    if [[ $RUN_EXIT -eq 0 ]]; then
        # Success: nothing to inspect, take the whole session down (this also
        # stops redis if this script started it). This kills our own pane, so
        # it must be the last statement.
        tmux kill-session -t "$SESSION"
    else
        echo
        echo "=== Experiment FAILED (exit code $RUN_EXIT) - session kept alive for inspection ==="
        echo "=== Kill it with: tmux kill-session -t $SESSION ==="
        exec bash
    fi
    exit 0
fi

# ---------------------------------------------------------------------------
# Orchestrator mode.
# ---------------------------------------------------------------------------
LABEL="${1:-run}"
[[ "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]] || die "label must match [A-Za-z0-9._-]+, got: $LABEL"
CONFIG_PATH="${2:-$REPO_DIR/config.json}"
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" 2>/dev/null && pwd)/$(basename "$CONFIG_PATH")" || die "config file not found: ${2:-$REPO_DIR/config.json}"
[[ -f "$CONFIG_PATH" ]] || die "config file not found: $CONFIG_PATH"
STAMP="$(date '+%Y%m%d_%H%M%S')"
RUN_DIR="$REPO_DIR/runs/${LABEL}_${STAMP}"
cd "$REPO_DIR"
[[ -f pom.xml ]] || die "pom.xml not found in $REPO_DIR - run this from the repo"

# --- Preflight ---
command -v tmux >/dev/null || die "tmux not found. Install: brew install tmux"
command -v mvn  >/dev/null || die "maven not found. Install: brew install maven"
command -v java >/dev/null || die "java not found. Install: brew install openjdk@17"

JAVA_MAJOR="$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')"
[[ "$JAVA_MAJOR" =~ ^[0-9]+$ ]] || die "could not parse java version from: $(java -version 2>&1 | head -1)"
[[ "$JAVA_MAJOR" -ge 17 ]] || die "java 17+ required, found major version $JAVA_MAJOR"

tmux has-session -t "$SESSION" 2>/dev/null && \
    die "tmux session '$SESSION' already exists (previous run?). Inspect or remove it: tmux kill-session -t $SESSION"

# --- Redis ---
REDIS_STARTED_BY_US=0
if command -v redis-cli >/dev/null && [[ "$(redis-cli -p $REDIS_PORT ping 2>/dev/null)" == "PONG" ]]; then
    echo "Redis already running on port $REDIS_PORT - using it (will not be stopped by this script)."
else
    command -v redis-server >/dev/null || die "redis-server not found and no Redis running on port $REDIS_PORT. Install: brew install redis"
    REDIS_STARTED_BY_US=1
fi

# --- Build (foreground: a build failure should stop everything, loudly) ---
# Built into target-script/ (see pom build.dir property) so the IDE's
# concurrent compilation into target/ can never corrupt what we run.
echo "Building project..."
mvn clean install -Dbuild.dir=target-script

mkdir -p "$RUN_DIR"

# --- tmux session (detached; the calling terminal is never attached) ---
echo "Creating tmux session '$SESSION'..."

if [[ $REDIS_STARTED_BY_US -eq 1 ]]; then
    tmux new-session -d -s "$SESSION" -n redis -c "$REPO_DIR" \
        "redis-server --port $REDIS_PORT --save '' --appendonly no"
    # Wait until Redis answers before the experiment tries to use it.
    for i in $(seq 1 20); do
        [[ "$(redis-cli -p $REDIS_PORT ping 2>/dev/null)" == "PONG" ]] && break
        [[ $i -eq 20 ]] && die "Redis did not respond on port $REDIS_PORT within 10s (see tmux window 'redis')"
        sleep 0.5
    done
    echo "Redis started inside tmux (dies with the session)."
    tmux new-window -t "$SESSION" -n experiment -c "$RUN_DIR" \
        "'$SCRIPT_PATH' _experiment '$LABEL' '$CONFIG_PATH'"
else
    tmux new-session -d -s "$SESSION" -n experiment -c "$RUN_DIR" \
        "'$SCRIPT_PATH' _experiment '$LABEL' '$CONFIG_PATH'"
fi

# Live metrics: file appears once the leader starts writing; tail -F waits for it.
tmux new-window -t "$SESSION" -n metrics -c "$RUN_DIR" \
    "echo 'Waiting for system_tps_global.csv (appears after leader election)...'; tail -F system_tps_global.csv"

echo
echo "Experiment running (label: $LABEL). Peek anytime with: tmux attach -t $SESSION"
echo "Waiting for completion (~250s)..."

trap 'echo; echo "Interrupted. The experiment is still running in tmux session '"'"'$SESSION'"'"'."; echo "Kill it with: tmux kill-session -t $SESSION"; exit 130' INT

# Block until the experiment writes run_info.txt (normal completion, success or
# failure) or the session disappears (killed externally).
while [[ ! -f "$RUN_DIR/run_info.txt" ]]; do
    tmux has-session -t "$SESSION" 2>/dev/null || break
    sleep 2
done

trap - INT

echo
if [[ -f "$RUN_DIR/run_info.txt" ]]; then
    RUN_EXIT="$(sed -nE 's/^exit_code: *//p' "$RUN_DIR/run_info.txt")"
    CSV_COUNT="$(find "$RUN_DIR" -maxdepth 1 -name "*.csv" | wc -l | tr -d ' ')"
    if [[ "$RUN_EXIT" == "0" ]]; then
        echo "Run complete: $CSV_COUNT CSV files in ${RUN_DIR#"$REPO_DIR"/}"
        echo "tmux session terminated."
    else
        echo "Run FAILED (exit code $RUN_EXIT). Partial data ($CSV_COUNT CSV files) and run.log in ${RUN_DIR#"$REPO_DIR"/}"
        echo "tmux session '$SESSION' kept alive for inspection: tmux attach -t $SESSION"
        exit 1
    fi
else
    die "tmux session '$SESSION' disappeared before the run completed (killed externally?). Partial data may be in ${RUN_DIR#"$REPO_DIR"/}"
fi
