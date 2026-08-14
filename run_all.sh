#!/usr/bin/env bash
#
# run_all.sh - all-in-one local experiment runner.
#
#   1. Preflight checks (java 17+, maven, tmux) - fails fast, no silent fallbacks
#   2. Builds the project (foreground, so build errors stop everything immediately)
#      and writes the runtime classpath to target-script/classpath.txt
#   3. Runs the experiment inside a detached tmux session "tuneable":
#        experiment - one ServerNode JVM per Raft node (logs to server_<id>.log)
#                     plus the WorkloadDriver JVM in the foreground
#        metrics    - live tail of client_metrics_global.csv
#   4. Every process runs with runs/<label>_<timestamp>/ as its working
#      directory, so all CSVs plus run.log, server_<id>.log, run_info.txt are
#      written there directly and the repo root stays clean.
#
# The calling terminal is never attached to tmux. The script blocks until the
# run completes, then prints a summary. On success the tmux session is killed
# automatically; on failure it is kept alive for inspection.
#
# Usage:
#   ./run_all.sh [label] [config.yaml]     e.g. ./run_all.sh upgrades-on
#                                               ./run_all.sh pressure my-config.yaml
# The config file (default: repo config.yaml; .json also accepted) is passed
# to every process and archived alongside the results for provenance.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$REPO_DIR/run_all.sh"
SESSION="tuneable"
CLASSPATH_FILE="$REPO_DIR/target-script/classpath.txt"

die() { echo "ERROR: $*" >&2; exit 1; }

# protobuf-java 3.x uses sun.misc.Unsafe, which JDK 24+ warns about on every
# JVM start; the acknowledgment flag does not exist before JDK 23, so it is
# gated on the java binary's major version. Shared by both modes.
JAVA_MAJOR="$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')"
JAVA_FLAGS=()
if [[ "$JAVA_MAJOR" =~ ^[0-9]+$ && "$JAVA_MAJOR" -ge 24 ]]; then
    JAVA_FLAGS+=(--sun-misc-unsafe-memory-access=allow)
fi

# ---------------------------------------------------------------------------
# Internal mode: this is what runs inside the tmux "experiment" window.
# Working directory: the run directory (set by tmux -c). All CSVs land here.
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "_experiment" ]]; then
    LABEL="$2"
    CONFIG_PATH="$3"
    RUN_DIR="$(pwd)"

    # Archive the exact config used before the run, for provenance.
    case "$CONFIG_PATH" in
        *.yaml|*.yml) CONFIG_ARCHIVE="config.yaml" ;;
        *)            CONFIG_ARCHIVE="config.json" ;;
    esac
    cp "$CONFIG_PATH" "$CONFIG_ARCHIVE"

    CP="$REPO_DIR/target-script/classes:$(cat "$CLASSPATH_FILE")"

    # Preflight-parse the config with the same strict loader the processes
    # use; this also tells us how many server processes to launch.
    CONFIG_INFO="$(java ${JAVA_FLAGS[@]+"${JAVA_FLAGS[@]}"} -cp "$CP" org.example.Utility.ExperimentConfig "$CONFIG_PATH")" \
        || { echo "=== Config rejected, aborting ==="; echo "exit_code:    65" > run_info.txt; exec bash; }
    NUM_SERVERS="$(printf '%s\n' "$CONFIG_INFO" | sed -n 's/^numServers=//p')"

    echo "=== Experiment starting (label: $LABEL) ==="
    echo "=== Working directory: $RUN_DIR ==="
    echo "=== Config: $CONFIG_PATH ==="
    echo "=== Cluster: $NUM_SERVERS server processes ==="
    echo

    # One process per Raft node. Their stdout goes to server_<id>.log; the
    # WorkloadDriver stops them over the Admin service when the run ends.
    SERVER_PIDS=()
    for ((i = 0; i < NUM_SERVERS; i++)); do
        java ${JAVA_FLAGS[@]+"${JAVA_FLAGS[@]}"} -cp "$CP" org.example.Server.ServerNode "$CONFIG_PATH" "$i" \
            > "server_$i.log" 2>&1 &
        SERVER_PIDS+=($!)
    done
    echo "Started $NUM_SERVERS server processes (pids: ${SERVER_PIDS[*]})"

    set +e
    java ${JAVA_FLAGS[@]+"${JAVA_FLAGS[@]}"} -cp "$CP" org.example.Client.WorkloadDriver "$CONFIG_PATH" 2>&1 | tee run.log
    RUN_EXIT=${PIPESTATUS[0]}
    set -e

    # The driver already requested shutdown over the Admin service; give the
    # server processes a moment to flush and exit, then escalate. SIGTERM
    # still triggers their orderly-teardown hook; SIGKILL is the last resort.
    DEADLINE=$(( $(date +%s) + 10 ))
    for pid in "${SERVER_PIDS[@]}"; do
        while kill -0 "$pid" 2>/dev/null && [[ $(date +%s) -lt $DEADLINE ]]; do
            sleep 0.5
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "Server process $pid still alive, sending SIGTERM"
            kill "$pid" 2>/dev/null || true
            sleep 2
            kill -9 "$pid" 2>/dev/null || true
        fi
    done

    # Record what was run so results stay comparable across config edits.
    # Written last: the orchestrator polls for this file to detect completion.
    {
        echo "label:        $LABEL"
        echo "date:         $(date '+%Y-%m-%d %H:%M:%S')"
        echo "exit_code:    $RUN_EXIT"
        echo "git revision: $(git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
        echo "git status:   $(git -C "$REPO_DIR" status --porcelain 2>/dev/null | wc -l | tr -d ' ') modified/untracked files"
        echo "java:         $(java -version 2>&1 | head -1)"
        echo "processes:    $NUM_SERVERS ServerNode + 1 WorkloadDriver"
        echo "config:       $CONFIG_PATH (archived as $CONFIG_ARCHIVE in this directory)"
    } > run_info.txt

    if [[ $RUN_EXIT -eq 0 ]]; then
        # Success: nothing to inspect, take the whole session down. This kills
        # our own pane, so it must be the last statement.
        tmux kill-session -t "$SESSION"
    else
        echo
        echo "=== Experiment FAILED (exit code $RUN_EXIT) - session kept alive for inspection ==="
        echo "=== Driver log: run.log | Server logs: server_<id>.log ==="
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
CONFIG_PATH="${2:-$REPO_DIR/config.yaml}"
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" 2>/dev/null && pwd)/$(basename "$CONFIG_PATH")" || die "config file not found: ${2:-$REPO_DIR/config.yaml}"
[[ -f "$CONFIG_PATH" ]] || die "config file not found: $CONFIG_PATH"
STAMP="$(date '+%Y%m%d_%H%M%S')"
RUN_DIR="$REPO_DIR/runs/${LABEL}_${STAMP}"
cd "$REPO_DIR"
[[ -f pom.xml ]] || die "pom.xml not found in $REPO_DIR - run this from the repo"

# --- Preflight ---
command -v tmux >/dev/null || die "tmux not found on PATH"
command -v mvn  >/dev/null || die "maven not found on PATH"
command -v java >/dev/null || die "java (17+) not found on PATH"

[[ "$JAVA_MAJOR" =~ ^[0-9]+$ ]] || die "could not parse java version from: $(java -version 2>&1 | head -1)"
[[ "$JAVA_MAJOR" -ge 17 ]] || die "java 17+ required, found major version $JAVA_MAJOR"

tmux has-session -t "$SESSION" 2>/dev/null && \
    die "tmux session '$SESSION' already exists (previous run?). Inspect or remove it: tmux kill-session -t $SESSION"

# --- Build (foreground: a build failure should stop everything, loudly) ---
# Built into target-script/ (see pom build.dir property) so the IDE's
# concurrent compilation into target/ can never corrupt what we run.
# Tests are skipped: this script only runs experiments; run the suite with
# `mvn clean install -Dbuild.dir=target-script` when changing code.
echo "Building project (tests skipped)..."
mvn clean install -Dbuild.dir=target-script -DskipTests

# The experiment JVMs are launched with plain `java -cp`, one process per
# Raft node plus the workload driver, so resolve the runtime classpath once.
echo "Resolving runtime classpath..."
mvn -q -Dbuild.dir=target-script dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"
[[ -s "$CLASSPATH_FILE" ]] || die "classpath resolution produced no output at $CLASSPATH_FILE"

# Validate the config up front and learn the run's real duration, from the
# same strict loader the experiment processes use.
CP="$REPO_DIR/target-script/classes:$(cat "$CLASSPATH_FILE")"
CONFIG_INFO="$(java ${JAVA_FLAGS[@]+"${JAVA_FLAGS[@]}"} -cp "$CP" org.example.Utility.ExperimentConfig "$CONFIG_PATH")" \
    || die "config validation failed: $CONFIG_PATH"
DURATION_S="$(printf '%s\n' "$CONFIG_INFO" | sed -n 's/^durationSeconds=//p')"
[[ "$DURATION_S" =~ ^[0-9]+$ ]] || die "could not read durationSeconds from the config helper"

# Simulated geo latency needs tc/netem and is a CloudLab concern; running it
# locally would silently produce a no-delay experiment.
GEO_ENABLED="$(printf '%s\n' "$CONFIG_INFO" | sed -n 's/^geoEnabled=//p')"
[[ "$GEO_ENABLED" == "true" ]] && \
    die "geo.enabled is true; run this config with ./run_all_cloudlab.sh instead (local runs apply no delays)"

mkdir -p "$RUN_DIR"

# --- tmux session (detached; the calling terminal is never attached) ---
echo "Creating tmux session '$SESSION'..."

tmux new-session -d -s "$SESSION" -n experiment -c "$RUN_DIR" \
    "'$SCRIPT_PATH' _experiment '$LABEL' '$CONFIG_PATH'"

# Live metrics: file appears once the client ledger starts flushing; tail -F waits for it.
tmux new-window -t "$SESSION" -n metrics -c "$RUN_DIR" \
    "echo 'Waiting for client_metrics_global.csv (appears once the workload starts)...'; tail -F client_metrics_global.csv"

echo
echo "Experiment running (label: $LABEL). Peek anytime with: tmux attach -t $SESSION"
echo "Waiting for completion (~${DURATION_S}s)..."

# On interrupt the experiment keeps running in tmux, so the delay rules must
# stay in place; clearing them here would silently change the experiment.
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
        echo "Analyze with: python3 analyze_run.py ${RUN_DIR#"$REPO_DIR"/}"
        echo "tmux session terminated."
    else
        echo "Run FAILED (exit code $RUN_EXIT). Partial data ($CSV_COUNT CSV files), run.log and server_<id>.log in ${RUN_DIR#"$REPO_DIR"/}"
        echo "tmux session '$SESSION' kept alive for inspection: tmux attach -t $SESSION"
        exit 1
    fi
else
    die "tmux session '$SESSION' disappeared before the run completed (killed externally?). Partial data may be in ${RUN_DIR#"$REPO_DIR"/}"
fi
