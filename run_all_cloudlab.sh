#!/usr/bin/env bash
#
# run_all_cloudlab.sh - run one experiment on a CloudLab node (Ubuntu 22.04,
# provisioned once with setup_cloudlab.sh) and fetch the results back.
#
#   1. Builds the fat jar locally and validates the config with the same
#      strict loader the experiment uses
#   2. Ships the jar, the config, and the remote runner to the node
#   3. If geo.enabled is true, applies the per-pair tc/netem delay rules on
#      the node's loopback (matching on source/destination IP; the servers
#      bind their outgoing connections to their configured 127.x addresses)
#   4. Runs the experiment inside a remote tmux session (it survives ssh
#      drops) while this terminal blocks, streaming run.log
#   5. Clears the delay rules and copies the remote run directory back into
#      local runs/<label>_<stamp>/ for analyze_run.py
#
# On Ctrl-C the experiment keeps running remotely; the printed instructions
# tell you how to attach, clear the delays, and fetch the results by hand.
#
# Usage: ./run_all_cloudlab.sh <user@host> <ssh-key-path> [label] [config.yaml]
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SESSION="tuneable"

die() { echo "ERROR: $*" >&2; exit 1; }

HOST="${1:-}"
KEY="${2:-}"
LABEL="${3:-run}"
CONFIG_PATH="${4:-$REPO_DIR/config.yaml}"
[[ -n "$HOST" && -n "$KEY" ]] || die "usage: ./run_all_cloudlab.sh <user@host> <ssh-key-path> [label] [config.yaml]"
[[ -f "$KEY" ]] || die "ssh key not found: $KEY"
[[ "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]] || die "label must match [A-Za-z0-9._-]+, got: $LABEL"
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" 2>/dev/null && pwd)/$(basename "$CONFIG_PATH")" || die "config file not found"
[[ -f "$CONFIG_PATH" ]] || die "config file not found: $CONFIG_PATH"
case "$CONFIG_PATH" in
    *.yaml|*.yml) ;;
    *) die "run_all_cloudlab requires a YAML config (cluster.serverHosts is rewritten for the node)" ;;
esac
cd "$REPO_DIR"

# The node runs everything on its own loopback: rewrite cluster.serverHosts
# to distinct literal IPs (127.0.1.1 .. 127.0.1.N), which Linux answers
# natively. This is what the geo delay rules and the servers' source binding
# key on, and it means the local config can keep plain "localhost" hosts.
# The .yaml suffix matters: the strict loader dispatches YAML vs JSON on the
# file extension.
CLOUD_CONFIG="$(mktemp).yaml"
awk '
    /^  serverHosts:/ { inblock = 1; n = 0; print; next }
    inblock && /^    - / { n++; print "    - 127.0.1." n; next }
    { inblock = 0; print }
' "$CONFIG_PATH" > "$CLOUD_CONFIG"
trap 'rm -f "$CLOUD_CONFIG" "${CLOUD_CONFIG%.yaml}"' EXIT

# One multiplexed ssh connection for everything (auth once, fast polling).
CTRL_PATH="/tmp/tuneable-cloudlab-$$"
SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30
          -o ControlMaster=auto -o "ControlPath=$CTRL_PATH" -o ControlPersist=120)
ssh_run() { ssh "${SSH_OPTS[@]}" "$HOST" "$@"; }

# protobuf-java 3.x uses sun.misc.Unsafe, which JDK 24+ warns about; only the
# LOCAL helper invocation needs this (the node runs JDK 17).
JAVA_MAJOR="$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')"
JAVA_FLAGS=()
[[ "$JAVA_MAJOR" =~ ^[0-9]+$ && "$JAVA_MAJOR" -ge 24 ]] && JAVA_FLAGS+=(--sun-misc-unsafe-memory-access=allow)

# --- Build the fat jar ---
echo "Building fat jar (tests skipped)..."
mvn -q clean package -Dbuild.dir=target-script -DskipTests
JAR="$(ls target-script/*-all.jar 2>/dev/null | head -1)"
[[ -n "$JAR" && -f "$JAR" ]] || die "fat jar not found in target-script/ (expected *-all.jar)"

# --- Validate the rewritten config and read what the orchestration needs ---
CONFIG_INFO="$(java ${JAVA_FLAGS[@]+"${JAVA_FLAGS[@]}"} -cp "$JAR" org.example.Utility.ExperimentConfig "$CLOUD_CONFIG")" \
    || die "config validation failed: $CONFIG_PATH (with serverHosts rewritten to 127.0.1.x)"
cfg() { printf '%s\n' "$CONFIG_INFO" | sed -n "s/^$1=//p"; }
NUM_SERVERS="$(cfg numServers)"
DURATION_S="$(cfg durationSeconds)"
GEO_ENABLED="$(cfg geoEnabled)"
[[ "$NUM_SERVERS" =~ ^[0-9]+$ ]] || die "could not read numServers from the config helper"

# --- Remote preflight ---
ssh_run "command -v java >/dev/null && command -v tmux >/dev/null" \
    || die "$HOST is missing java or tmux; provision it first: ./setup_cloudlab.sh $HOST $KEY"
ssh_run "tmux has-session -t $SESSION 2>/dev/null" \
    && die "tmux session '$SESSION' already exists on $HOST (previous run?). Inspect: ssh -i $KEY $HOST -t tmux attach -t $SESSION"

STAMP="$(date '+%Y%m%d_%H%M%S')"
RUN_NAME="${LABEL}_${STAMP}"
REMOTE_RUN="tuneable/runs/$RUN_NAME"

# --- Ship everything ---
echo "Shipping jar, config, and runner to $HOST..."
ssh_run "mkdir -p $REMOTE_RUN"
scp -q "${SSH_OPTS[@]}" "$JAR" "$HOST:tuneable/app-all.jar"
scp -q "${SSH_OPTS[@]}" "$REPO_DIR/cloudlab_remote_run.sh" "$HOST:tuneable/cloudlab_remote_run.sh"
CONFIG_ARCHIVE="config.yaml"
scp -q "${SSH_OPTS[@]}" "$CLOUD_CONFIG" "$HOST:$REMOTE_RUN/$CONFIG_ARCHIVE"

# --- Simulated geo latency (tc/netem on the node's loopback) ---
GEO_APPLIED=0
if [[ "$GEO_ENABLED" == "true" ]]; then
    IFS=',' read -r -a GEO_HOSTS <<< "$(cfg serverHosts)"
    IFS=';' read -r -a GEO_ROWS <<< "$(cfg geoInterServerLatencyMs)"
    GEO_CLIENT_CSV="$(cfg geoClientToServerLatencyMs)"
    GEO_NUM=${#GEO_HOSTS[@]}

    GEO_SCRIPT="$(mktemp)"
    GEO_RULE_INDEX=0
    {
        echo "set -euo pipefail"
        echo "tc qdisc del dev lo root 2>/dev/null || true"
        echo "tc qdisc add dev lo root handle 1: htb default 1"
        echo "tc class add dev lo parent 1: classid 1:1 htb rate 100gbit"
    } > "$GEO_SCRIPT"
    geo_nonzero() { awk "BEGIN{exit !($1 > 0)}"; }
    geo_emit() { # srcIp dstIp delayMs
        GEO_RULE_INDEX=$((GEO_RULE_INDEX + 1))
        local class=$((GEO_RULE_INDEX + 1))
        {
            echo "tc class add dev lo parent 1: classid 1:$class htb rate 100gbit"
            echo "tc qdisc add dev lo parent 1:$class netem delay $3ms"
            echo "tc filter add dev lo protocol ip parent 1: prio 1 u32 match ip src $1/32 match ip dst $2/32 flowid 1:$class"
        } >> "$GEO_SCRIPT"
    }
    for ((i = 0; i < GEO_NUM; i++)); do
        IFS=',' read -r -a GEO_ROW <<< "${GEO_ROWS[$i]}"
        for ((j = 0; j < GEO_NUM; j++)); do
            [[ $i -eq $j ]] && continue
            geo_nonzero "${GEO_ROW[$j]}" && geo_emit "${GEO_HOSTS[$i]}" "${GEO_HOSTS[$j]}" "${GEO_ROW[$j]}"
        done
    done
    IFS=',' read -r -a GEO_CLIENT <<< "$GEO_CLIENT_CSV"
    for ((j = 0; j < GEO_NUM; j++)); do
        if geo_nonzero "${GEO_CLIENT[$j]}"; then
            geo_emit 127.0.0.1 "${GEO_HOSTS[$j]}" "${GEO_CLIENT[$j]}"
            geo_emit "${GEO_HOSTS[$j]}" 127.0.0.1 "${GEO_CLIENT[$j]}"
        fi
    done

    echo "Applying simulated geo latency on $HOST ($GEO_RULE_INDEX delay rules)..."
    scp -q "${SSH_OPTS[@]}" "$GEO_SCRIPT" "$HOST:tuneable/geo_apply.sh"
    rm -f "$GEO_SCRIPT"
    ssh_run "sudo bash tuneable/geo_apply.sh"
    GEO_APPLIED=1
fi

# --- Launch inside remote tmux (survives ssh drops) ---
echo "Starting remote tmux session '$SESSION'..."
ssh_run "tmux new-session -d -s $SESSION -c \"\$HOME/$REMOTE_RUN\" \
    \"bash \$HOME/tuneable/cloudlab_remote_run.sh \$HOME/tuneable/app-all.jar $CONFIG_ARCHIVE $NUM_SERVERS $LABEL\""

echo
echo "Experiment running on $HOST (label: $LABEL). Attach with: ssh -i $KEY $HOST -t tmux attach -t $SESSION"
echo "Waiting for completion (~${DURATION_S}s)..."
echo

on_interrupt() {
    echo
    echo "Interrupted. The experiment keeps running on $HOST in tmux session '$SESSION'."
    echo "  attach:        ssh -i $KEY $HOST -t tmux attach -t $SESSION"
    if [[ "$GEO_APPLIED" == 1 ]]; then
        echo "  clear delays:  ssh -i $KEY $HOST 'sudo tc qdisc del dev lo root'"
    fi
    echo "  fetch results: scp -i $KEY -r $HOST:$REMOTE_RUN runs/"
    exit 130
}
trap on_interrupt INT

# Stream the driver's log while waiting; the poll below is authoritative.
ssh "${SSH_OPTS[@]}" "$HOST" "touch $REMOTE_RUN/run.log && tail -F $REMOTE_RUN/run.log" &
TAIL_PID=$!

while ! ssh_run "test -f $REMOTE_RUN/run_info.txt" 2>/dev/null; do
    ssh_run "tmux has-session -t $SESSION 2>/dev/null" || break
    sleep 5
done

trap - INT
kill "$TAIL_PID" 2>/dev/null || true
wait "$TAIL_PID" 2>/dev/null || true

# --- Clear delay rules ---
if [[ "$GEO_APPLIED" == 1 ]]; then
    echo "Clearing simulated geo latency on $HOST..."
    ssh_run "sudo tc qdisc del dev lo root 2>/dev/null || true"
fi

# --- Fetch results ---
ssh_run "test -f $REMOTE_RUN/run_info.txt" \
    || die "remote run ended without run_info.txt (session died?); inspect $HOST:$REMOTE_RUN"
echo "Fetching results into runs/$RUN_NAME/..."
mkdir -p "$REPO_DIR/runs"
scp -q -r "${SSH_OPTS[@]}" "$HOST:$REMOTE_RUN" "$REPO_DIR/runs/"

RUN_EXIT="$(sed -nE 's/^exit_code: *//p' "$REPO_DIR/runs/$RUN_NAME/run_info.txt")"
CSV_COUNT="$(find "$REPO_DIR/runs/$RUN_NAME" -maxdepth 1 -name "*.csv" | wc -l | tr -d ' ')"
echo
if [[ "$RUN_EXIT" == "0" ]]; then
    echo "Run complete: $CSV_COUNT CSV files in runs/$RUN_NAME"
    echo "Analyze with: python3 analyze_run.py runs/$RUN_NAME"
else
    echo "Run FAILED (exit code $RUN_EXIT). Partial data ($CSV_COUNT CSV files), run.log and server_<id>.log in runs/$RUN_NAME"
    exit 1
fi
