#!/usr/bin/env bash
#
# cloudlab_remote_run.sh - runs ON the CloudLab node, inside the tmux session
# started by run_all_cloudlab.sh. Launches one ServerNode JVM per Raft node
# plus the WorkloadDriver from the fat jar, waits for the driver to finish,
# then writes run_info.txt, which the local orchestrator polls for.
#
# Usage: cloudlab_remote_run.sh <fat-jar> <config> <numServers> <label>
# Working directory: the remote run directory (set by tmux -c); all CSVs and
# logs land there and are fetched back by run_all_cloudlab.sh.
#
set -euo pipefail

JAR="$1"
CONFIG="$2"
NUM_SERVERS="$3"
LABEL="$4"

echo "=== Experiment starting on $(hostname) (label: $LABEL) ==="
echo "=== Working directory: $(pwd) ==="
echo

SERVER_PIDS=()
for ((i = 0; i < NUM_SERVERS; i++)); do
    java -cp "$JAR" org.example.Server.ServerNode "$CONFIG" "$i" > "server_$i.log" 2>&1 &
    SERVER_PIDS+=($!)
done
echo "Started $NUM_SERVERS server processes (pids: ${SERVER_PIDS[*]})"

set +e
java -cp "$JAR" org.example.Client.WorkloadDriver "$CONFIG" 2>&1 | tee run.log
RUN_EXIT=${PIPESTATUS[0]}
set -e

# The driver already requested shutdown over the Admin service; give the
# server processes a moment to flush and exit, then escalate.
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

# Written last: the local orchestrator polls for this file.
{
    echo "label:        $LABEL"
    echo "date:         $(date '+%Y-%m-%d %H:%M:%S')"
    echo "exit_code:    $RUN_EXIT"
    echo "host:         $(hostname)"
    echo "java:         $(java -version 2>&1 | head -1)"
    echo "processes:    $NUM_SERVERS ServerNode + 1 WorkloadDriver"
    echo "config:       archived as $CONFIG in this directory"
} > run_info.txt

exit "$RUN_EXIT"
