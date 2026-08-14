#!/usr/bin/env bash
#
# setup_cloudlab.sh - one-time provisioning of a CloudLab node (Ubuntu 22.04)
# so run_all_cloudlab.sh can run experiments on it. Idempotent: re-run it
# after CloudLab reimages the node.
#
# Usage: ./setup_cloudlab.sh <user@host> <ssh-key-path>
#
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }

HOST="${1:-}"
KEY="${2:-}"
[[ -n "$HOST" && -n "$KEY" ]] || die "usage: ./setup_cloudlab.sh <user@host> <ssh-key-path>"
[[ -f "$KEY" ]] || die "ssh key not found: $KEY"

SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30)

echo "Provisioning $HOST..."
ssh "${SSH_OPTS[@]}" "$HOST" bash -s <<'REMOTE'
set -euo pipefail
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
    openjdk-17-jdk-headless tmux iproute2
mkdir -p "$HOME/tuneable/runs"
echo
echo "Installed:"
java -version 2>&1 | head -1
tmux -V
tc -V | head -1
REMOTE

echo
echo "Node ready. Run experiments with: ./run_all_cloudlab.sh $HOST $KEY [label] [config.yaml]"
