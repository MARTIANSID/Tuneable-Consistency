#!/usr/bin/env bash
set -euo pipefail

# Simulate geo-distributed latency on loopback traffic for this project.
# Delays packets to/from selected TCP service ports using tc/netem.
#
# Usage:
#   sudo ./simulate_geo_latency.sh apply [delay_ms] [num_servers] [client_port]
#   sudo ./simulate_geo_latency.sh clear
#   sudo ./simulate_geo_latency.sh show
#
# Defaults:
#   delay_ms    = 100
#   num_servers = 15   (ports 8001..(8000+num_servers))
#   client_port = 9000

ACTION="${1:-show}"
DELAY_MS="${2:-100}"
NUM_SERVERS="${3:-15}"
CLIENT_PORT="${4:-9000}"
BASE_SERVER_PORT=8000
DEV=lo

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Please run as root (use sudo)."
    exit 1
  fi
}

ports_csv() {
  local ports=()
  local i
  for ((i=1; i<=NUM_SERVERS; i++)); do
    ports+=("$((BASE_SERVER_PORT + i))")
  done
  ports+=("${CLIENT_PORT}")
  # If your client ACK server is on 9001 in this setup, uncomment below:
  # ports+=("9001")
  local IFS=,
  echo "${ports[*]}"
}

apply_latency() {
  require_root

  echo "Applying ${DELAY_MS}ms netem delay on ${DEV} for ports: $(ports_csv)"

  # Reset any previous setup from this script.
  tc qdisc del dev "${DEV}" root 2>/dev/null || true

  # Root prio qdisc: band 1 = delayed traffic, band 2/3 = normal traffic.
  tc qdisc add dev "${DEV}" root handle 1: prio
  tc qdisc add dev "${DEV}" parent 1:1 handle 10: netem delay "${DELAY_MS}"ms

  # Match both destination and source port so request and response paths are delayed.
  local prio=1
  local p
  for p in $(ports_csv | tr ',' ' '); do
    tc filter add dev "${DEV}" protocol ip parent 1: prio "${prio}" u32 \
      match ip protocol 6 0xff \
      match ip dport "${p}" 0xffff flowid 1:1
    prio=$((prio + 1))

    tc filter add dev "${DEV}" protocol ip parent 1: prio "${prio}" u32 \
      match ip protocol 6 0xff \
      match ip sport "${p}" 0xffff flowid 1:1
    prio=$((prio + 1))
  done

  echo "Done."
  tc -s qdisc show dev "${DEV}"
}

clear_latency() {
  require_root
  echo "Clearing netem rules on ${DEV}"
  tc qdisc del dev "${DEV}" root 2>/dev/null || true
  echo "Done."
}

show_latency() {
  echo "qdisc on ${DEV}:"
  tc -s qdisc show dev "${DEV}" || true
  echo
  echo "filters on ${DEV}:"
  tc filter show dev "${DEV}" parent 1: || true
}

case "${ACTION}" in
  apply)
    apply_latency
    ;;
  clear)
    clear_latency
    ;;
  show)
    show_latency
    ;;
  *)
    echo "Unknown action: ${ACTION}"
    echo "Usage: sudo ./simulate_geo_latency.sh {apply [delay_ms] [num_servers] [client_port]|clear|show}"
    exit 1
    ;;
esac
