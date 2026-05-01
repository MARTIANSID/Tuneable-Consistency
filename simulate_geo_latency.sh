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
#   client_port = auto  (detect active callback in 9000..9100)
#               = none  (do not delay callback/client port)

ACTION="${1:-show}"
DELAY_MS="${2:-100}"
NUM_SERVERS="${3:-15}"
CLIENT_PORT="${4:-auto}"
BASE_SERVER_PORT=8000
CALLBACK_PORT_RANGE_START=9000
CALLBACK_PORT_RANGE_END=9100
DEV=lo

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Please run as root (use sudo)."
    exit 1
  fi
}

detect_callback_port() {
  if [[ "${CLIENT_PORT}" == "none" || "${CLIENT_PORT}" == "0" || "${CLIENT_PORT}" == "-1" ]]; then
    echo ""
    return
  fi

  if [[ "${CLIENT_PORT}" != "auto" ]]; then
    echo "${CLIENT_PORT}"
    return
  fi

  # Detect the active callback port selected by findAvailablePort(9000, 9100).
  # We choose the first listening TCP port in that range.
  local detected
  detected=$(ss -ltnH 2>/dev/null \
    | awk '{print $4}' \
    | sed -n 's/.*:\([0-9][0-9]*\)$/\1/p' \
    | awk -v s="${CALLBACK_PORT_RANGE_START}" -v e="${CALLBACK_PORT_RANGE_END}" '$1 >= s && $1 <= e' \
    | sort -n \
    | head -n 1)

  if [[ -n "${detected}" ]]; then
    echo "${detected}"
  else
    # Fallback keeps behavior deterministic if server is not started yet.
    echo "${CALLBACK_PORT_RANGE_START}"
  fi
}

ports_csv() {
  local ports=()
  local callback_port
  local i
  for ((i=1; i<=NUM_SERVERS; i++)); do
    ports+=("$((BASE_SERVER_PORT + i))")
  done
  callback_port="$(detect_callback_port)"
  if [[ -n "${callback_port}" ]]; then
    ports+=("${callback_port}")
  fi
  local IFS=,
  echo "${ports[*]}"
}

apply_latency() {
  require_root
  local selected_callback_port
  selected_callback_port="$(detect_callback_port)"

  if [[ "${CLIENT_PORT}" == "auto" ]]; then
    echo "Detected callback port: ${selected_callback_port} (range ${CALLBACK_PORT_RANGE_START}-${CALLBACK_PORT_RANGE_END})"
  elif [[ -z "${selected_callback_port}" ]]; then
    echo "Client callback latency disabled (client_port=${CLIENT_PORT})."
  fi

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
