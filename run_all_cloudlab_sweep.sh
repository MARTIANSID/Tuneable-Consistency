#!/usr/bin/env bash
#
# run_all_cloudlab_sweep.sh - run one experiment per mode on a CloudLab node
# by invoking run_all_cloudlab.sh once per mode with the base config's
# `mode:` line rewritten. Runs are sequential (the runner blocks until the
# remote experiment finishes and fetches its results), and every generated
# config is validated with the strict loader BEFORE the first run starts, so
# a typo in --modes fails in seconds instead of hours into the sweep.
#
# Each run gets the label <label>_<mode>, so results land in
# runs/<label>_<mode>_<stamp>/ ready for analyze_run.py.
#
# On a failed or interrupted run the sweep stops and prints the modes that
# still remain, so it can be resumed with a narrowed --modes list.
#
# Usage:
#   ./run_all_cloudlab_sweep.sh <user@host> <ssh-key-path> [label] \
#       --modes "chameleon;chameleonPileus;pileus;highestProfit;lowestProfit" \
#       <config.yaml>
#
# --modes takes a ';'-separated list and may appear anywhere among the
# arguments; the remaining positionals are host, key, optional label
# (default "sweep"), and the base config, in that order.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() { echo "ERROR: $*" >&2; exit 1; }

# --- Arguments ---
MODES_RAW=""
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --modes)
            [[ $# -ge 2 ]] || die "--modes requires a ';'-separated list of modes"
            MODES_RAW="$2"
            shift 2
            ;;
        --modes=*)
            MODES_RAW="${1#--modes=}"
            shift
            ;;
        --*)
            die "unknown flag: $1"
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done

USAGE="usage: ./run_all_cloudlab_sweep.sh <user@host> <ssh-key-path> [label] --modes \"m1;m2;...\" <config.yaml>"
[[ ${#POSITIONAL[@]} -ge 3 && ${#POSITIONAL[@]} -le 4 ]] || die "$USAGE"
HOST="${POSITIONAL[0]}"
KEY="${POSITIONAL[1]}"
if [[ ${#POSITIONAL[@]} -eq 4 ]]; then
    LABEL="${POSITIONAL[2]}"
    CONFIG_PATH="${POSITIONAL[3]}"
else
    LABEL="sweep"
    CONFIG_PATH="${POSITIONAL[2]}"
fi

[[ -n "$MODES_RAW" ]] || die "--modes is required. $USAGE"
[[ -f "$KEY" ]] || die "ssh key not found: $KEY"
[[ "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]] || die "label must match [A-Za-z0-9._-]+, got: $LABEL"
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" 2>/dev/null && pwd)/$(basename "$CONFIG_PATH")" || die "config file not found"
[[ -f "$CONFIG_PATH" ]] || die "config file not found: $CONFIG_PATH"
case "$CONFIG_PATH" in
    *.yaml|*.yml) ;;
    *) die "the sweep requires a YAML config (the mode: line is rewritten textually)" ;;
esac
grep -Ecq '^mode: ' "$CONFIG_PATH" || die "no top-level 'mode:' line found in $CONFIG_PATH"
[[ "$(grep -Ec '^mode: ' "$CONFIG_PATH")" == 1 ]] || die "multiple top-level 'mode:' lines in $CONFIG_PATH"

IFS=';' read -r -a MODES <<< "$MODES_RAW"
CLEANED=()
for m in "${MODES[@]}"; do
    m="${m//[[:space:]]/}"
    [[ -z "$m" ]] && continue
    [[ "$m" =~ ^[A-Za-z]+$ ]] || die "mode name must be alphabetic, got: '$m'"
    CLEANED+=("$m")
done
[[ ${#CLEANED[@]} -ge 1 ]] || die "--modes contained no modes: '$MODES_RAW'"
MODES=("${CLEANED[@]}")

cd "$REPO_DIR"

# --- Generate one config per mode (kept for the whole sweep, then removed) ---
SWEEP_TMP="$(mktemp -d)"
trap 'rm -rf "$SWEEP_TMP"' EXIT
declare -a MODE_CONFIGS=()
for m in "${MODES[@]}"; do
    mode_config="$SWEEP_TMP/$m.yaml"
    sed -E "s/^mode: .*/mode: $m/" "$CONFIG_PATH" > "$mode_config"
    grep -q "^mode: $m\$" "$mode_config" || die "failed to rewrite mode: line for '$m'"
    MODE_CONFIGS+=("$mode_config")
done

# --- Validate every generated config before the first run ---
# Same build and strict loader run_all_cloudlab.sh uses; an unknown mode or
# any other config problem dies here, before anything touches the node.
JAVA_MAJOR="$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')"
JAVA_FLAGS=()
[[ "$JAVA_MAJOR" =~ ^[0-9]+$ && "$JAVA_MAJOR" -ge 24 ]] && JAVA_FLAGS+=(--sun-misc-unsafe-memory-access=allow)
echo "Building fat jar to pre-validate ${#MODES[@]} mode configs..."
mvn -q clean package -Dbuild.dir=target-script -DskipTests
JAR="$(ls target-script/*-all.jar 2>/dev/null | head -1)"
[[ -n "$JAR" && -f "$JAR" ]] || die "fat jar not found in target-script/ (expected *-all.jar)"
for i in "${!MODES[@]}"; do
    java ${JAVA_FLAGS[@]+"${JAVA_FLAGS[@]}"} -cp "$JAR" org.example.Utility.ExperimentConfig \
        "${MODE_CONFIGS[$i]}" > /dev/null \
        || die "config validation failed for mode '${MODES[$i]}' (base: $CONFIG_PATH)"
done
echo "All ${#MODES[@]} mode configs validate: ${MODES[*]}"

# --- Run the sweep sequentially ---
for i in "${!MODES[@]}"; do
    m="${MODES[$i]}"
    echo
    echo "================================================================"
    echo "Sweep run $((i + 1))/${#MODES[@]}: mode=$m (label: ${LABEL}_${m})"
    echo "================================================================"
    if ! "$REPO_DIR/run_all_cloudlab.sh" "$HOST" "$KEY" "${LABEL}_${m}" "${MODE_CONFIGS[$i]}"; then
        remaining=("${MODES[@]:$((i + 1))}")
        echo
        echo "Sweep stopped: mode '$m' failed (its partial results, if any, are under runs/${LABEL}_${m}_*)." >&2
        if [[ ${#remaining[@]} -gt 0 ]]; then
            remaining_list="$(IFS=';'; echo "${remaining[*]}")"
            echo "Resume the rest with:" >&2
            echo "  ./run_all_cloudlab_sweep.sh $HOST $KEY $LABEL --modes \"$m;$remaining_list\" $CONFIG_PATH" >&2
        fi
        exit 1
    fi
done

echo
echo "Sweep complete: ${#MODES[@]} runs."
for m in "${MODES[@]}"; do
    latest="$(ls -dt runs/${LABEL}_${m}_* 2>/dev/null | head -1 || true)"
    echo "  $m -> ${latest:-runs/${LABEL}_${m}_<stamp>}"
done
echo "Analyze each with: python3 analyze_run.py <run-dir>"
