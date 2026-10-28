#!/usr/bin/env bash

# Run the given gtest cases over and over again, until one of them fails or a
# given time is up. Each run is a fresh process (so that the test index of the
# test is built again every time, see `getQec`), which is started via
# `run-test-with-stall-watchdog.sh` (in the same directory as this script).
# Several workers run concurrently, each in its own working directory (the test
# indices are written to the working directory, under a name that only depends
# on the test).
#
# Usage: repeat-tests-until-failure.sh <seconds> <numWorkers> <test>...
#
# Each <test> has the form `<path to test binary>:<gtest filter>`. Only the
# output of the first failed run is printed (completely). Apart from that,
# only runs that took unusually long and a progress line every ten minutes are
# printed, to keep the output small (the log of a step of a Docker build is
# truncated after a few MB). The exit status is 1 if a run failed, else 0.
#
# This is used to diagnose a rare stall and corruption of the test index in
# the arm64 Docker build, see the `Dockerfile`.

set -u

if [ $# -lt 3 ]; then
  echo "Usage: $0 <seconds> <numWorkers> <binary:gtestFilter>..." >&2
  exit 2
fi

DURATION_SECONDS=$1
NUM_WORKERS=$2
shift 2
TESTS=("$@")

WATCHDOG="$(cd "$(dirname "$0")" && pwd)/run-test-with-stall-watchdog.sh"
WORK_DIR="$(mktemp -d)"
FAILURE_MARKER="$WORK_DIR/failed"
END_TIME=$(($(date +%s) + DURATION_SECONDS))
# A run that takes longer than this is reported, even if it passes.
SLOW_RUN_MILLISECONDS=2000

# Make the paths of the test binaries absolute, because the workers change
# their working directory.
for i in "${!TESTS[@]}"; do
  BINARY="${TESTS[$i]%%:*}"
  TESTS[$i]="$(cd "$(dirname "$BINARY")" && pwd)/$(basename "$BINARY"):${TESTS[$i]#*:}"
done

echo "Repeating ${#TESTS[@]} tests with $NUM_WORKERS workers for" \
  "${DURATION_SECONDS}s: ${TESTS[*]}"

# Run the tests in a loop until the time is up or any worker has found a
# failure. The first worker that finds a failure prints its output.
worker() {
  local id=$1
  local dir="$WORK_DIR/worker$id"
  local log="$dir/output.log"
  mkdir -p "$dir"
  cd "$dir" || exit 2
  local numRuns=0
  local nextProgress=$(($(date +%s) + 600))
  while [ "$(date +%s)" -lt "$END_TIME" ] && [ ! -e "$FAILURE_MARKER" ]; do
    for test in "${TESTS[@]}"; do
      local binary="${test%%:*}"
      local filter="${test#*:}"
      local startMs
      startMs=$(($(date +%s%N) / 1000000))
      "$WATCHDOG" "$binary" --gtest_filter="$filter" > "$log" 2>&1
      local status=$?
      local durationMs=$(($(date +%s%N) / 1000000 - startMs))
      numRuns=$((numRuns + 1))
      if [ "$durationMs" -gt "$SLOW_RUN_MILLISECONDS" ]; then
        echo "=== SLOW RUN: $filter took ${durationMs}ms (worker $id," \
          "run $numRuns, status $status)"
      fi
      if [ "$status" -ne 0 ]; then
        # `mkdir` is atomic, so only the first failure is printed.
        if mkdir "$FAILURE_MARKER" 2> /dev/null; then
          echo "=== FAILURE: $filter (worker $id, run $numRuns, status" \
            "$status, ${durationMs}ms, $(date -u +%Y-%m-%dT%H:%M:%SZ)):"
          cat "$log"
        fi
        return
      fi
    done
    if [ "$(date +%s)" -ge "$nextProgress" ]; then
      echo "=== Worker $id: $numRuns runs without a failure" \
        "($(date -u +%Y-%m-%dT%H:%M:%SZ))"
      nextProgress=$((nextProgress + 600))
    fi
  done
  echo "=== Worker $id finished after $numRuns runs"
}

for id in $(seq 1 "$NUM_WORKERS"); do
  worker "$id" &
done
wait

if [ -e "$FAILURE_MARKER" ]; then
  exit 1
fi
echo "=== No failure within ${DURATION_SECONDS}s"
exit 0
