#!/usr/bin/env bash

# Run the given command (a test binary with its arguments) and dump the stacks
# of all its threads with `gdb` if it is still running after a given time.
#
# This is meant as the `CMAKE_TEST_LAUNCHER` of `ctest` (see the `Dockerfile`).
# It was added to diagnose rare stalls of single unit tests in the arm64 Docker
# build, which ran for up to 40 minutes instead of a few milliseconds, and
# which so far always coincided with a corrupted test index.
#
# Usage: run-test-with-stall-watchdog.sh <command> [<args>...]
#
# The time after which the stacks are dumped is taken from the environment
# variable `QLEVER_TEST_STALL_TIMEOUT_SECONDS` (default: 60). After the dump,
# the command is resumed under `gdb` until it terminates, and the exit status
# is then always 124, so that the stall is reported as a failure even if the
# command eventually succeeds. To bound the total runtime, run `ctest` with
# `--timeout`.
#
# NOTE: The `gdb` replaces this script via `exec`, so that it becomes the
# parent of the command. That way, attaching to the command is also allowed
# with `kernel.yama.ptrace_scope = 1` (the default of Ubuntu), which only
# allows a process to trace its descendants.

set -u

if [ $# -eq 0 ]; then
  echo "Usage: $0 <command> [<args>...]" >&2
  exit 2
fi

STALL_TIMEOUT_SECONDS="${QLEVER_TEST_STALL_TIMEOUT_SECONDS:-60}"

"$@" &
PID=$!
sleep "$STALL_TIMEOUT_SECONDS" &
SLEEPER_PID=$!

# Wait for whichever of the two finishes first.
wait -n -p FINISHED_PID "$PID" "$SLEEPER_PID"
STATUS=$?
if [ "$FINISHED_PID" = "$PID" ]; then
  kill "$SLEEPER_PID" 2> /dev/null
  wait "$SLEEPER_PID" 2> /dev/null
  exit "$STATUS"
fi

echo ""
echo "=== STALL WATCHDOG: \`$*\` (pid $PID) is still running after" \
  "${STALL_TIMEOUT_SECONDS}s at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=== Load average: $(cat /proc/loadavg)"
echo "=== Status of pid $PID:"
grep -E '^(State|Threads|VmRSS|VmSwap|voluntary_ctxt_switches|nonvoluntary_ctxt_switches):' \
  "/proc/$PID/status"
echo "=== Memory:"
free -m
echo "=== All processes:"
ps -eo pid,ppid,stat,pcpu,rss,etime,args --sort=-pcpu

# Print one line per thread of the command, with its state, the CPU time that
# it has consumed so far (user + system, in clock ticks), and the kernel
# function in which it is waiting. This needs no `ptrace`, and printing it
# twice tells threads that spin apart from threads that are blocked.
print_threads() {
  echo "=== Threads of pid $PID (tid, name, state, CPU ticks, wchan):"
  for TASK in /proc/"$PID"/task/*; do
    # The name (field 2) is in parentheses and may contain spaces, so the
    # fields are counted from the closing parenthesis.
    STAT=$(cat "$TASK/stat" 2> /dev/null) || continue
    NAME="${STAT#*(}"
    NAME="${NAME%)*}"
    read -r -a FIELDS <<< "${STAT##*) }"
    echo "$(basename "$TASK") ($NAME) ${FIELDS[0]}" \
      "$((FIELDS[11] + FIELDS[12]))" "$(cat "$TASK/wchan" 2> /dev/null)"
  done
}
print_threads
sleep 2
print_threads

echo "=== Stacks of all threads of pid $PID:"
# NOTE: If the attaching fails, the `continue` fails as well, and the `kill`
# makes sure that the command doesn't outlive this script. Otherwise, the
# `kill` comes after the command has terminated, and is a no-op.
exec gdb -q -nx -batch -p "$PID" \
  -ex "set pagination off" \
  -iex "set debuginfod enabled off" \
  -ex "set confirm off" \
  -ex "set print thread-events off" \
  -ex "handle all nostop noprint pass" \
  -ex "info threads" \
  -ex "thread apply all bt" \
  -ex "echo === STALL WATCHDOG: resuming the command until it terminates\n" \
  -ex "continue" \
  -ex "echo === STALL WATCHDOG: failing the test because it stalled\n" \
  -ex "shell kill -9 $PID 2> /dev/null" \
  -ex "quit 124"
