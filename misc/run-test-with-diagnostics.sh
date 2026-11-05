#!/usr/bin/env bash

# Run the given command (a test binary with its arguments) and collect
# diagnostics if it stalls or crashes. The exit status is always the one of the
# command itself, so the diagnostics never change whether a test passes.
#
# This is meant as the `CMAKE_TEST_LAUNCHER` of `ctest` (see the `Dockerfile`).
# It was added to diagnose rare failures of single unit tests in the arm64
# Docker build: a corrupted test index (unwritten heap memory in a single
# column of POS) or a `Bus error`, which often coincided with a stall of the
# test for seconds to more than an hour (instead of a few milliseconds).
#
# Usage: run-test-with-diagnostics.sh <command> [<args>...]
#
# Environment variables:
#   QLEVER_TEST_STALL_SECONDS: The time after which a test counts as stalled
#     (default: 30). Healthy tests of the Docker build take at most 5 s.
#   QLEVER_TEST_DIAGNOSTICS_DIR: If set, the diagnostics are also appended to a
#     file in this directory, because `ctest --output-on-failure` only shows
#     the output of failed tests, but a stalled test may still pass.
#
# What is collected:
#
# 1. If the command is still running after `QLEVER_TEST_STALL_SECONDS`, two
#    samples (2 s apart) of the state, the consumed CPU time, the time spent
#    waiting for a CPU, and the kernel wait channel of each of its threads, the
#    CPU steal time and pressure of the machine, and the stacks of all threads
#    (via `gdb`). Then `gdb` stays attached until the command terminates, so
#    that a later crash is reported with the signal info, the registers, and
#    the stacks of all threads.
#
# 2. If the command is terminated by a signal before it counts as stalled, the
#    same crash report from its core dump, if there is one. This requires that
#    `kernel.core_pattern` is `core.%p` (which the CI sets on the host, the
#    setting is not namespaced). The core dump is deleted afterwards.
#
# NOTE: After a stall, `gdb` replaces this script via `exec`, so that it becomes
# the parent of the command. That way, attaching to the command is also allowed
# with `kernel.yama.ptrace_scope = 1` (the default of Ubuntu), which only
# allows a process to trace its descendants.

set -u

if [ $# -eq 0 ]; then
  echo "Usage: $0 <command> [<args>...]" >&2
  exit 2
fi

STALL_SECONDS="${QLEVER_TEST_STALL_SECONDS:-30}"
DIAGNOSTICS_DIR="${QLEVER_TEST_DIAGNOSTICS_DIR:-}"

# Let a crashing command leave a core dump (if the hard limit allows it).
ulimit -c unlimited 2> /dev/null

# All core dumps that are newer than this marker were written while the command
# ran (by the command itself, or by the children of the death tests).
CORE_MARKER=$(mktemp)
trap 'rm -f "$CORE_MARKER"' EXIT

"$@" &
PID=$!
sleep "$STALL_SECONDS" &
SLEEPER_PID=$!

# Append all further output of this script (and of the `gdb` that replaces it)
# also to a file in the `DIAGNOSTICS_DIR`.
redirect_output_to_diagnostics_file() {
  if [ -n "$DIAGNOSTICS_DIR" ]; then
    mkdir -p "$DIAGNOSTICS_DIR"
    exec > >(tee -a "$DIAGNOSTICS_DIR/$(basename "$1")-$PID.txt") 2>&1
  fi
}

# Print the `gdb` command line options that disable everything that is not
# needed (or might hang) in a non-interactive environment.
GDB_OPTIONS=(-q -nx -batch -iex "set debuginfod enabled off"
  -iex "set pagination off" -iex "set confirm off"
  -iex "set print thread-events off")

# Delete the core dumps that were written while the command ran.
delete_core_dumps() {
  find . -maxdepth 1 -name 'core.*' -newer "$CORE_MARKER" -delete 2> /dev/null
}

# Wait for whichever of the two finishes first.
wait -n -p FINISHED_PID "$PID" "$SLEEPER_PID"
STATUS=$?
if [ "$FINISHED_PID" = "$PID" ]; then
  kill "$SLEEPER_PID" 2> /dev/null
  wait "$SLEEPER_PID" 2> /dev/null
  # An exit status above 128 means that the command was killed by a signal.
  if [ "$STATUS" -gt 128 ]; then
    redirect_output_to_diagnostics_file "$1"
    echo ""
    echo "=== CRASH DIAGNOSTICS: \`$*\` (pid $PID) was terminated by" \
      "SIG$(kill -l "$((STATUS - 128))") at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if [ -f "core.$PID" ]; then
      gdb "${GDB_OPTIONS[@]}" -ex "print \$_siginfo" -ex "info registers" \
        -ex "x/i \$pc" -ex "info threads" -ex "thread apply all bt" \
        "$1" "core.$PID"
    else
      echo "=== No core dump \`core.$PID\` (core pattern:" \
        "\`$(cat /proc/sys/kernel/core_pattern)\`, core size limit:" \
        "$(ulimit -c), hard limit: $(ulimit -Hc))"
    fi
  fi
  delete_core_dumps
  exit "$STATUS"
fi

redirect_output_to_diagnostics_file "$1"
echo ""
echo "=== STALL DIAGNOSTICS: \`$*\` (pid $PID) is still running after" \
  "${STALL_SECONDS}s at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=== Status of pid $PID:"
grep -E '^(State|Threads|VmPeak|VmRSS|VmSwap|voluntary_ctxt_switches|nonvoluntary_ctxt_switches):' \
  "/proc/$PID/status"
echo "=== Memory:"
free -m
echo "=== All processes:"
ps -eo pid,ppid,stat,pcpu,rss,etime,args --sort=-pcpu | head -n 30

# Print one line per thread of the command with its state, the CPU time that it
# has consumed so far (user + system, in clock ticks), the time that it has
# spent on and waiting for a CPU (in ns, from `schedstat`), and the kernel
# function in which it is waiting. Also print the load, the pressure, and the
# CPU line of `/proc/stat` (whose eighth number is the steal time, which is
# the time that the hypervisor didn't give to this machine). This needs no
# `ptrace`, and two samples tell threads that spin apart from threads that are
# blocked, and from threads that don't get a CPU.
print_sample() {
  echo "=== Sample at $(date -u +%H:%M:%S.%N)"
  echo "loadavg: $(cat /proc/loadavg)"
  echo "$(head -n 1 /proc/stat) (user nice system idle iowait irq softirq steal)"
  for RESOURCE in cpu memory io; do
    if [ -r "/proc/pressure/$RESOURCE" ]; then
      echo "pressure $RESOURCE: $(tr '\n' ' ' < "/proc/pressure/$RESOURCE")"
    fi
  done
  echo "Threads of pid $PID (tid, name, state, CPU ticks, schedstat [ns on" \
    "CPU, ns waiting for a CPU, timeslices], wchan):"
  for TASK in /proc/"$PID"/task/*; do
    # The name (field 2) is in parentheses and may contain spaces, so the
    # fields are counted from the closing parenthesis.
    STAT=$(cat "$TASK/stat" 2> /dev/null) || continue
    NAME="${STAT#*(}"
    NAME="${NAME%)*}"
    read -r -a FIELDS <<< "${STAT##*) }"
    echo "$(basename "$TASK") ($NAME) ${FIELDS[0]}" \
      "$((FIELDS[11] + FIELDS[12]))" \
      "[$(cat "$TASK/schedstat" 2> /dev/null)]" \
      "$(cat "$TASK/wchan" 2> /dev/null)"
  done
}
print_sample
sleep 2
print_sample

# The `gdb` script that dumps the stacks of all threads, and then resumes the
# command and reports each fatal signal that it receives, until it terminates.
# Its exit status is the one that the command would have had without `gdb`.
GDB_SCRIPT=$(mktemp --suffix=.py)
cat > "$GDB_SCRIPT" << 'EOF'
import os

import gdb

# The script is a temporary file, see below.
try:
    os.remove(__file__)
except Exception:
    pass


def run(command):
    try:
        gdb.execute(command)
    except gdb.error as error:
        print(f"({command}: {error})")


print("=== Stacks of all threads:")
run("info threads")
run("thread apply all bt")
run("handle all nostop noprint pass")
for signal in ["SIGSEGV", "SIGBUS", "SIGILL", "SIGFPE", "SIGABRT"]:
    run(f"handle {signal} stop print pass")
print("=== Resuming the command until it terminates")
for _ in range(5):
    run("continue")
    if gdb.selected_inferior().pid == 0:
        break
    print("=== The command received a fatal signal, its state:")
    run("print $_siginfo")
    run("info registers")
    run("x/i $pc")
    run("thread apply all bt")
else:
    run("kill")


def convenienceVariable(name):
    value = gdb.convenience_variable(name)
    return None if value is None else int(value)


exitSignal = convenienceVariable("_exitsignal")
exitCode = convenienceVariable("_exitcode")
if exitSignal is not None:
    status = 128 + exitSignal
elif exitCode is not None:
    status = exitCode
else:
    status = 1
print(f"=== The command terminated with exit status {status}")
gdb.execute(f"quit {status}")
EOF

# If the command has terminated in the meantime (it is then a zombie, because
# this script hasn't waited for it yet), `gdb` could neither attach to it nor
# tell its exit status.
STATE=$(cat "/proc/$PID/stat" 2> /dev/null)
STATE="${STATE##*) }"
if [ "${STATE%% *}" = "Z" ]; then
  rm -f "$GDB_SCRIPT"
  wait "$PID"
  STATUS=$?
  echo "=== The command terminated with exit status $STATUS"
  exit "$STATUS"
fi

# NOTE: `exec` replaces this script, so the `EXIT` trap doesn't run anymore.
rm -f "$CORE_MARKER"
exec gdb "${GDB_OPTIONS[@]}" -p "$PID" -x "$GDB_SCRIPT"
