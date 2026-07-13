#!/bin/bash
# Copyright 2026, University of Freiburg,
# Chair of Algorithms and Data Structures.
# Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

# Build and run QLever's unit tests inside a CMake build directory that was
# configured for the restricted "true C++17" configuration (see
# `.github/workflows/cpp-17-libqlever.yml`, `compiler-version: 8`), and cross-
# check the set of test targets that fail to build against the documented
# exclude list at `misc/cpp17_test_exclude_list.txt`.
#
# The set of "all unit test targets" is derived directly from the
# `addLinkAndDiscoverTest*`/`addAndLinkTest` macro invocations in
# `test/**/CMakeLists.txt`, so newly added test files are picked up
# automatically. For each such target, this script checks whether an
# executable of that name was actually produced by the build; the CMake
# macro used to register a target with `ctest` differs (some use
# `gtest_discover_tests`, some register a single `ctest` entry for the whole
# binary), so checking for the executable file directly is more robust than
# relying on any one ctest-internal naming convention.
#
# Usage: run_cpp17_tests.sh <build-dir>

set -uo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <build-dir>"
  exit 1
fi

build_dir="$1"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
exclude_file="${script_dir}/cpp17_test_exclude_list.txt"

cd "${build_dir}" || exit 1

# Build everything, tolerating failures in the known-excluded targets. The
# actual pass/fail gate for this script is the `ctest` invocation below, not
# the exit code of this build.
cmake --build . -- -k -i -j "$(nproc)"

all_targets="$(grep -ohrE '^[[:space:]]*(addLinkAndDiscoverTest|addLinkAndDiscoverTestNoLibs|addLinkAndRunAsSingleTest|addLinkAndDiscoverTestSerial|addLinkAndDiscoverTestSerialNoLibs|addAndLinkTest)\([[:space:]]*[A-Za-z0-9_]+' \
  $(find "${repo_root}/test" -iname CMakeLists.txt) \
  | sed -E 's/^[[:space:]]*[A-Za-z]+\([[:space:]]*//' | sort -u)"

actual_not_built=""
while IFS= read -r target; do
  [[ -z "${target}" ]] && continue
  if ! find test -type f -name "${target}" -perm -u+x 2>/dev/null | grep -q .; then
    actual_not_built="${actual_not_built}${target}
"
  fi
done <<<"${all_targets}"
actual_not_built="$(echo -n "${actual_not_built}" | sed '/^$/d' | sort -u)"

expected_excluded="$(grep -v '^[[:space:]]*#' "${exclude_file}" | grep -v '^[[:space:]]*$' | sort -u)"

if [[ "${actual_not_built}" != "${expected_excluded}" ]]; then
  echo "::error::The set of unit test targets that fail to build under C++17 no longer matches ${exclude_file}."
  echo "--- Expected (from cpp17_test_exclude_list.txt) ---"
  echo "${expected_excluded}"
  echo "--- Actual (targets that failed to build just now) ---"
  echo "${actual_not_built}"
  echo "--- Diff (expected vs. actual) ---"
  diff <(echo "${expected_excluded}") <(echo "${actual_not_built}")
  echo "If a target now builds successfully, remove it from cpp17_test_exclude_list.txt so its tests actually run."
  echo "If a new target has regressed, investigate it and either fix it, or add it (with a comment explaining why) to cpp17_test_exclude_list.txt."
  exit 1
fi

# Every unit test target's ctest entries are labeled with its own target name
# (see the `linkAndDiscoverTest` function in `test/CMakeLists.txt`), but the
# `<Target>_NOT_BUILT` placeholder ctest entry that `gtest_discover_tests`
# emits for a target that failed to build does *not* carry that label, so
# excluding by name prefix instead of by label reliably excludes both a
# failed target's placeholder entry and (for targets not using
# `gtest_discover_tests`) its single ctest entry.
exclude_regex="^($(echo "${expected_excluded}" | paste -sd '|' -))"
env CTEST_OUTPUT_ON_FAILURE=1 ctest -j "$(nproc)" -E "${exclude_regex}"
