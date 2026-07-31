#!/usr/bin/env bash
# Copyright 2026 The QLever Authors, in particular:
#
# 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
#
# UFR = University of Freiburg, Chair of Algorithms and Data Structures
#
# You may not use this file except in compliance with the Apache 2.0 License,
# which can be found in the `LICENSE` file at the root of the QLever project.

# The workspace status command of QLever's Bazel build, wired up in `.bazelrc`.
# It provides the values that `CompilationInfo.cmake` collects in the CMake
# build. Bazel runs it before every build and passes the result to
# `//bazel:compilation_info.bzl`, which turns it into `CompilationInfo.cpp`.
#
# Keys prefixed with `STABLE_` only trigger a rebuild of the actions that read
# them when their value actually changes, which is what we want for the git hash
# and the project version. The build time deliberately uses a key without that
# prefix ("volatile"), so that it does not cause a relink on every build. This is
# the equivalent of `-DDONT_UPDATE_COMPILATION_INFO=true` being the default,
# except that Bazel gets the bookkeeping right: the timestamp is refreshed
# whenever `CompilationInfo.cpp` is regenerated for another reason.

set -euo pipefail

# `git log -1 --format=%H`, as in `CompilationInfo.cmake`.
if git_hash="$(git log -1 --format=%H 2>/dev/null)" && [ -n "${git_hash}" ]; then
  echo "STABLE_QLEVER_GIT_HASH ${git_hash}"
else
  echo "STABLE_QLEVER_GIT_HASH QLever compilation not taking place in a git repository"
fi

# `git describe --tags --always`, as in `GitVersion.cmake`.
if project_version="$(git describe --tags --always 2>/dev/null)" && [ -n "${project_version}" ]; then
  echo "STABLE_QLEVER_PROJECT_VERSION ${project_version}"
else
  echo "STABLE_QLEVER_PROJECT_VERSION unknown"
fi

echo "QLEVER_DATETIME_OF_COMPILATION $(date)"
echo "QLEVER_TIME_OF_COMPILATION_UNIX $(date +%s)"
