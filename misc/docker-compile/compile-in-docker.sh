#!/usr/bin/env bash
#
# Compile QLever inside a Docker container with a specific version of GCC.
#
# Usage:
#   misc/docker-compile/compile-in-docker.sh <gcc-version> [command ...]
#
# Without a command, the script configures (if necessary) and builds all
# targets. With a command, the command is run inside the container, with the
# build directory as the working directory, e.g.
#
#   misc/docker-compile/compile-in-docker.sh 16 ninja IndexTest
#   misc/docker-compile/compile-in-docker.sh 16 ctest -j $(nproc)
#   misc/docker-compile/compile-in-docker.sh 16 bash
#
# The build artifacts are kept in a named Docker volume (one per compiler
# version), so that consecutive invocations are incremental. Set
# `QLEVER_DOCKER_BUILD_VOLUME` to use a different volume or a host directory.
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <gcc-version> [command ...]" >&2
  exit 1
fi

GCC_VERSION="$1"
shift

REPO_DIR="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
IMAGE="qlever-compile-gcc${GCC_VERSION}"
BUILD_VOLUME="${QLEVER_DOCKER_BUILD_VOLUME:-qlever-build-gcc${GCC_VERSION}}"

docker build --build-arg "GCC_VERSION=${GCC_VERSION}" -t "${IMAGE}" \
  "${REPO_DIR}/misc/docker-compile"

# The configure step is a no-op if the build directory is already configured.
CONFIGURE_AND_BUILD="
  cmake -S /qlever/source -B /qlever/build -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE=/qlever/source/toolchains/gcc${GCC_VERSION}.cmake \
    -DADDITIONAL_COMPILER_FLAGS='-Wall -Wextra -Werror -Woverloaded-virtual' \
    -DADDITIONAL_LINKER_FLAGS='-fuse-ld=mold' \
    -DUSE_PARALLEL=true -DBUILD_SHARED_LIBS=ON \
    -DUSE_PRECOMPILED_HEADERS=OFF -DQLEVER_ONE_TEST_CASE_PER_FILE=ON &&
  cmake --build /qlever/build -- -j \$(nproc)"

# Allocate a terminal only if we have one (so the script also works in CI).
TTY_FLAGS=()
if [ -t 0 ]; then TTY_FLAGS=(-it); fi

docker run --rm "${TTY_FLAGS[@]}" \
  -v "${REPO_DIR}:/qlever/source" \
  -v "${BUILD_VOLUME}:/qlever/build" \
  -w /qlever/build \
  "${IMAGE}" \
  bash -c "$( [ $# -eq 0 ] && echo "${CONFIGURE_AND_BUILD}" || printf '%q ' "$@" )"
