#!/bin/bash
# Script to run GCC8 C++17 compatibility check for QLever
# This script mimics the GitHub Actions workflow for local testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IMAGE_NAME="qlever-gcc8-cpp17"
IMAGE_TAG="latest"
BUILD_LOG="/tmp/build.log"

# Check if Docker image exists
if ! docker image inspect "${IMAGE_NAME}:${IMAGE_TAG}" >/dev/null 2>&1; then
    echo "Error: Docker image ${IMAGE_NAME}:${IMAGE_TAG} not found."
    echo "Please build it first using: ./build-gcc8-docker.sh"
    exit 1
fi

echo "Running C++17 compatibility check with GCC8..."
echo "Source directory: ${REPO_ROOT}"
echo ""

# Run Docker container with mounted source directory
# The container will:
# 1. Configure CMake with GCC8 and C++17 settings
# 2. Build the project (with keep-going and ignore-errors flags)
# 3. Run the log analyzer on the build output
docker run --rm \
    -v "${REPO_ROOT}:/qlever" \
    -w /qlever \
    "${IMAGE_NAME}:${IMAGE_TAG}" \
    bash -c '
        set -o pipefail

        echo "=== Configuring CMake with GCC8 and C++17 settings ==="
        cmake -B /qlever/build \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_TOOLCHAIN_FILE=/qlever/toolchains/gcc8.cmake \
            -DADDITIONAL_COMPILER_FLAGS="" \
            -DUSE_PARALLEL=true \
            -DRUN_EXPENSIVE_TESTS=true \
            -DENABLE_EXPENSIVE_CHECKS=true \
            -DREDUCED_FEATURE_SET_FOR_CPP17=ON \
            -DUSE_CPP_17_BACKPORTS=ON \
            -DCMAKE_CXX_STANDARD=17 \
            -DCOMPILER_VERSION_CHECK_DEACTIVATED=ON \
            -DADDITIONAL_LINKER_FLAGS="-B /usr/bin/mold"

        echo ""
        echo "=== Building QLever with GCC8 (this will take a while) ==="
        cmake --build /qlever/build \
            --target QleverTest \
            --config Release \
            -- -i -k -j $(nproc) 2>&1 | tee '"${BUILD_LOG}"'

        BUILD_EXIT_CODE=${PIPESTATUS[0]}

        echo ""
        echo "=== Build completed with exit code: ${BUILD_EXIT_CODE} ==="
        echo ""
        echo "=== Running GCC8 Log Analyzer ==="
        python3 /qlever/misc/gcc8_logs_analyzer.py '"${BUILD_LOG}"'

        echo ""
        echo "=== Analysis Complete ==="
        echo "Build log saved to: '"${BUILD_LOG}"'"
    '

echo ""
echo "Local GCC8 C++17 compatibility check finished!"
echo "Build log available at: ${BUILD_LOG}"
