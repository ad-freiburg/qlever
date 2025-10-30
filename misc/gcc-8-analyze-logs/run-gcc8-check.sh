#!/bin/bash
# Script to run GCC8 C++17 compatibility check for QLever
# This script mimics the GitHub Actions workflow for local testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IMAGE_NAME="qlever-gcc8-cpp17"
IMAGE_TAG="latest"

# Check if Docker image exists
if ! docker image inspect "${IMAGE_NAME}:${IMAGE_TAG}" >/dev/null 2>&1; then
    echo "Error: Docker image ${IMAGE_NAME}:${IMAGE_TAG} not found."
    echo "Please build it first using: ./build-gcc8-docker.sh"
    exit 1
fi

echo "Running C++17 compatibility check with GCC8..."
echo "Source directory: ${REPO_ROOT}"
echo ""
echo "Starting interactive container..."
echo "The build script will run automatically."
echo "After completion, you can:"
echo "  - View the build log: less /qlever/build.log"
echo "  - Re-run the analyzer: python3 /qlever/misc/gcc8_logs_analyzer.py /qlever/build.log"
echo "  - Exit the container: type 'exit'"
echo ""

# Run Docker container interactively with mounted source directory
# The build-and-analyze.sh script runs the build and analysis
# After completion, drop into an interactive shell
docker run --rm -it \
    -v "${REPO_ROOT}:/qlever" \
    -w /qlever \
    "${IMAGE_NAME}:${IMAGE_TAG}" \
    bash -c '/qlever/misc/gcc-8-analyze-logs/build-and-analyze.sh; exec bash'
