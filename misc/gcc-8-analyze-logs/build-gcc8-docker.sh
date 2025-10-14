#!/bin/bash
# Script to build the GCC8 Docker container for QLever C++17 compatibility testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IMAGE_NAME="qlever-gcc8-cpp17"
IMAGE_TAG="latest"

echo "Building Docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "This may take several minutes..."

DOCKER_BUILDKIT=0 docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" -f "${SCRIPT_DIR}/Dockerfile.gcc8" "${REPO_ROOT}"

echo ""
echo "Docker image built successfully: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "You can now run the C++17 compatibility check with: ./run-gcc8-check.sh"
