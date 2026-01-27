#!/usr/bin/env bash

set -e

# This script rewrites C++20 "using enum" declarations to C++17-compatible code
# using a Docker container with the backport-using-enum tool.

DOCKER_IMAGE="ghcr.io/joka921/llvm-enum-rewriter:latest"
WORKSPACE_DIR="$(pwd)"

echo "Pulling Docker image..."
docker pull "$DOCKER_IMAGE"

echo "Running enum rewriter inside Docker container..."
docker run --rm \
  -v "$WORKSPACE_DIR:/workspace" \
  -w /workspace \
  "$DOCKER_IMAGE" \
  bash run-enum-rewriter-inside-docker.sh
