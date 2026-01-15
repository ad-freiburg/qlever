#!/usr/bin/env bash

set -e

# This script rewrites C++20 "using enum" declarations to C++17-compatible code
# using a Docker container with the backport-using-enum tool.

DOCKER_IMAGE="ghcr.io/joka921/llvm-enum-rewriter:latest"
WORKSPACE_DIR="$(pwd)"
BUILD_DIR="build-enum-rewriter"

echo "Pulling Docker image..."
docker pull "$DOCKER_IMAGE"

echo "Generating compilation database using clang++ from Docker..."
docker run --rm \
  -v "$WORKSPACE_DIR:/workspace" \
  -w /workspace \
  "$DOCKER_IMAGE" \
  bash -c "cmake -B $BUILD_DIR \
    -DCMAKE_CXX_COMPILER=/opt/llvm/bin/clang++ \
    -DCMAKE_C_COMPILER=/opt/llvm/bin/clang \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DCMAKE_BUILD_TYPE=Release \
    -DREDUCED_FEATURE_SET_FOR_CPP17=ON \
    -DUSE_CPP_17_BACKPORTS=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -DCOMPILER_VERSION_CHECK_DEACTIVATED=ON"

echo "Finding source files with 'using enum'..."
SOURCE_FILES=()
find "./src" \( -name "*.h" -o -name "*.cpp" \) -print0 > sourcelist
while IFS= read -r -d $'\0'; do
    if grep -q "using enum" "$REPLY"; then
        SOURCE_FILES+=("$REPLY")
        echo "Found: $REPLY"
    fi
done <sourcelist
rm sourcelist

if [ ${#SOURCE_FILES[@]} -eq 0 ]; then
    echo "No files with 'using enum' found. Nothing to rewrite."
    exit 0
fi

echo "Rewriting ${#SOURCE_FILES[@]} files..."
for source in "${SOURCE_FILES[@]}"; do
    echo "Rewriting: $source"
    docker run --rm \
      -v "$WORKSPACE_DIR:/workspace" \
      -w /workspace \
      "$DOCKER_IMAGE" \
      /opt/llvm/bin/backport-using-enum -p "$BUILD_DIR" "$source"
done

echo "Finished rewriting all files"
echo "Cleaning up temporary build directory..."
rm -rf "$BUILD_DIR"
