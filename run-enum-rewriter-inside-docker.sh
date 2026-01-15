#!/usr/bin/env bash

set -e

# This script runs INSIDE the Docker container to rewrite "using enum" declarations.
# It should be called by run-enum-rewriter-docker.sh.

BUILD_DIR="build-enum-rewriter"

echo "Installing dependencies..."
apt-get update && apt-get install -y libboost-container1.81-dev

echo "Generating compilation database using clang++..."
cmake -B "$BUILD_DIR" \
  -DCMAKE_CXX_COMPILER=/opt/llvm/bin/clang++ \
  -DCMAKE_C_COMPILER=/opt/llvm/bin/clang \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DCMAKE_BUILD_TYPE=Release \
  -DREDUCED_FEATURE_SET_FOR_CPP17=ON \
  -DUSE_CPP_17_BACKPORTS=ON \
  -DUSE_PRECOMPILED_HEADERS=OFF \
  -DCMAKE_CXX_STANDARD=17 \
  -DCOMPILER_VERSION_CHECK_DEACTIVATED=ON

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
    rm -rf "$BUILD_DIR"
    exit 0
fi

echo "Rewriting ${#SOURCE_FILES[@]} files..."
for source in "${SOURCE_FILES[@]}"; do
    echo "Rewriting: $source"
    /opt/llvm/bin/backport-using-enum -p "$BUILD_DIR" "$source"
done

echo "Finished rewriting all files"
echo "Cleaning up temporary build directory..."
rm -rf "$BUILD_DIR"
