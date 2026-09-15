#!/usr/bin/env bash
# Build portable binaries of `qlever-index` and `qlever-server` for macOS on
# Apple Silicon (arm64). The third-party libraries are linked statically, so
# the only dynamic dependencies are the libraries that are part of macOS
# itself (libSystem and libc++). Together with the deployment target below,
# the binaries run on any Mac with that version of macOS or newer. This is
# the macOS counterpart of `build-portable-binaries.sh`.
#
# Prerequisites: Xcode command line tools (Apple clang), cmake >= 3.27,
# conan 2.x, and jemalloc via Homebrew.
#
# Usage: build-portable-binaries-macos.sh [<build-dir>]
# (default: build-portable; a relative path is taken relative to the repo)

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
case "${1:-build-portable}" in
    /*) BUILD_DIR="$1" ;;
    *) BUILD_DIR="$REPO_DIR/${1:-build-portable}" ;;
esac
STATIC_LIBS="$BUILD_DIR/static-libs"
DIST_DIR="$BUILD_DIR/dist"
NUM_THREADS=$(sysctl -n hw.ncpu)

# The oldest macOS the binaries run on. The native build with Apple clang
# (see `.github/workflows/macos-appleclang-native.yml`) uses the same value.
DEPLOYMENT_TARGET=11.0

# jemalloc is not available via conan (see `conanfile.txt`), so it comes from
# Homebrew, which also ships a static archive. A directory that contains ONLY
# that archive is put first in the linker search path, so that the
# `-ljemalloc` of QLever's CMake resolves to the `.a` instead of the `.dylib`.
JEMALLOC_PREFIX="$(brew --prefix jemalloc)"
if [ ! -f "$JEMALLOC_PREFIX/lib/libjemalloc.a" ]; then
    echo "ERROR: static jemalloc not found at $JEMALLOC_PREFIX/lib/libjemalloc.a"
    exit 1
fi
mkdir -p "$STATIC_LIBS"
ln -sf "$JEMALLOC_PREFIX/lib/libjemalloc.a" "$STATIC_LIBS/libjemalloc.a"

# Build the third-party libraries as static libraries via conan (all used
# recipes default to static), for the deployment target (`os.version`), with
# ICU's Unicode data compiled in (see `build-portable-binaries.sh`).
conan profile detect --force
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
conan install "$REPO_DIR" -pr:b=default -pr:h=default -of=. \
    -s:h "os.version=$DEPLOYMENT_TARGET" \
    -o 'icu/*:data_packaging=static' --build=missing

# No `-march=native`, so that the binaries run on every Apple Silicon CPU. No
# OpenMP, since Apple clang has none (as in the other macOS builds).
cmake -B "$BUILD_DIR" -S "$REPO_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DLOGLEVEL=INFO \
    -DCMAKE_TOOLCHAIN_FILE="$BUILD_DIR/conan_toolchain.cmake" \
    -DCMAKE_POLICY_DEFAULT_CMP0091=NEW \
    -DCMAKE_OSX_DEPLOYMENT_TARGET="$DEPLOYMENT_TARGET" \
    -DCOMPILER_SUPPORTS_MARCH_NATIVE=FALSE \
    -DUSE_PARALLEL=false \
    -DRUN_EXPENSIVE_TESTS=false \
    -DJEMALLOC_MANUALLY_INSTALLED=True \
    -DCMAKE_EXE_LINKER_FLAGS="-L$STATIC_LIBS"

cmake --build "$BUILD_DIR" --target qlever-index qlever-server -- -j "$NUM_THREADS"

# Check that nothing but the libraries of macOS itself is linked dynamically,
# and that the binaries are built for the deployment target.
for binary in qlever-index qlever-server; do
    dependencies=$(otool -L "$BUILD_DIR/$binary" | tail -n +2)
    if grep -vE '^[[:space:]]+(/usr/lib/|/System/Library/)' <<< "$dependencies"; then
        echo "ERROR: $binary has dynamic dependencies beyond macOS (see above)"
        exit 1
    fi
    minos=$(otool -l "$BUILD_DIR/$binary" | awk '/LC_BUILD_VERSION/{f=1} f && /minos/{print $2; exit}')
    if [ "$minos" != "$DEPLOYMENT_TARGET" ]; then
        echo "ERROR: $binary is built for macOS $minos, not $DEPLOYMENT_TARGET"
        exit 1
    fi
done

# The stripped binaries are the artifacts; keep the unstripped ones around
# for debugging.
mkdir -p "$DIST_DIR"
for binary in qlever-index qlever-server; do
    cp "$BUILD_DIR/$binary" "$DIST_DIR/$binary"
    strip "$DIST_DIR/$binary"
done

"$REPO_DIR/.github/scripts/smoke-test-portable-binaries.sh" "$DIST_DIR" "$BUILD_DIR/smoke-test"

echo "Portable binaries built, checked, and smoke-tested successfully:"
ls -la "$DIST_DIR"
