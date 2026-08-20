#!/usr/bin/env bash
# Build portable Linux binaries of `qlever-index` and `qlever-server`, whose
# only dynamic dependency is glibc (libc, libm, ld-linux, vdso). All other
# libraries — Boost, ICU (including its Unicode data), OpenSSL, zstd, zlib,
# bzip2, jemalloc, libgomp, libstdc++, libgcc — are linked statically. Such
# binaries run on any Linux distribution and architecture-matching machine
# whose glibc is at least as new as the one of the build machine (glibc is
# backward compatible), which is why the CI builds them on the oldest
# supported Ubuntu LTS.
#
# Works on x86_64 and aarch64 alike (all paths are derived from the compiler).
#
# Prerequisites: cmake >= 3.27, gcc, conan 2.x, libjemalloc-dev.
#
# Usage: build-portable-binaries.sh [<build-dir>]   (default: build-portable)
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_DIR="$REPO_DIR/${1:-build-portable}"
STATIC_LIBS="$BUILD_DIR/static-libs"
DIST_DIR="$BUILD_DIR/dist"
NUM_THREADS=$(nproc)

# The compiler can be overridden via the usual `CC`/`CXX` environment
# variables (the CI sets them, because the default gcc 11 of Ubuntu 22.04
# crashes with an internal compiler error on QLever's C++20 code).
GCC="${CC:-gcc}"

# A directory that contains ONLY static archives and is put first in the
# linker search path. This forces `-ljemalloc`/`-lgomp`/`-lstdc++` to resolve
# to the `.a` instead of the `.so`. This is needed because an explicit
# `-lstdc++` (injected by conan's package configs) bypasses
# `-static-libstdc++`, and because QLever's CMake links jemalloc by name.
mkdir -p "$STATIC_LIBS"
MULTIARCH_LIBDIR="/usr/lib/$($GCC -print-multiarch)"
ln -sf "$MULTIARCH_LIBDIR/libjemalloc.a" "$STATIC_LIBS/libjemalloc.a"
ln -sf "$($GCC -print-file-name=libgomp.a)" "$STATIC_LIBS/libgomp.a"
ln -sf "$($GCC -print-file-name=libstdc++.a)" "$STATIC_LIBS/libstdc++.a"

# Build the third-party libraries as static libraries via conan (using the
# repo's `conanfile.txt`; all used recipes default to static). The option
# `data_packaging=static` compiles ICU's ~30 MB Unicode data into
# `libicudata.a` — without it, that file is a stub and the binaries fail at
# runtime with `U_FILE_ACCESS_ERROR`.
# Always re-detect, so that the profile picks up the compiler from `CC`.
conan profile detect --force
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
conan install "$REPO_DIR" -pr:b=default -of=. \
    -o 'icu/*:data_packaging=static' --build=missing

cmake -B "$BUILD_DIR" -S "$REPO_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE="$BUILD_DIR/conan_toolchain.cmake" \
    -DCMAKE_POLICY_DEFAULT_CMP0091=NEW \
    -DUSE_PARALLEL=true \
    -DRUN_EXPENSIVE_TESTS=false \
    -DJEMALLOC_MANUALLY_INSTALLED=True \
    -DCMAKE_EXE_LINKER_FLAGS="-static-libstdc++ -static-libgcc -L$STATIC_LIBS"

cmake --build "$BUILD_DIR" --target qlever-index qlever-server -- -j "$NUM_THREADS"

# Check that nothing but glibc is linked dynamically (libpthread, libdl,
# librt, and libresolv are part of glibc and may appear as separate
# libraries, depending on the glibc version).
for binary in qlever-index qlever-server; do
    if ldd "$BUILD_DIR/$binary" \
            | grep -vE 'linux-vdso|ld-linux|lib(c|m|pthread|dl|rt|resolv)\.so'; then
        echo "ERROR: $binary has dynamic dependencies beyond glibc (see above)"
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

# Smoke test: build a tiny index and answer a query whose result order
# checks that the statically linked ICU collation data works (`Apfel` must
# sort before `äpfel` before `zebra`; byte-wise ordering would sort `äpfel`
# last).
SMOKE_DIR="$BUILD_DIR/smoke-test"
SMOKE_PORT="${SMOKE_PORT:-7777}"
rm -rf "$SMOKE_DIR" && mkdir -p "$SMOKE_DIR" && cd "$SMOKE_DIR"
printf '<a> <p> "\xc3\xa4pfel" .\n<b> <p> "zebra" .\n<c> <p> "Apfel" .\n' > smoke.nt
"$DIST_DIR/qlever-index" -i smoke -f smoke.nt -F nt
"$DIST_DIR/qlever-server" -i smoke -p "$SMOKE_PORT" &
SERVER_PID=$!
trap 'kill $SERVER_PID 2> /dev/null || true' EXIT
RESULT=$(curl -sf --retry 30 --retry-connrefused --retry-delay 1 \
    "http://localhost:$SMOKE_PORT/" \
    --data-urlencode 'query=SELECT ?s ?o WHERE { ?s <p> ?o } ORDER BY ?o' \
    -H 'Accept: text/csv' | tr -d '\r')
EXPECTED='s,o
c,Apfel
a,äpfel
b,zebra'
if [ "$RESULT" != "$EXPECTED" ]; then
    echo "ERROR: smoke test failed, got result:"
    echo "$RESULT"
    exit 1
fi

echo "Portable binaries built, checked, and smoke-tested successfully:"
ls -la "$DIST_DIR"
