#!/bin/bash
# This script compiles QLever using `g++-<version>`, where the version is specified as an argument to the script.
# The script assumes that all the required toolchains, in particular the compiler and all dependencies of QLever
# are already installed, so you typically run it inside a docker container.
# The script compiles in C++17 mode, with all the required macros defined, and doesn't stop on the first error,
# but continues the compilation to get as much information on the compilation errors as possible. The logs are stored
# on disk for later analysis and are also directly fed to the `gcc-8-logs-analyzer` script.

set -o pipefail

# Get GCC version from argument, default to 8
GCC_VERSION="${1:-8}"

BUILD_LOG="/qlever/build.log"

echo "=== Configuring CMake with GCC${GCC_VERSION} and C++17 settings ==="
cmake -B /qlever/build-gcc-${GCC_VERSION} \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_COMPILER=g++-${GCC_VERSION} \
    -DADDITIONAL_COMPILER_FLAGS="-w" \
    -DUSE_PARALLEL=true \
    -DRUN_EXPENSIVE_TESTS=true \
    -DENABLE_EXPENSIVE_CHECKS=true \
    -DREDUCED_FEATURE_SET_FOR_CPP17=ON \
    -DUSE_CPP_17_BACKPORTS=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -DCOMPILER_VERSION_CHECK_DEACTIVATED=ON \
    -DADDITIONAL_LINKER_FLAGS="-B /usr/bin/mold"

echo ""
echo "=== Building QLever with GCC${GCC_VERSION} (this will take a while) ==="
cmake --build /qlever/build-gcc-${GCC_VERSION} \
    --target QleverTest \
    --config Release \
    -- -i -k -j $(nproc) 2>&1 | tee "${BUILD_LOG}"

BUILD_EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "=== Build completed with exit code: ${BUILD_EXIT_CODE} ==="
echo ""
echo "=== Running GCC${GCC_VERSION} Log Analyzer ==="
python3 /qlever/misc/gcc8_logs_analyzer.py "${BUILD_LOG}"

echo ""
echo "=== Analysis Complete ==="
echo "Build log saved to: ${BUILD_LOG}"
echo ""
echo "You can re-run the analyzer with:"
echo "  python3 /qlever/misc/gcc8_logs_analyzer.py ${BUILD_LOG}"
echo ""
echo "Or examine the build log directly:"
echo "  less ${BUILD_LOG}"
