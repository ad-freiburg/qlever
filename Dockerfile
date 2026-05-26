# Ubuntu 26.04 LTS (Resolute Raccoon) — same base the upstream adfreiburg CI uses.
# Ubuntu 26.04 ships with Python 3.13+, common C++ stubs, and more pre-installed
# packages, so the apt-get install layer in the runtime stage is smaller and
# aligns better with upstream layer caching.
FROM ubuntu:26.04 AS base
LABEL maintainer="Hannah Bast <bast@cs.uni-freiburg.de>"

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV LC_CTYPE=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# ╔═════════════════════════════════════════════════════════════════════╗
# ║  STAGE 1 — BUILD                                                    ║
# ║  Compile QLever with Clang 22 (LLVM 22) and optionally run tests.   ║
# ║  This stage is discarded; only the compiled binaries are kept.      ║
# ╚═════════════════════════════════════════════════════════════════════╝
FROM base AS builder
ENV DEBIAN_FRONTEND=noninteractive

# Core build tools, C/C++ library headers, and Clang 22.
# Notes:
#   build-essential  — even with Clang, GCC's libc6-dev/libstdc++ headers
#                      and the system linker are needed at compile time.
#   cmake / ninja    — Ubuntu 26.04 ships CMake 3.31+ natively (no PPA needed).
#   clang-22 / lld-22 — Ubuntu 26.04 ships LLVM 22 natively; no external
#                        apt repo is required (unlike older Ubuntu releases).
#   git              — CMake FetchContent clones googletest, abseil, ctre, re2,
#                      fsst, ANTLR, s2geometry, spatialjoin at configure time.
#   libomp-dev       — OpenMP support for the parallel sort used in index building.
RUN apt-get update && apt-get install -y \
    build-essential cmake ninja-build git \
    clang-22 lld-22 \
    libicu-dev pkg-config \
    uuid-runtime uuid-dev \
    libjemalloc-dev libzstd-dev libssl-dev \
    libboost-dev \
    libboost-program-options-dev \
    libboost-iostreams-dev \
    libboost-url-dev \
    libboost-container-dev \
    libomp-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source. Deliberately explicit: avoids .dockerignore surprises and lets
# docker-entrypoint.sh be updated without invalidating the full build cache.
COPY src /qlever/src/
COPY test /qlever/test/
COPY e2e /qlever/e2e/
COPY benchmark /qlever/benchmark/
COPY .git /qlever/.git/
COPY CMakeLists.txt /qlever/
COPY CompilationInfo.cmake /qlever/
COPY GitVersion.cmake /qlever/

# Configure and build.
#
# Build arguments:
#   RUN_TESTS (default: true)
#     Pass --build-arg RUN_TESTS=false to skip ctest and build only the
#     production binaries (qlever-server, qlever-index, VocabularyMergerMain,
#     PrintIndexVersionMain) without compiling or running the test suite.
#
#   MARCH (default: auto)
#     Controls the CPU microarchitecture target. Leave empty (the default) for
#     automatic selection per architecture:
#       amd64  → x86-64-v3  (AVX2 + FMA + BMI2, Intel Haswell 2013+, AMD Zen1 2017+)
#       arm64  → armv8.2-a+dotprod+crypto  (Graviton2, Neoverse N1, Ampere Altra 2018+)
#       other  → no -march flag (compiler default - baseline, portable but suboptimal)
#
#     Common overrides:
#       x86-64      (baseline)      SSE2 — fully portable across all x86-64
#       x86-64-v2                   SSE4.2 / POPCNT — older x86-64 hardware
#       x86-64-v4                   AVX-512 (Intel Ice Lake 2019+, AMD Zen4 2022+)
#       armv8-a     (baseline)      NEON 128-bit (pre-2018 hardware, e.g. Graviton1)
#       armv8.4-a                   SVE 256-bit — Graviton3, Neoverse V1 (2021+)
#       armv9-a                     SVE2 — Graviton4, Neoverse V2 (2023+)
#       native                      Build machine CPU (non-portable, local builds only)
#     Example: docker build --build-arg MARCH=x86-64-v4 .
#
# -DCOMPILER_SUPPORTS_MARCH_NATIVE=FALSE prevents fsst from auto-detecting the
# build machine's CPU independently of our MARCH setting.
#
# Performance flags:
#   -fuse-ld=lld      LLVM lld linker — 2-3x faster than GNU ld. Always on.
#   -flto=thin        ThinLTO — cross-module inlining at link time between QLever
#                     and its FetchContent dependencies (abseil, re2, s2geometry).
#                     Enabled by default: Disable with `--build-arg USE_LTO=false` 
#                     on machines with limited RAM (≤12 GB).
#
# MARCH defaults to empty: the RUN step below auto-selects x86-64-v3 on amd64
# and leaves the flag unset on all other architectures (e.g. arm64).
# USE_LTO=auto   default: LTO is enabled only when RUN_TESTS=false (i.e. the
#                CI production build), and disabled when RUN_TESTS=true.
#                Rationale: ThinLTO (-flto=thin) triggers an LLD vtable-ownership
#                bug when CMake repeats the same archive in a test binary link
#                command, causing linker failures. The main server binaries do not
#                have this issue and benefit from ThinLTO's cross-module inlining.
#                Override with --build-arg USE_LTO=true or --build-arg USE_LTO=false
#                to force regardless of RUN_TESTS. Requires ≥16 GB for LTO builds.
#
# SHOW_BUILD_STATS=false  default: no extra output.
# SHOW_BUILD_STATS=true   adds -fproc-stat-report, which prints peak RSS to
#                          stderr after every compilation step. Use this once
#                          to identify which TUs consume the most memory, then
#                          disable it for normal builds.
#   Example: docker build --build-arg USE_LTO=true --build-arg SHOW_BUILD_STATS=true .
ARG RUN_TESTS=true
ARG MARCH=
ARG USE_LTO=auto
ARG SHOW_BUILD_STATS=false
WORKDIR /qlever/build/
RUN set -e && \
    ARCH=$(uname -m) && \
    if [ -n "${MARCH}" ]; then \
        MARCH_FLAG="-march=${MARCH}"; \
    elif [ "${ARCH}" = "x86_64" ]; then \
        MARCH_FLAG="-march=x86-64-v3"; \
    elif [ "${ARCH}" = "aarch64" ]; then \
        MARCH_FLAG="-march=armv8.2-a+dotprod+crypto"; \
    else \
        MARCH_FLAG=""; \
    fi && \
    EFFECTIVE_LTO="${USE_LTO}" && \
    if [ "${EFFECTIVE_LTO}" = "auto" ]; then \
        if [ "${RUN_TESTS}" = "false" ]; then EFFECTIVE_LTO="true"; \
        else EFFECTIVE_LTO="false"; fi; \
    fi && \
    if [ "${EFFECTIVE_LTO}" = "true" ]; then \
        LTO_CFLAGS="-flto=thin"; \
        LTO_LFLAGS="-flto=thin -Wl,--thinlto-jobs=3"; \
    else \
        LTO_CFLAGS=""; \
        LTO_LFLAGS=""; \
    fi && \
    if [ "${SHOW_BUILD_STATS}" = "true" ]; then \
        STATS_FLAG="-fproc-stat-report"; \
    else \
        STATS_FLAG=""; \
    fi && \
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DLOGLEVEL=INFO \
        -DUSE_PARALLEL=true \
        -D_NO_TIMING_TESTS=ON \
        -DCOMPILER_SUPPORTS_MARCH_NATIVE=FALSE \
        -DCMAKE_C_COMPILER=clang-22 \
        -DCMAKE_CXX_COMPILER=clang++-22 \
        "-DADDITIONAL_COMPILER_FLAGS=-Wno-deprecated-declarations ${MARCH_FLAG} ${LTO_CFLAGS} ${STATS_FLAG}" \
        "-DADDITIONAL_LINKER_FLAGS=-fuse-ld=lld ${LTO_LFLAGS}" \
        -GNinja ..
RUN if [ "$RUN_TESTS" = "true" ]; then \
      cmake --build . && ctest --rerun-failed --output-on-failure; \
    else \
      cmake --build . --target qlever-index qlever-server VocabularyMergerMain PrintIndexVersionMain && echo "Skipping tests"; \
    fi

# Strip debug sections from the compiled binaries. CMake Release mode does not
# emit -g, but FetchContent dependencies can still contain debug info.
# Smaller binaries → faster cold-start I/O and better instruction-cache density.
RUN strip --strip-debug \
    /qlever/build/qlever-server \
    /qlever/build/qlever-index \
    /qlever/build/VocabularyMergerMain \
    /qlever/build/PrintIndexVersionMain

# ╔═════════════════════════════════════════════════════════════════════╗
# ║  STAGE 2 — RUNTIME                                                  ║
# ║  Deployment image built on ubuntu:26.04 (same base as upstream CI). ║
# ║  Contains only the compiled binaries and their runtime deps.        ║
# ╚═════════════════════════════════════════════════════════════════════╝
FROM base AS runtime
WORKDIR /qlever
ENV DEBIAN_FRONTEND=noninteractive
# jemalloc tuning: enable background threads so dirty/muzzy pages are
# returned to the OS between queries instead of accumulating indefinitely.
# This keeps RSS low for long-running server instances without adding latency
# to individual allocations. See https://jemalloc.net/jemalloc.3.html
ENV MALLOC_CONF=background_thread:true,metadata_thp:auto

# Shared libraries linked by qlever-server and qlever-index:
#   libicu78                    Unicode/ICU (text processing, collation)
#   libssl3                     OpenSSL — TLS for the built-in HTTP server
#   libjemalloc2                jemalloc allocator (reduces heap fragmentation)
#   libzstd1                    Zstandard compression (index file storage)
#   libgomp1                    OpenMP runtime (parallel index building)
#   libboost-program-options    command-line argument parsing
#   libboost-iostreams          stream compression / decompression
#   libboost-url                URL parsing in SPARQL endpoint requests
#   libboost-container          Boost container data structures
#
# QLever Python CLI and its dependencies:
#   python3-yaml, python3-icu   required by the qlever CLI tool
#   pipx                        installs the qlever CLI into an isolated venv
#
# Index build and data management utilities:
#   uuid-runtime                uuidgen for blank-node ID generation
#   make                        runs the Qleverfile (dataset Makefile)
#   lbzip2, bzip2               parallel + standard bzip2 for index archives
#   curl, unzip                 downloading and unpacking dataset files
#   pkg-config                  used by some qlever CLI build helpers
#
# Container UX:
#   bash-completion, vim-tiny, sudo  interactive shell convenience inside the container
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    libicu78 \
    libssl3 \
    libjemalloc2 \
    libzstd1 \
    libgomp1 \
    libboost-program-options1.90.0 \
    libboost-iostreams1.90.0 \
    libboost-url1.90.0 \
    libboost-container1.90.0 \
    python3-yaml python3-icu pipx \
    uuid-runtime make lbzip2 bzip2 \
    curl unzip pkg-config \
    bash-completion vim-tiny sudo \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Set up user `qlever` with temporary sudo rights (which will be removed again
# by the `docker-entrypoint.sh` script, see there).
RUN groupadd -r qlever && useradd --no-log-init -d /qlever -r -g qlever qlever
RUN chown qlever:qlever /qlever
RUN echo "qlever ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Profile script sourced by every login shell (any user, hence /etc/profile.d/).
# Copied to /qlever/.bashrc as well, because podman reads it from there.
ENV QLEVER_PROFILE=/etc/profile.d/qlever.sh
RUN echo "eval \"\$(register-python-argcomplete qlever)\"" >> $QLEVER_PROFILE
RUN echo "export QLEVER_ARGCOMPLETE_ENABLED=1 && export QLEVER_IS_RUNNING_IN_CONTAINER=1" >> $QLEVER_PROFILE
RUN echo "PATH=/qlever:/qlever/.local/bin:$PATH && PS1=\"\u@docker:\W\$ \"" >> $QLEVER_PROFILE
RUN echo 'alias ll="ls -l"' >> $QLEVER_PROFILE
RUN echo "if [ -d /data ]; then cd /data; fi" >> $QLEVER_PROFILE
RUN cp $QLEVER_PROFILE /qlever/.bashrc

# Install the `qlever` command line tool.
# The two ENV vars are set again here because batch-mode RUN steps do not
# source the profile script above. PATH is repeated to suppress a pipx warning.
USER qlever
ENV PATH=/qlever:/qlever/.local/bin:$PATH
RUN pipx install qlever && \
    find /qlever/.local -name '*.pyc' -delete 2>/dev/null || true && \
    find /qlever/.local -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true && \
    rm -rf /qlever/.local/pipx/.cache 2>/dev/null || true
ENV QLEVER_ARGCOMPLETE_ENABLED=1
ENV QLEVER_IS_RUNNING_IN_CONTAINER=1

# Copy compiled binaries and the entrypoint from the build stage.
COPY --from=builder /qlever/build/qlever-* /qlever/
COPY --from=builder /qlever/build/*Main /qlever/
COPY --from=builder /qlever/e2e/* /qlever/e2e/
COPY --chmod=755 docker-entrypoint.sh /qlever/

ENTRYPOINT ["/qlever/docker-entrypoint.sh"]
