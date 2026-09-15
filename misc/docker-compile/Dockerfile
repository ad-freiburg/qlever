# A minimal image for compiling QLever with a specific version of GCC.
#
# It deliberately mirrors the setup of the `Native build` GitHub workflow
# (Ubuntu 24.04 + `ppa:ubuntu-toolchain-r/test` for the compiler +
# `ppa:mhier/libboost-latest` for Boost), such that compiler errors found
# locally are exactly the ones that CI would report.
#
# The image only contains the toolchain and the dependencies; the sources and
# the build directory are bind-mounted at runtime (see `compile-in-docker.sh`).
FROM ubuntu:24.04

ARG GCC_VERSION=15
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN apt-get update && apt-get install -y software-properties-common wget git \
    ca-certificates build-essential ninja-build cmake mold pkg-config \
    libicu-dev tzdata libzstd-dev libjemalloc-dev libssl-dev uuid-dev \
    python3-yaml python3-icu unzip

# The compiler, from the same PPA that the CI uses.
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test && apt-get update \
    && apt-get install -y gcc-${GCC_VERSION} g++-${GCC_VERSION}

# Boost, from the same PPA that the CI uses.
RUN add-apt-repository -y ppa:mhier/libboost-latest && apt-get update \
    && apt-get install -y libboost1.83-all-dev libboost-url1.83-dev \
    libboost-container1.83-dev

# `git` refuses to work in the bind-mounted repository otherwise, because it is
# owned by a different user inside the container.
RUN git config --global --add safe.directory '*'

WORKDIR /qlever
