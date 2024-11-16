FROM ubuntu:24.04 AS base
LABEL maintainer="Johannes Kalmbach <kalmbacj@informatik.uni-freiburg.de>"
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV LC_CTYPE=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y software-properties-common wget && add-apt-repository -y ppa:mhier/libboost-latest
RUN wget https://apt.kitware.com/kitware-archive.sh && chmod +x kitware-archive.sh &&./kitware-archive.sh

FROM base AS builder
ARG TARGETPLATFORM
RUN apt-get update && apt-get install -y build-essential cmake libicu-dev tzdata pkg-config uuid-runtime uuid-dev git libjemalloc-dev ninja-build libzstd-dev libssl-dev libboost1.83-dev libboost-program-options1.83-dev libboost-iostreams1.83-dev libboost-url1.83-dev

COPY . /qlever/

WORKDIR /qlever/
ENV DEBIAN_FRONTEND=noninteractive

# Don't build and run tests on ARM64, as it takes too long on GitHub actions.
# TODO: re-enable these tests as soon as we can use a native ARM64 platform to compile the docker container.
WORKDIR /qlever/build/
RUN cmake -DCMAKE_BUILD_TYPE=Release -DLOGLEVEL=INFO -DUSE_PARALLEL=true -D_NO_TIMING_TESTS=ON -GNinja ..
RUN if  [ $TARGETPLATFORM = "linux/arm64" ] ; then echo "target is ARM64, don't build tests to avoid timeout"; fi
RUN if [ $TARGETPLATFORM = "linux/arm64" ] ; then cmake --build . --target IndexBuilderMain ServerMain; else cmake --build . ; fi
RUN if [ $TARGETPLATFORM = "linux/arm64" ] ; then echo "Skipping tests for ARM64" ; else ctest --rerun-failed --output-on-failure ; fi

FROM base AS runtime
WORKDIR /qlever
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y wget python3-yaml unzip curl bzip2 pkg-config libicu-dev python3-icu libgomp1 uuid-runtime make lbzip2 libjemalloc-dev libzstd-dev libssl-dev libboost1.83-dev libboost-program-options1.83-dev libboost-iostreams1.83-dev libboost-url1.83-dev pipx bash-completion

ARG UID=2000
RUN groupadd -r qlever && useradd --no-log-init -d /qlever -r -u $UID -g qlever qlever && chown qlever:qlever /qlever
USER qlever
RUN PIPX_HOME=/qlever/.local PIPX_BIN_DIR=/qlever/.local/bin PIPX_MAN_DIR=/qlever/.local/share pipx install qlever
RUN echo "eval \"\$(register-python-argcomplete qlever)\"" >> /qlever/.bashrc
ENV QLEVER_ARGCOMPLETE_ENABLED=1

COPY --from=builder /qlever/build/*Main /qlever/
COPY --from=builder /qlever/e2e/* /qlever/e2e/
ENV PATH=/qlever/:/qlever/.local/bin:$PATH

USER qlever
EXPOSE 7001
VOLUME ["/data"]

ENTRYPOINT ["bash"]

# Build image:  docker build -t qlever .

# Run container, interactive session:  docker run -it --rm -v "$(pwd)":/data --name qlever qlever

# Run container, create SPARQL endpoint for "Olympics" dataset: docker run -it --rm -v "$(pwd)":/data -p 7001:7001 --name qlever qlever -c "qlever setup-config olympics && qlever get-data && qlever index --system native && qlever start --system native --port 7001 && qlever example-queries --port 7001"
