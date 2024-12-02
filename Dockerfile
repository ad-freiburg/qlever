FROM ubuntu:24.04 AS base
LABEL maintainer="Hannah Bast <bast@cs.uni-freiburg.de>"

# Packages needed for both both building and running the binaries.
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV LC_CTYPE=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y software-properties-common wget && add-apt-repository -y ppa:mhier/libboost-latest
RUN wget https://apt.kitware.com/kitware-archive.sh && chmod +x kitware-archive.sh &&./kitware-archive.sh

# Stage 1: Build the binaries.
FROM base AS builder
ARG TARGETPLATFORM
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y build-essential cmake libicu-dev tzdata pkg-config uuid-runtime uuid-dev git libjemalloc-dev ninja-build libzstd-dev libssl-dev libboost1.83-dev libboost-program-options1.83-dev libboost-iostreams1.83-dev libboost-url1.83-dev

# Copy everything we need to build the binaries.
#
# NOTE: We are deliberately explicit here, for two reasons. First, so that we
# don't copy more than necessary without having to rely on `.dockerignore`.
# Second, so that we can copy `docker-entrypoint.sh` separately below (we don't
# to rebuild the whole container when making a small change in there).
COPY src /qlever/src/
COPY test /qlever/test/
COPY e2e /qlever/e2e/
COPY benchmark /qlever/benchmark/
COPY .git /qlever/.git/
COPY CMakeLists.txt /qlever/
COPY CompilationInfo.cmake /qlever/

# Don't build and run tests on ARM64, as it takes too long on GitHub actions.
# TODO: re-enable these tests as soon as we can use a native ARM64 platform to compile the docker container.
WORKDIR /qlever/build/
RUN cmake -DCMAKE_BUILD_TYPE=Release -DLOGLEVEL=INFO -DUSE_PARALLEL=true -D_NO_TIMING_TESTS=ON -GNinja ..
RUN if  [ $TARGETPLATFORM = "linux/arm64" ] ; then echo "target is ARM64, don't build tests to avoid timeout"; fi
RUN if [ $TARGETPLATFORM = "linux/arm64" ] ; then cmake --build . --target IndexBuilderMain ServerMain; else cmake --build . ; fi
RUN if [ $TARGETPLATFORM = "linux/arm64" ] ; then echo "Skipping tests for ARM64" ; else ctest --rerun-failed --output-on-failure ; fi

# Stage 2: Create the final image.
FROM base AS runtime
WORKDIR /qlever
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y wget python3-yaml unzip curl bzip2 pkg-config libicu-dev python3-icu libgomp1 uuid-runtime make lbzip2 libjemalloc-dev libzstd-dev libssl-dev libboost1.83-dev libboost-program-options1.83-dev libboost-iostreams1.83-dev libboost-url1.83-dev pipx bash-completion vim sudo && rm -rf /var/lib/apt/lists/*

# Set up user `qlever` with temporary sudo rights (which will be removed again
# by the `docker-entrypoint.sh` script, see the comments in that file).
RUN groupadd -r qlever && useradd --no-log-init -d /qlever -r -g qlever qlever
RUN chown qlever:qlever /qlever
RUN echo "qlever ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
USER qlever
RUN pipx install qlever
ENV PATH=/qlever:/qlever/.local/bin:$PATH
RUN echo "eval \"\$(register-python-argcomplete qlever)\"" >> /qlever/.bashrc
RUN echo "export QLEVER_ARGCOMPLETE_ENABLED=1 && export QLEVER_IS_RUNNING_IN_CONTAINER=1" >> /qlever/.bashrc
RUN echo "PATH=$PATH && PS1=\"\u@docker:\W\$ \"" >> /qlever/.bashrc
RUN echo 'alias ll="ls -l"' >> /qlever/.bashrc
RUN echo "cd /data" >> /qlever/.bashrc
RUN echo "source /qlever/.bashrc" >> /qlever/.bash_profile

# Copy the binaries and the entrypoint script.
COPY --from=builder /qlever/build/*Main /qlever/
COPY --from=builder /qlever/e2e/* /qlever/e2e/
COPY docker-entrypoint.sh /qlever/
RUN sudo chmod +x /qlever/docker-entrypoint.sh

# TODO: Are these necessary or useful for anything?
EXPOSE 7001
VOLUME ["/data"]

# Our entrypoint script does some clever things; see the comments in there.
ENTRYPOINT ["/qlever/docker-entrypoint.sh"]
