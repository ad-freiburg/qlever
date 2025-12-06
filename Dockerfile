FROM ubuntu:24.04 AS base
LABEL maintainer="Hannah Bast <bast@cs.uni-freiburg.de>"

# Packages needed for both both building and running the binaries.
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV LC_CTYPE=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Install the packages needed for building the binaries (this is a separate
# stage to keep the final image small).
FROM base AS builder
ARG TARGETPLATFORM
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y wget
RUN wget https://apt.kitware.com/kitware-archive.sh && chmod +x kitware-archive.sh && ./kitware-archive.sh
RUN apt-get update && apt-get install -y build-essential cmake libicu-dev tzdata pkg-config uuid-runtime uuid-dev git libjemalloc-dev ninja-build libzstd-dev libssl-dev libboost1.83-dev libboost-program-options1.83-dev libboost-iostreams1.83-dev libboost-url1.83-dev libboost-container1.83-dev

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

# Build and compile. By default, also compile and run all tests. In order not
# to, build the image with `--build-arg RUN_TESTS=false`.
ARG RUN_TESTS=true
WORKDIR /qlever/build/
RUN cmake -DCMAKE_BUILD_TYPE=Release -DLOGLEVEL=INFO -DUSE_PARALLEL=true -D_NO_TIMING_TESTS=ON -GNinja ..
RUN if [ "$RUN_TESTS" = "true" ]; then \
      cmake --build . && ctest --rerun-failed --output-on-failure; \
    else \
      cmake --build . --target IndexBuilderMain ServerMain && echo "Skipping tests"; \
    fi

# Install the packages needed for the final image.
FROM base AS runtime
WORKDIR /qlever
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y wget python3-yaml unzip curl bzip2 pkg-config libicu74 python3-icu libgomp1 uuid-runtime make lbzip2 libjemalloc2 libzstd1 libboost-program-options1.83.0 libboost-iostreams1.83.0 libboost-url1.83.0 pipx bash-completion vim sudo && rm -rf /var/lib/apt/lists/*

# Set up user `qlever` with temporary sudo rights (which will be removed again
# by the `docker-entrypoint.sh` script, see there).
RUN groupadd -r qlever && useradd --no-log-init -d /qlever -r -g qlever qlever
RUN chown qlever:qlever /qlever
RUN echo "qlever ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Set up a profile script that is sourced whenever a new login shell is
# started (by any user, hence in `/etc/profile.d/`). For some reason, podman
# wants it in `/qlever/.bashrc`, so we also copy it there.
ENV QLEVER_PROFILE=/etc/profile.d/qlever.sh
RUN echo "eval \"\$(register-python-argcomplete qlever)\"" >> $QLEVER_PROFILE
RUN echo "export QLEVER_ARGCOMPLETE_ENABLED=1 && export QLEVER_IS_RUNNING_IN_CONTAINER=1" >> $QLEVER_PROFILE
RUN echo "PATH=/qlever:/qlever/.local/bin:$PATH && PS1=\"\u@docker:\W\$ \"" >> $QLEVER_PROFILE
RUN echo 'alias ll="ls -l"' >> $QLEVER_PROFILE
RUN echo "if [ -d /data ]; then cd /data; fi" >> $QLEVER_PROFILE
RUN cp $QLEVER_PROFILE /qlever/.bashrc

# Install the `qlever` command line tool. We have to set the two environment
# variables again here because in batch mode, the profile script above is not
# sourced. The `PATH` is set again to avoid a warning from `pipx`.
USER qlever
ENV PATH=/qlever:/qlever/.local/bin:$PATH
RUN pipx install qlever
ENV QLEVER_ARGCOMPLETE_ENABLED=1
ENV QLEVER_IS_RUNNING_IN_CONTAINER=1

# Copy the binaries and the entrypoint script.
COPY --from=builder /qlever/build/*Main /qlever/
COPY --from=builder /qlever/e2e/* /qlever/e2e/
COPY docker-entrypoint.sh /qlever/
RUN sudo chmod +x /qlever/docker-entrypoint.sh

# Our entrypoint script does some clever things; see the comments in there.
ENTRYPOINT ["/qlever/docker-entrypoint.sh"]
