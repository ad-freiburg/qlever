FROM ubuntu:18.04 as base
LABEL maintainer="Niklas Schnelle <schnelle@informatik.uni-freiburg.de>"
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV LC_CTYPE C.UTF-8

FROM base as builder
RUN apt-get update && apt-get install -y build-essential cmake clang-format-8 libsparsehash-dev libicu-dev
COPY . /app/

# Check formatting with the .clang-format project style
WORKDIR /app/
RUN misc/format-check.sh

WORKDIR /app/build/
RUN cmake -DCMAKE_BUILD_TYPE=Release -DLOGLEVEL=DEBUG -DUSE_PARALLEL=true .. && make -j $(nproc) && make test

FROM base as runtime
WORKDIR /app
RUN apt-get update && apt-get install -y wget python3-yaml unzip curl bzip2 pkg-config libicu-dev python3-icu libgomp1

ARG UID=1000
RUN groupadd -r qlever && useradd --no-log-init -r -u $UID -g qlever qlever && chown qlever:qlever /app
USER qlever
ENV PATH=/app/:$PATH

COPY --from=builder /app/build/*Main /app/src/web/* /app/
COPY --from=builder /app/e2e/* /app/e2e/
ENV PATH=/app/:$PATH

USER qlever
EXPOSE 7001
VOLUME ["/input", "/index"]

ENV INDEX_PREFIX index
# Need the shell to get the INDEX_PREFIX envirionment variable
ENTRYPOINT ["/bin/sh", "-c", "exec ServerMain -i \"/index/${INDEX_PREFIX}\" -p 7001 \"$@\"", "--"]

# docker build -t qlever-<name> .
# # When running with user namespaces you may need to make the index folder accessible
# # to e.g. the "nobody" user
# chmod -R o+rw ./index
# # For an existing index copy it into the ./index folder and make sure to either name it
# # index.* or
# # set the environment variable "INDEX_PREFIX" during `docker run` using `-e INDEX_PREFIX=<prefix>`
# # To build an index run a bash inside the container as follows
# docker run -it --rm --entrypoint bash -v "<path_to_input>:/input" -v "$(pwd)/index:/index" qlever-<name>
# # Then inside that shell IndexBuilder is in the path and can be used like
# # described in the README.md with the files in /input
# # To run a server use
# docker run -d -p 7001:7001 -e "INDEX_PREFIX=<prefix>" -v "$(pwd)/index:/index" --name qlever-<name> qlever-<name>

