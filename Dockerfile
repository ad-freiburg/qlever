FROM ubuntu:18.04 as base
MAINTAINER Niklas Schnelle <schnelle@informatik.uni-freiburg.de>
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV LC_CTYPE C.UTF-8

FROM base as builder
RUN apt-get update && apt-get install -y build-essential cmake libsparsehash-dev
RUN mkdir -p /app/build
COPY . /app/
WORKDIR /app/build/
RUN cmake -DCMAKE_BUILD_TYPE=Release .. && make -j 10

FROM base as runtime
RUN mkdir /app /index /input
WORKDIR /app
ENV PATH=/app/:$PATH
COPY --from=builder /app/build/*Main /app/src/web/* /app/

EXPOSE 7001
VOLUME ["/input", "/index"]

ENV INDEX_PREFIX index
# Need the shell to get the INDEX_PREFIX envirionment variable
ENTRYPOINT ["/bin/sh", "-c", "ServerMain -i \"/index/${INDEX_PREFIX}\" -p 7001 \"$@\"", "--"]
CMD ["-t", "-a", "-l"]

# docker build -t qlever-<name> .
# # When running with user namespaces you may need to make the index folder accessible
# # to e.g. the "nobody" user
# chmod -R o+rw ./index
# # For an existing index copy it into the ./index folder and make sure to either name it
# # index.X or
# # set -e INDEX_PREFIX=prefix during docker run
# # To build an index run a bash inside the container as follows
# docker run -it --rm -v "<path_to_input>:/input" -v "$(pwd)/index:/index" qlever-<name> bash
# # Then inside that shell IndexBuilder is in the path and can be used like
# # described in the README.md with the files in /input
# # To run a server use
# docker run -d -p 9001:9001 -v "$(pwd)/index:/index" --name qlever-<name> qlever-<name>

