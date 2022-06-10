FROM ubuntu:22.04 as base
LABEL maintainer="Johannes Kalmbach <kalmbacj@informatik.uni-freiburg.de>"
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV LC_CTYPE C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

FROM base as builder
RUN apt-get update && apt-get install -y build-essential cmake libicu-dev tzdata pkg-config uuid-runtime uuid-dev git
RUN apt install -y libjemalloc-dev ninja-build libzstd-dev
RUN apt install -y libboost1.74-dev libboost-program-options1.74-dev libboost-iostreams1.74-dev

COPY . /app/

WORKDIR /app/
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app/build/
RUN cmake -DCMAKE_BUILD_TYPE=Release -DLOGLEVEL=INFO -DUSE_PARALLEL=true -GNinja .. && ninja
RUN make test

FROM base as runtime
WORKDIR /app
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y wget python3-yaml unzip curl bzip2 pkg-config libicu-dev python3-icu libgomp1 uuid-runtime make
RUN apt install -y lbzip2 libjemalloc-dev libzstd-dev
RUN apt install -y libboost1.74-dev libboost-program-options1.74-dev libboost-iostreams1.74-dev

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
ENV MEMORY_FOR_QUERIES 70
ENV CACHE_MAX_SIZE_GB 30
ENV CACHE_MAX_SIZE_GB_SINGLE_ENTRY 5
ENV CACHE_MAX_NUM_ENTRIES 1000
# Need the shell to get the INDEX_PREFIX envirionment variable
ENTRYPOINT ["/bin/sh", "-c", "exec ServerMain -i \"/index/${INDEX_PREFIX}\" -j 8 -m ${MEMORY_FOR_QUERIES} -c ${CACHE_MAX_SIZE_GB} -e ${CACHE_MAX_SIZE_GB_SINGLE_ENTRY} -k ${CACHE_MAX_NUM_ENTRIES} -p 7001 \"$@\"", "--"]

# Build image:  docker build -t qlever.master .

# Build index:  DB=wikidata; docker run -it --rm -v "$(pwd)":/index --entrypoint bash --name qlever.$DB-index qlever.master -c "IndexBuilderMain -f /index/$DB.nt -i /index/$DB -s /index/$DB.settings.json | tee /index/$DB.index-log.txt"; rm -f $DB/*tmp*

# Run engine:   DB=wikidata; PORT=7001; docker rm -f qlever.$DB; docker run -d --restart=unless-stopped -v "$(pwd)":/index -p $PORT:7001 -e INDEX_PREFIX=$DB -e MEMORY_FOR_QUERIES=30 --name qlever.$DB qlever.master; docker logs -f --tail=100 qlever.$DB
