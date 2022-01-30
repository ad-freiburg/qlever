# Running QLever natively on a Linux or Unix-like (untested) host
## Requirements:
QLever is primarily tested on a Ubuntu 18.04 based container as well as current
Arch Linux.

For Ubuntu 18.04 the following packages are required

* build-essential cmake libicu-dev
* wget python3-yaml unzip curl (for End-to-End Tests)
* libjemalloc-dev ninja-build libzstd-dev google-perftools libunwind-dev libgoogle-perftools-dev

This roughly translates to

* GCC >= 7.x
* CMake >= 2.8.4
* python >= 3.6 (for End-to-End Tests with type hints)
* python-yaml >= 3.10



## Build and run unit tests

- Go to a folder where you want to build the binaries. Usually this is done
  with a separate `build` subfolder. This is also assumed by the `e2e/e2e.sh`
  script.

      mkdir build && cd build

- Build the project with TYPE='Release' or TYPE='Debug' (Optional: add `-DPERFTOOLS_PROFILER=True/False` and `-DALLOW_SHUTDOWN=True/False`)

      cmake -DCMAKE_BUILD_TYPE=Release .. && make -j $(nproc)
  or

      cmake -DCMAKE_BUILD_TYPE=Release -DLOGLEVEL=DEBUG -DUSE_PARALLEL=true -DPERFTOOLS_PROFILER=true -DALLOW_SHUTDOWN=true -GNinja .. && ninja



- Run ctest. All tests should pass:

      ctest
  or

      env CTEST_OUTPUT_ON_FAILURE=1 ctest -C Release

- Run e2e.sh tests. All tests should also pass:

      ./path/to/qlever-code/e2e/e2e.sh

## Create or reuse an index

<!---
See the main [README](../README.md#creating-an-index) but make sure to
either add `./build/` to your path or prefix all commands with `./` and that
`/index` and `/input` need to be the path to the index and input on your host.
-->

Index file-path example:

    /path/to/myindex/indexName.nt

Notice the lack of the file-extension '.nt' on the following commands.

    cd /path/to/qlever-code/build && chmod o+w .
    mkdir -p qlever-indices/olympics && cd qlever-indices/olympics 
    cp ../../../examples/olympics.* .
    xzcat olympics.nt.xz | ./../../IndexBuilderMain -F ttl -f - -l -i olympics -s olympics.settings.json | tee olympics.index-log.txt

All options:

    «Argument»            Required  Flag   Input

    docs-by-contexts          Y      'd'  path/to/file
    help                      N      'h'
    index-basename            Y      'i'  string of index-basename
    file-format               Y      'F'  [tsv|nt|ttl|mmap]
    knowledge-base-input-file Y      'f'  stdin ('-') or path/to/file
    kb-index-name             Y      'K'  string for UI (default: name of [file-format]-file)
    on-disk-literals          N      'l'  
    no-patterns               N      'P'  
    text-index-name           Y      'T'  string for UI (default: name of words-file)
    words-by-contexts         Y      'w'  /path/to/file
    add-text-index            N      'A'  
    keep-temporary-files      N      'k'  
    settings-file             Y      's'  path/to/file (normally: filename.settings.json)
    no-compressed-vocabulary  N      'N'  

## Start a Sever

Reminder of the Index file-path example:

    /path/to/qlever-code/build/qlever-indices/indexName/indexName.nt

* Without text collection:

      cd /path/to/qlever-code/build/
      ./ServerMain -i /path/to/indexName/indexName -p $PORT

  or

      cd /path/to/qlever-code/build/qlever-indices/indexName/
      ./../../ServerMain -i indexName -p $PORT

* With text collection:

      cd /path/to/qlever-code/build/
      ./ServerMain -i /path/to/indexName/indexName -p $PORT -t

  or

      cd /path/to/qlever-code/build/qlever-indices/indexName/
      ./../../ServerMain -i indexName -p $PORT -t

<!---

    ./ServerMain -i /path/to/indexName/indexName -j ${worker-threads} -m ${MEMORY_FOR_QUERIES} -c ${CACHE_MAX_SIZE_GB} -e ${CACHE_MAX_SIZE_GB_SINGLE_ENTRY} -k ${CACHE_MAX_NUM_ENTRIES} -p ${PORT}
-->

All options:

    «Argument»                 Required  Flag   Input

    help                           N     'h'
    index                          Y     'i'   /path/to/indexName/indexName
    worker-threads                 Y     'j'   recommended between [1, threads_hardware_concurrency] (enter '-1' for threads_hardware_concurrency)
    memory-for-queries             Y     'm'   [int]
    cache-max-size-gb              Y     'c'   [int]
    cache-max-size-gb-single-entry Y     'e'   [int]
    cache-max-num-entries          Y     'k'   [int]
    on-disk-literals               N     'l' 
    port                           Y     'p'   [0, 65535]
    no-patterns                    N     'P'
    no-pattern-trick               N     'T'
    text                           N     't'


<!---
Depending on if you built the index with the -a version, two or six index permutations will be registered.
For some data this can be a significant difference in memory consumption.

If you built an index using the -a option, make sure to include it at startup
(otherwise only 2 of the 6 permutations will be registered).
-->

