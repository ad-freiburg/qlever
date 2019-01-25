# Running QLever natively on a Linux or Unix-like (untested) host
## Requirements:
QLever is primarily tested on a Ubuntu 18.04 based container as well as current
Arch Linux.

For Ubuntu 18.04 the following packages are required

* build-essential cmake libsparsehash-dev
* wget python3-yaml unzip curl (for End-to-End Tests)

This roughly translates to

* GCC >= 7.x
* CMake >= 2.8.4
* Google's sparsehash >= 2.02
* python >= 3.6 (for End-to-End Tests with type hints)
* python-yaml >= 3.10


## Build and run unit tests

Go to a folder where you want to build the binaries.  Usually this is done
with a separate `build` subfolder. This is also assumed by the `e2e/e2e.sh`
script.

    mkdir build && cd build

Build the project (Optional: add `-DPERFTOOLS_PROFILER=True/False` and `-DALLOW_SHUTDOWN=True/False`)

    cmake -DCMAKE_BUILD_TYPE=Release .. && make -j $(nproc)

Run ctest. All tests should pass:

    ctest

## Create or reuse an index
See the main [README](README.md#create_or_reuse_an_index) but make sure to
either add `./build/` to your path or prefix all commands with `./` and that
`/index` and `/input` need to be the path to the index and input on your host.

## Start a Sever

* Without text collection:

    ./ServerMain -i /path/to/myindex -p <PORT>

* With text collection:

    ./ServerMain -i /path/to/myindex -p <PORT> -t

Depending on if you built the index with the -a version, two or six index permutations will be registered.
For some data this can be a significant difference in memory consumption.

If you built an index using the -a option, make sure to include it at startup
(otherwise only 2 of the 6 permutations will be registered).

    ./ServerMain -i /path/to/myindex -p <PORT> -t -a
