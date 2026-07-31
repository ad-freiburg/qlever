# Building QLever with Bazel

CMake is QLever's primary build system; the Bazel configuration described here is
an alternative that produces the same libraries, executables and unit tests. The
two are kept structurally parallel: every `CMakeLists.txt` has a `BUILD.bazel`
next to it that declares the same libraries with the same names, sources and
dependencies. When you add a source file or a library, add it in both places.

## Quickstart

```bash
# Everything: libraries, executables and unit tests.
bazel build //...

# The two main executables, under the same names as in the CMake build.
bazel build //:qlever-server //:qlever-index

# All unit tests, or a single one.
bazel test //...
bazel test //test:QueryPlannerTest
bazel test //test:QueryPlannerTest --test_filter='*SubqueryColumnStripping*'
```

Bazel 9 or newer is required; the exact version is pinned in `.bazelversion`.

## Prerequisites

Most dependencies are fetched automatically, either from the Bazel Central
Registry (see the `bazel_dep` calls in `MODULE.bazel`) or as an `http_archive`
(see `bazel/third_party/extensions.bzl`). The following are taken from the system
via `pkg-config`, mirroring the `find_package` and `pkg_check_modules` calls in
`CMakeLists.txt`:

| Library    | Required | Ubuntu package     |
| ---------- | -------- | ------------------ |
| ICU        | unless `--config=no-unicode` | `libicu-dev`   |
| OpenSSL    | yes      | `libssl-dev`       |
| zstd       | yes      | `libzstd-dev`      |
| jemalloc   | no       | `libjemalloc-dev`  |
| liburing   | no       | `liburing-dev`     |

The two optional ones degrade to an empty library when `pkg-config` cannot find
them, in which case `QLEVER_HAS_IO_URING` also stays undefined. See
`bazel/system_library.bzl`.

## Build settings

Every CMake `option()` that changes the compiled code has a counterpart in
`//bazel:BUILD.bazel`, and `.bazelrc` defines shorthands for the combinations
that are used often.

| CMake                                      | Bazel                                          |
| ------------------------------------------ | ---------------------------------------------- |
| `-DCMAKE_BUILD_TYPE=Debug`                 | `--config=debug` (the default)                 |
| `-DCMAKE_BUILD_TYPE=Release`               | `--config=release`                             |
| `-DCMAKE_BUILD_TYPE=RelWithDebInfo`        | `--config=relwithdebinfo`                      |
| `-DCMAKE_BUILD_TYPE=ASAN`                  | `--config=asan`, `--config=tsan`               |
| `-DBUILD_SHARED_LIBS=ON`                   | `--config=shared`                              |
| `-DCHEAPER_COMPILATION=ON`                 | `--config=cheap`                               |
| `-DUSE_CPP_17_BACKPORTS=ON`                | `--config=cpp17`                               |
| `-DREDUCED_FEATURE_SET_FOR_CPP17=ON`       | `--config=reduced`                             |
| `-DQLEVER_NO_UNICODE=ON`                   | `--config=no-unicode`                          |
| `-DRUN_EXPENSIVE_TESTS=true`               | `--config=expensive-tests`                     |
| `-DENABLE_EXPENSIVE_CHECKS=true`           | `--config=expensive-checks`                    |
| `-DLOGLEVEL=INFO`                          | `--//bazel:loglevel=INFO`                      |
| `-DQUERY_CANCELLATION_MODE=DISABLED`       | `--//bazel:query_cancellation_mode=DISABLED`   |
| `-DUSE_PARALLEL=true`                      | `--//bazel:use_parallel`                       |
| `-DVOCAB_UNCOMPRESSED_IN_MEMORY=ON`        | `--//bazel:vocab_uncompressed_in_memory`       |
| `-DEXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17=ON` | `--//bazel:expression_generator_backports_for_cpp17` |
| `-D_NO_TIMING_TESTS=ON`                    | `--//bazel:no_timing_tests`                    |
| `-DUSE_TREE_BASED_CACHE=ON`                | `--//bazel:use_tree_based_cache`               |
| `-DALLOW_SHUTDOWN=ON`                      | `--//bazel:allow_shutdown`                     |
| `-DCMAKE_CXX_COMPILER=clang++-18`          | `CC=clang-18 CXX=clang++-18 bazel build ...`   |
| `-DADDITIONAL_COMPILER_FLAGS=...`          | `--copt=...`                                   |
| `-DADDITIONAL_LINKER_FLAGS=...`            | `--linkopt=...`                                |

Machine-specific settings belong into `user.bazelrc`, which `.bazelrc` imports at
the end and which is not checked in. A local output base on a fast disk is a
common example:

```
startup --output_user_root=/local/scratch/bazel
```

## How the CMake concepts are mapped

**`include_directories(src)` becomes one aggregate header target.** Taken as a
whole, the `#include` graph of `src/**/*.h` is one big cycle: for example
`util/JoinAlgorithms/IndexNestedLoopJoin.h` includes `engine/idTable/IdTable.h`,
`global/IdTriple.h` includes `index/CompressedRelation.h`, and `rdfTypes/Iri.h`
includes `parser/NormalizedString.h`. CMake does not care, because only the
*link* graph has to be acyclic there, and it is. Bazel forbids dependency cycles
outright, so each directory declares a dependency-free `headers` target with its
own headers, and `//src:qlever_hdrs` aggregates all of them. `qlever_cc_library`
and friends add that aggregate to every target, which reproduces the effect of a
global include path. See the long comment in `src/BUILD.bazel`.

**`qlever_target_link_libraries` becomes a macro.** Use `qlever_cc_library`,
`qlever_cc_binary` and `qlever_cc_test` from `//bazel:qlever.bzl` instead of the
plain `cc_*` rules. They add the aggregate header target, the preprocessor
definitions that `CMakeLists.txt` sets via `add_compile_definitions`, and the
libraries that `qlever_target_link_libraries` links everywhere.

**`CompilationInfo.cmake` becomes a rule plus a workspace status command.** A
build rule must not run `git` itself, because Bazel could not track that. The git
hash and the project version therefore come from
`bazel/workspace_status.sh`, which `.bazelrc` registers via
`--workspace_status_command`, and `//bazel:compilation_info.bzl` substitutes them
into `bazel/CompilationInfo.cpp.in`. Unlike the CMake build, the compiler, its
version and the C++ standard are derived from the predefined preprocessor macros
in that template rather than substituted by the build system; the resulting
strings are the same.

**Sources that a library borrows from another directory need `exports_files`.** A
Bazel target may only use sources from its own package. `src/parser` therefore
exports `RdfParser.cpp`, `TripleComponent.cpp` and `AsyncFileBlockDriver.cpp` for
`//src/index:index`, and `src/parser/data` and `src/parser/sparqlParser` export
one file each for `//src/parser:parser`. This keeps the set of libraries exactly
as in the CMake build, including the deliberate cycle-breaking described in
`src/index/CMakeLists.txt`.

## Dependencies that need more than a `bazel_dep`

Most dependencies are a single `bazel_dep` in `MODULE.bazel`. These are not:

- **GoogleTest** is pinned to the same commit as
  `FetchContent_Declare(googletest ...)` via an `archive_override`, so that both
  build systems test against the same framework; the newest version in the
  registry is older than that. Update the two pins together.
- **range-v3** is QLever's fork, so it comes from `bazel/third_party` rather than
  from the registry; the upstream version in the registry does not work.
- **`uriparser`, `ctre`, `fsst`, `spatialjoin` and `pbutil`** are not in the
  registry at all and have hand-written `BUILD` files in `bazel/third_party`.
  `pbutil` is a git submodule of `spatialjoin`, which release tarballs do not
  contain, so it is a repository of its own.
- **OpenTelemetry** needs `--@opentelemetry-cpp//api:abi_version_no=2` in
  `.bazelrc`, the equivalent of `WITH_ABI_VERSION_2` in `CMakeLists.txt`.
- **`prometheus-cpp`** still uses the native C++ rules that Bazel 9 removed, so
  `.bazelrc` re-enables them with `--incompatible_autoload_externally`. Drop that
  line once the module has been updated.
- **`boost.asio`** needs `--@boost.asio//:ssl=boringssl`, see the OpenSSL note
  below.

Two of `pbutil`'s headers are deliberately not exposed: `util/PriorityQueue.h`
would collide with QLever's own header of the same name, and `graph/EDijkstra.h`
is its only user. See `bazel/third_party/pbutil.BUILD.bazel`.

## Deliberate differences

- **No precompiled headers.** Bazel has no equivalent of
  `target_precompile_headers`, so the `USE_PRECOMPILED_HEADERS` option has no
  counterpart. Compile times are correspondingly higher on a cold cache, but
  Bazel's action cache absorbs most of that in day-to-day work.
- **One test target per test binary.** Bazel always runs a test binary as a
  whole, which is what `QLEVER_ONE_TEST_CASE_PER_FILE=ON` does in the CMake
  build; there is no equivalent of `gtest_discover_tests`. Use `--test_filter` to
  select individual cases, and `--test_sharding_strategy` to split a slow binary
  across shards. `SINGLE_TEST_BINARY` has no counterpart either, and needs none:
  Bazel caches and parallelizes per target, so a single binary would only take
  away the granularity it works with.
- **`-fno-semantic-interposition` is always on.** Bazel compiles
  position-independent objects by default, and GCC then refuses to honor
  `always_inline` on functions with external linkage. `CMakeLists.txt` passes the
  same flag, but only for `BUILD_SHARED_LIBS=ON`, because its default build does
  not use `-fPIC` at all.
- **The whole build uses one OpenSSL.** `@s2geometry` depends on the `boringssl`
  module, whose partial `openssl/` headers would otherwise get mixed with the
  system ones. `MODULE.bazel` redirects that module to the system OpenSSL with a
  `local_path_override`; for the same reason `.bazelrc` selects
  `--@boost.asio//:ssl=boringssl`, which after the redirect means "the one
  OpenSSL of this build".
- **No Emscripten support.** The `_EMSCRIPTEN_NO_INDEXBUILDER_AND_SERVER`,
  `_DISABLE_EMSCRIPTEN_PROBLEMATIC_TESTS` and `_NO_BENCHMARK` options have no
  counterpart, so all targets are always defined.
- **The benchmarks are not ported yet.** `benchmark/BUILD.bazel` only builds the
  infrastructure library, because two unit tests link against it. The four
  benchmark executables and `benchmarkWithMain` are still CMake-only.
- **No `install` or CPack equivalent.** Packaging stays with CMake.

## Formatting

`BUILD.bazel`, `.bzl` and `MODULE.bazel` files are formatted with
[buildifier](https://github.com/bazelbuild/buildtools):

```bash
buildifier -r bazel src test benchmark BUILD.bazel MODULE.bazel
buildifier --lint=warn --mode=check -r bazel src test benchmark BUILD.bazel MODULE.bazel
```
