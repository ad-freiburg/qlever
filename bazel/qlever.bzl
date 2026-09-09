# Copyright 2026 The QLever Authors, in particular:
#
# 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
#
# UFR = University of Freiburg, Chair of Algorithms and Data Structures
#
# You may not use this file except in compliance with the Apache 2.0 License,
# which can be found in the `LICENSE` file at the root of the QLever project.

"""The shared build rules of QLever.

`qlever_cc_library`, `qlever_cc_binary` and `qlever_cc_test` are the Bazel
counterparts of the `qlever_target_link_libraries` function in the top-level
`CMakeLists.txt`, and of the `addTest`/`linkTest` family in
`test/CMakeLists.txt`. Use them instead of plain `cc_library`, `cc_binary` and
`cc_test` for everything that is part of QLever itself.

They add three things to every target:

1. `//src:qlever_hdrs`, the single target that carries all of QLever's headers.
   It is the equivalent of `include_directories(src)`; see the comment in
   `src/BUILD.bazel` for why the headers cannot be split up per directory.
2. The preprocessor definitions that `CMakeLists.txt` sets globally via
   `add_compile_definitions`, and the ones that follow from the build settings
   in `//bazel:BUILD.bazel`.
3. `//src/util/MemorySize:memorySize`, which `qlever_target_link_libraries`
   links into every target as well.
"""

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

###############################################################################
# The values of the multiple-choice build settings
###############################################################################

# The log levels of `//bazel:loglevel`. `CMakeLists.txt` maps a level to itself
# (`set(LOG_LEVEL_DEBUG DEBUG)`), so `LOGLEVEL` is simply the chosen value.
LOG_LEVELS = [
    "FATAL",
    "ERROR",
    "WARN",
    "INFO",
    "DEBUG",
    "TIMING",
    "TRACE",
]

# The values of `//bazel:query_cancellation_mode`.
QUERY_CANCELLATION_MODES = [
    "ENABLED",
    "NO_WATCH_DOG",
    "DISABLED",
]

def _multiple_choice_defines(setting, values, define):
    """Return a `select()` that turns a multiple-choice setting into a define."""
    return select({
        "//bazel:%s_%s" % (setting, value): ["%s=%s" % (define, value)]
        for value in values
    })

###############################################################################
# The preprocessor definitions
###############################################################################

# The definitions that `CMakeLists.txt` applies unconditionally.
_UNCONDITIONAL_DEFINES = [
    # Boost::ASIO has a bug when multiple coroutine streams are concurrently in
    # flight; disabling the recycling of awaitable frames works around it. See
    # the long comment in `CMakeLists.txt`.
    "BOOST_ASIO_DISABLE_AWAITABLE_FRAME_RECYCLING",
    # Disable nlohmann implicit conversions (e.g. `json` to `std::string`).
    "JSON_USE_IMPLICIT_CONVERSIONS=0",
]

# The definitions that follow from a single boolean build setting. Each entry
# maps the name of a setting in `//bazel` to the definitions it implies.
_CONDITIONAL_DEFINES = {
    "cheaper_compilation": [
        "_QLEVER_TYPE_ERASED_EXPRESSIONS",
        "QLEVER_CHEAPER_COMPILATION",
    ],
    "reduced_feature_set_for_cpp17": ["QLEVER_REDUCED_FEATURE_SET_FOR_CPP17"],
    "no_unicode": ["QLEVER_NO_UNICODE"],
    "vocab_uncompressed_in_memory": ["QLEVER_VOCAB_UNCOMPRESSED_IN_MEMORY"],
    "run_expensive_tests": ["QLEVER_RUN_EXPENSIVE_TESTS"],
    "enable_expensive_checks": ["AD_ENABLE_EXPENSIVE_CHECKS"],
    "no_timing_tests": ["_QLEVER_NO_TIMING_TESTS"],
    "use_tree_based_cache": ["_QLEVER_USE_TREE_BASED_CACHE"],
    "use_parallel": ["_PARALLEL_SORT"],
    "allow_shutdown": ["ALLOW_SHUTDOWN"],
    "build_shared_libraries": ["QLEVER_BUILD_SHARED_LIBRARIES"],
}

def _conditional_defines():
    """Return the `select()`s for all settings in `_CONDITIONAL_DEFINES`."""
    result = []
    for setting, defines in _CONDITIONAL_DEFINES.items():
        result = result + select({
            "//bazel:%s_enabled" % setting: defines,
            "//conditions:default": [],
        })
    return result

# All definitions that QLever is compiled with. These are `defines` rather than
# `local_defines`, because QLever reads them in headers and not only in `.cpp`
# files, so every dependent has to see the same values.
QLEVER_DEFINES = _UNCONDITIONAL_DEFINES + _conditional_defines() + select({
    # In the C++17 backports mode, `ql::ranges` is `range-v3` and the concept
    # macros expand to `std::enable_if_t`. Otherwise `range-v3` is only used
    # where it is combined with `std::ranges`.
    "//bazel:use_cpp_17_backports_enabled": [
        "QLEVER_CPP_17",
        "CPP_CXX_CONCEPTS=0",
    ],
    "//conditions:default": ["RANGE_V3_COMBINE_WITH_STD"],
}) + select({
    # The backports inside `SparqlExpressionGenerators.h` are requested by three
    # different settings, see `//bazel:expression_generator_backports`.
    "//bazel:expression_generator_backports": [
        "QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17",
    ],
    "//conditions:default": [],
}) + _multiple_choice_defines("loglevel", LOG_LEVELS, "LOGLEVEL") + _multiple_choice_defines(
    "query_cancellation_mode",
    QUERY_CANCELLATION_MODES,
    "QUERY_CANCELLATION_MODE",
)

###############################################################################
# The compiler options
###############################################################################

# The warning flags that `CMakeLists.txt` appends to `CMAKE_CXX_FLAGS`. Unlike
# there, they are set per target rather than globally, so that the third-party
# dependencies keep building with their own warning configuration.
QLEVER_COPTS = [
    "-Wall",
    "-Wextra",
    # Bazel compiles position-independent objects by default, and for those GCC
    # refuses to honor `always_inline` on a function with external linkage,
    # because its body could be replaced at link time ("inlining failed in call
    # to `always_inline` ...: function body can be overwritten at link time").
    # That would break every use of `AD_ALWAYS_INLINE`, see
    # `util/CompilerExtensions.h`. Promising that QLever's own definitions are
    # not interposed makes the attribute work again. `CMakeLists.txt` passes the
    # same flag, but only for `BUILD_SHARED_LIBS=ON`, because its default build
    # does not use `-fPIC` in the first place.
    "-fno-semantic-interposition",
] + select({
    "//bazel:compiler_is_gcc": [
        # Coroutines, which are called differently on Clang and GCC.
        "-fcoroutines",
        # GCC 11-15 emit spurious `-Wmaybe-uninitialized` for template
        # instantiations that cross the system-header boundary, and
        # `-Wmisleading-indentation` is unreliable on large translation units.
        # Formatting is enforced by `clang-format` anyway.
        "-Wno-maybe-uninitialized",
        "-Wno-misleading-indentation",
    ],
    "//conditions:default": [],
}) + select({
    # OpenMP-based parallel sorting, the `USE_PARALLEL` block of `CMakeLists.txt`.
    "//bazel:use_parallel_enabled": ["-fopenmp"],
    "//conditions:default": [],
})

QLEVER_LINKOPTS = select({
    "//bazel:use_parallel_enabled": ["-fopenmp"],
    "//conditions:default": [],
})

###############################################################################
# The implicit dependencies
###############################################################################

# All headers of QLever, plus every third-party library whose headers they
# include. This is the `include_directories(src)` of `CMakeLists.txt`.
_QLEVER_HEADERS = "//src:qlever_hdrs"

# `qlever_target_link_libraries` links `memorySize` into every target except
# into `memorySize` itself.
_MEMORY_SIZE = "//src/util/MemorySize:memorySize"

# GoogleTest and GoogleMock, which `qlever_target_link_libraries` links
# everywhere, because QLever's headers use `FRIEND_TEST`. In the Bazel build of
# GoogleTest, the `gtest` target contains GoogleMock as well.
_GTEST = "@googletest//:gtest"

def _implicit_deps(name):
    deps = [_QLEVER_HEADERS, _GTEST]
    if name != "memorySize":
        deps.append(_MEMORY_SIZE)
    return deps

def _merge_deps(deps, implicit_deps):
    """Add `implicit_deps` to `deps`, skipping the ones that are already there.

    Bazel rejects a duplicated label in `deps`, and a target may well name one of
    the implicit dependencies explicitly, just like it does in the corresponding
    `qlever_target_link_libraries` call. `MemorySizeTest` and
    `RuntimeParametersTest` are examples.
    """
    if type(deps) != "list":
        # `deps` is the result of concatenating a `select()`, whose contents
        # cannot be inspected before the configuration is known. Avoiding
        # duplicates is then up to the caller.
        return deps + implicit_deps
    return deps + [dep for dep in implicit_deps if dep not in deps]

###############################################################################
# The rules
###############################################################################

def qlever_cc_library(name, deps = [], copts = [], defines = [], linkopts = [], **kwargs):
    """A `cc_library` that is part of QLever.

    Args:
      name: The name of the library, as in `add_library`.
      deps: The libraries to link against, as in `qlever_target_link_libraries`.
      copts: Additional compiler options, on top of `QLEVER_COPTS`.
      defines: Additional definitions, on top of `QLEVER_DEFINES`.
      linkopts: Additional linker options, on top of `QLEVER_LINKOPTS`.
      **kwargs: Passed on to `cc_library`.
    """
    cc_library(
        name = name,
        copts = QLEVER_COPTS + copts,
        defines = QLEVER_DEFINES + defines,
        linkopts = QLEVER_LINKOPTS + linkopts,
        deps = _merge_deps(deps, _implicit_deps(name)),
        **kwargs
    )

def qlever_cc_binary(name, srcs = None, deps = [], copts = [], defines = [], linkopts = [], **kwargs):
    """A `cc_binary` that is part of QLever.

    Args:
      name: The name of the executable, as in `add_executable`.
      srcs: The sources. Defaults to `<name>.cpp`.
      deps: The libraries to link against, as in `qlever_target_link_libraries`.
      copts: Additional compiler options, on top of `QLEVER_COPTS`.
      defines: Additional definitions, on top of `QLEVER_DEFINES`.
      linkopts: Additional linker options, on top of `QLEVER_LINKOPTS`.
      **kwargs: Passed on to `cc_binary`.
    """
    cc_binary(
        name = name,
        srcs = srcs if srcs != None else ["%s.cpp" % name],
        copts = QLEVER_COPTS + copts,
        defines = QLEVER_DEFINES + defines,
        linkopts = QLEVER_LINKOPTS + linkopts,
        deps = _merge_deps(deps, _implicit_deps(name)),
        **kwargs
    )

def qlever_cc_test(
        name,
        srcs = None,
        deps = [],
        copts = [],
        defines = [],
        linkopts = [],
        test_utils = True,
        serial = False,
        tags = [],
        **kwargs):
    """A GoogleTest binary that is compiled from `<name>.cpp` and run by `bazel test`.

    This covers the whole `addLinkAndDiscoverTest*` family of
    `test/CMakeLists.txt`. There is no equivalent of `gtest_discover_tests`:
    Bazel always treats a test binary as one test, which is what
    `QLEVER_ONE_TEST_CASE_PER_FILE=ON` does in the CMake build. Individual test
    cases can still be selected with `--test_filter`.

    Args:
      name: The name of the test, as in `addLinkAndDiscoverTest`.
      srcs: The sources. Defaults to `<name>.cpp`.
      deps: The libraries to link against.
      copts: Additional compiler options, on top of `QLEVER_COPTS`.
      defines: Additional definitions, on top of `QLEVER_DEFINES`.
      linkopts: Additional linker options, on top of `QLEVER_LINKOPTS`.
      test_utils: Whether to link against `//test/util:testUtil`, i.e. against
        basically all of QLever. `False` corresponds to
        `addLinkAndDiscoverTestNoLibs`, which is used for the tests of
        standalone utilities.
      serial: Whether the test has to run on its own, without other tests
        running concurrently. This corresponds to
        `addLinkAndDiscoverTestSerial` and is only for tests whose behavior
        depends on wall-clock timing.
      tags: Additional tags, on top of the ones implied by `serial`.
      **kwargs: Passed on to `cc_test`.
    """
    if test_utils:
        deps = deps + ["//test/util:testUtil"]
    cc_test(
        name = name,
        srcs = srcs if srcs != None else ["%s.cpp" % name],
        copts = QLEVER_COPTS + copts,
        defines = QLEVER_DEFINES + defines,
        linkopts = QLEVER_LINKOPTS + linkopts,
        tags = tags + (["exclusive"] if serial else []),
        deps = _merge_deps(deps, _implicit_deps(name) + [
            # `linkTest` in `test/CMakeLists.txt` links every test against
            # `gmock_main` and `global`.
            "//src/global",
            "@googletest//:gtest_main",
            # The shared test helpers, see `//test:headers`.
            "//test:headers",
        ]),
        **kwargs
    )
