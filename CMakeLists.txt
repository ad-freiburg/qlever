cmake_minimum_required(VERSION 3.27)
if (POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif ()
project(QLever C CXX)

if (APPLE)
    # Dynamically detect Homebrew prefix
    execute_process(
        COMMAND brew --prefix
        OUTPUT_VARIABLE HOMEBREW_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE BREW_RESULT
        ERROR_QUIET
    )
    if (BREW_RESULT EQUAL 0 AND HOMEBREW_PREFIX)
        message(STATUS "Found Homebrew at: ${HOMEBREW_PREFIX}")
        list(APPEND CMAKE_PREFIX_PATH "${HOMEBREW_PREFIX}")
        link_directories(${HOMEBREW_PREFIX}/lib)
        include_directories(${HOMEBREW_PREFIX}/include)
    else()
        message(WARNING "Homebrew not found. You may need to manually set CMAKE_PREFIX_PATH to locate dependencies (Boost, ICU, OpenSSL, etc.)")
    endif()
endif ()

# C/C++ Versions
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Allow the explicit overriding of the C++ standard version, e.g. when the reduced feature set for C++17 is used.
if (NOT DEFINED CMAKE_CXX_STANDARD_MANUALLY_OVERRIDDEN)
  set(CMAKE_CXX_STANDARD 20)
else()
    set(CMAKE_CXX_STANDARD ${CMAKE_CXX_STANDARD_MANUALLY_OVERRIDDEN})
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    # will not take effect without FORCE
    set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR} CACHE PATH "Install top-level directory" FORCE)
endif ()

# Boost::ASIO currently seems to have a bug when multiple coroutine streams are
# concurrently in flight because there were multiple calls to `co_spawn`.
# `ASIO` recycles the memory for awaitable frames, but there seems to only be
# one global unsynchronized memory pool for this recycling, which leads to all
# kinds of race conditions. The following constant disables the memory
# recycling and gets rid of all of those errors and crashes.
# TODO<joka921> Further analyze and then report this bug to the ASIO
# developers.
add_compile_definitions(BOOST_ASIO_DISABLE_AWAITABLE_FRAME_RECYCLING)

# Optionally build shared instead of static libraries. This is useful in
# particular for Debug or RelWithDebugInfo builds, where the individual
# libraries are rather large because of the debug symbols and are linked
# into almost every test binary.
# Note: This option magically influences the behavior of `add_library`, see
# https://cmake.org/cmake/help/latest/variable/BUILD_SHARED_LIBS.html for
# details.
# Note on tests: With BUILD_SHARED_LIBS=ON, each test binary is smaller because
# shared libraries are loaded at runtime rather than linked statically. However,
# every test process now has to load those libraries at startup, which makes the
# per-test-case regime much more expensive; consider combining this option with
# `QLEVER_ONE_TEST_CASE_PER_FILE` below.
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
if(BUILD_SHARED_LIBS)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    add_compile_options(-fPIC -fno-semantic-interposition)
    # Let the code detect that it is compiled into shared libraries, see
    # `AD_ALWAYS_INLINE` in `CompilerExtensions.h` for the only current use.
    add_compile_definitions(QLEVER_BUILD_SHARED_LIBRARIES)
endif()

# Link QLever's shared libraries with `--no-undefined`, such that a library that
# is missing one of its dependencies fails to link right away. Without this, such
# a library links successfully and the missing dependency only shows up (much
# later) when an executable is linked against it, and only with those linkers
# that reject undefined symbols from shared libraries. In particular `mold`,
# which we use in the CI, silently accepts them, so without this check a missing
# dependency can pass the CI and still break the build for everyone who uses the
# default linker.
# The default is `ON`, except in the following cases:
# - The Apple linker doesn't know `--no-undefined` (its default behavior already
#   is to report undefined symbols).
# - Emscripten resolves symbols differently and doesn't support this.
# - The sanitizer runtimes are linked into the executable and not into the
#   individual shared libraries, so instrumented shared libraries legitimately
#   have undefined `__asan_*`, `__ubsan_*`, or `__tsan_*` symbols.
# NOTE: The sanitizer flags may arrive through any of several channels, and none
# of them has been applied to `CMAKE_CXX_FLAGS` yet at this point in the file. We
# therefore inspect all of them: `CMAKE_CXX_FLAGS` itself (for example set by one
# of the `toolchains`), `ADDITIONAL_COMPILER_FLAGS` (which is how the CI passes
# them and which is only appended to `CMAKE_CXX_FLAGS` much further below), and
# the flags of the selected build type. For the last one we additionally match
# the name of the build type, because QLever defines `CMAKE_CXX_FLAGS_ASAN`
# itself, also only further below.
string(TOUPPER "${CMAKE_BUILD_TYPE}" QLEVER_BUILD_TYPE_UPPERCASE)
set(QLEVER_ALL_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ADDITIONAL_COMPILER_FLAGS} ${CMAKE_CXX_FLAGS_${QLEVER_BUILD_TYPE_UPPERCASE}}")
if (QLEVER_ALL_CXX_FLAGS MATCHES "-fsanitize" OR QLEVER_BUILD_TYPE_UPPERCASE STREQUAL "ASAN")
    set(QLEVER_SANITIZER_ENABLED ON)
else ()
    set(QLEVER_SANITIZER_ENABLED OFF)
endif ()
if (BUILD_SHARED_LIBS AND NOT APPLE AND NOT EMSCRIPTEN AND
        NOT QLEVER_SANITIZER_ENABLED)
    set(QLEVER_NO_UNDEFINED_SYMBOLS_DEFAULT ON)
else ()
    set(QLEVER_NO_UNDEFINED_SYMBOLS_DEFAULT OFF)
endif ()
option(QLEVER_NO_UNDEFINED_SYMBOLS "Require that all symbols of a shared library can be resolved when it is linked" ${QLEVER_NO_UNDEFINED_SYMBOLS_DEFAULT})
message(STATUS "QLEVER_NO_UNDEFINED_SYMBOLS is ${QLEVER_NO_UNDEFINED_SYMBOLS}")

# Choose how `ctest` addresses the test cases of a test binary. When `OFF` (the
# default), `gtest_discover_tests` registers every single gtest case as its own
# `ctest` test. When `ON`, each test binary becomes a single `ctest` test that
# runs all of its cases at once.
# Note: `ON` is much faster together with `BUILD_SHARED_LIBS=ON`, because the
# per-case regime starts one subprocess per case, and each of those subprocesses
# has to load all the shared libraries again.
option(QLEVER_ONE_TEST_CASE_PER_FILE "Register one `ctest` test per test binary instead of one per gtest case" OFF)

# Disable nlohmann implicit conversions (e.g. json → std::string) globally so
# all TUs, including those compiled via PCH, see the same flag value.
add_compile_definitions(JSON_USE_IMPLICIT_CONVERSIONS=0)

# By default, QLever uses precompiled headers for widely used modules. However when heavily modifying the headers that
# contribute to the precompiled headers, then it might be beneficial to deactivate precompiled headers.
option(USE_PRECOMPILED_HEADERS "Use precompiled headers to reduce compile times" ON)

option(CHEAPER_COMPILATION "Use type erasure in several places, this reduces the compile time, but decreasese the performance. Can be used for local development" OFF)
if (CHEAPER_COMPILATION)
    add_compile_definitions(_QLEVER_TYPE_ERASED_EXPRESSIONS)
    # The backports are cheaper to compile, as they do not use coroutines, which have an inherent compile time overhead
    # even with debug builds.
    add_compile_definitions(QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17)
    # General flag used throughout the codebase to opt into cheaper-to-compile alternatives.
    add_compile_definitions(QLEVER_CHEAPER_COMPILATION)
endif()

# Coroutines require an additional compiler flag that is called differently
# on clang and g++
option(COMPILER_VERSION_CHECK_DEACTIVATED "Disable compiler version check" OFF)

option(REDUCED_FEATURE_SET_FOR_CPP17 "Exclude some features like `SERVICE`, `LOAD`, and transitive property paths, that haven't been migrated to C++17 yet" OFF)
if (REDUCED_FEATURE_SET_FOR_CPP17)
    add_compile_definitions(QLEVER_REDUCED_FEATURE_SET_FOR_CPP17 QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17)
endif()

option(EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17 "Use the C++17 backports inside `SparqlExpressionGenerators.h`. They are currently less efficient than the C++20 implementations. This option is implicitly activated by the `REDUCED_FEATURE_SET_FOR_CPP17` option, but can be activated separately to run the unit tests for the backports of the `ExpressionGenerators`" OFF)
if (EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17)
    add_compile_definitions(QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17)
endif()

set(RANGE_V3_REQUIRED_BY_COMPILER OFF)
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11.0.0" AND NOT COMPILER_VERSION_CHECK_DEACTIVATED)
        MESSAGE(FATAL_ERROR "G++ versions older than 11.0 are not supported by QLever")
    elseif (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "11.0.0")
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fcoroutines>)
    endif ()
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "12.0.0")
        # GCC 11's libstdc++ doesn't yet provide `std::ranges::owning_view`, on
        # which we now rely, so we fall back to `range-v3` for that compiler.
        MESSAGE(STATUS "range-v3 is used for GCC 11 because its libstdc++ doesn't yet provide `std::ranges::owning_view`")
        set(RANGE_V3_REQUIRED_BY_COMPILER ON)
    endif ()

elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" )
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "16.0.0" AND NOT COMPILER_VERSION_CHECK_DEACTIVATED)
        MESSAGE(FATAL_ERROR "Clang++ versions older than 16.0 are not supported by QLever")
    endif ()
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "17.0.0")
        # clang-16 doesn't support templated structs without explicit deduction guides, but those are required with
        # libstdc++13 ranges in some cases.
        MESSAGE(STATUS "range-v3 is used for clang-16 because of its incompatibility with libstdc++13")
        set(RANGE_V3_REQUIRED_BY_COMPILER ON)
    endif ()
else ()
    MESSAGE(FATAL_ERROR "QLever currently only supports the G++, LLVM-Clang++ or AppleClang compilers. Found ${CMAKE_CXX_COMPILER_ID}")
endif ()

# Additionally fall back to `range-v3` whenever the C++ standard library is
# libstdc++ older than version 12, independent of the compiler. Such versions
# (e.g. the libstdc++ of GCC 8-11) do not yet provide `std::ranges::owning_view`,
# on which we now rely. Detecting the standard library directly (rather than the
# compiler version) also covers Clang paired with an old libstdc++, e.g. in the
# `using enum` rewriter container.
include(CheckCXXSourceCompiles)
check_cxx_source_compiles("
#include <cstddef>
#if defined(_GLIBCXX_RELEASE) && _GLIBCXX_RELEASE < 12
#error libstdc++ older than 12 does not provide std::ranges::owning_view
#endif
int main() { return 0; }
" QLEVER_STDLIB_PROVIDES_OWNING_VIEW)
if (NOT QLEVER_STDLIB_PROVIDES_OWNING_VIEW)
    MESSAGE(STATUS "range-v3 is used because the C++ standard library is libstdc++ older than version 12, which doesn't provide `std::ranges::owning_view`")
    set(RANGE_V3_REQUIRED_BY_COMPILER ON)
endif ()

## Build targets for address sanitizer

set(CMAKE_C_FLAGS_ASAN
        "-fsanitize=address -fsanitize=undefined -fno-optimize-sibling-calls -fsanitize-address-use-after-scope -fno-omit-frame-pointer -g -O1"
        CACHE STRING "Flags used by the C compiler during AddressSanitizer builds."
        FORCE)
set(CMAKE_CXX_FLAGS_ASAN
        "-fsanitize=address -fsanitize=undefined -fno-optimize-sibling-calls -fsanitize-address-use-after-scope -fno-omit-frame-pointer -g -O1"
        CACHE STRING "Flags used by the C++ compiler during AddressSanitizer builds."
        FORCE)

# Add colored output for Ninja. Use per-language compiler-ID generator expressions so that
# flags are only passed to the compiler that actually supports them (e.g. uriparser can be
# compiled by /usr/bin/cc even when the main build uses Clang).
if ("${CMAKE_GENERATOR}" STREQUAL "Ninja")
    add_compile_options(
        $<$<AND:$<COMPILE_LANGUAGE:CXX>,$<CXX_COMPILER_ID:GNU>>:-fdiagnostics-color=always>
        $<$<AND:$<COMPILE_LANGUAGE:C>,$<C_COMPILER_ID:GNU>>:-fdiagnostics-color=always>
        $<$<AND:$<COMPILE_LANGUAGE:CXX>,$<CXX_COMPILER_ID:Clang,AppleClang>>:-fcolor-diagnostics>
        $<$<AND:$<COMPILE_LANGUAGE:C>,$<C_COMPILER_ID:Clang,AppleClang>>:-fcolor-diagnostics>
    )
endif ()

# Enable the manual usage of the C++ 17 backports (currently `range-v3` instead
# of `std::ranges` and the `std::enable_if_t` based expansion of the concept
# macros from `range-v3`.
option(USE_CPP_17_BACKPORTS "Use the C++17 backports (range-v3 and enable_if_t instead of std::ranges and concepts)" OFF)
if (${USE_CPP_17_BACKPORTS} OR ${RANGE_V3_REQUIRED_BY_COMPILER} OR (CMAKE_CXX_STANDARD LESS 20))
    MESSAGE(STATUS "Using the C++17 backports (e.g. range-v3)")
    add_compile_definitions(QLEVER_CPP_17 CPP_CXX_CONCEPTS=0)
    # In the C++17 backports mode, `ql::filesystem` (see `backports/filesystem.h`)
    # aliases `boost::filesystem` instead of `std::filesystem`, so we then need
    # `Boost::filesystem` as a dependency. Without the backports we use
    # `std::filesystem` and must not depend on `boost::filesystem` at all.
    set(QLEVER_CPP_17_BACKPORTS_ENABLED ON)
else ()
    add_compile_definitions(RANGE_V3_COMBINE_WITH_STD)
    set(QLEVER_CPP_17_BACKPORTS_ENABLED OFF)
endif ()

###############################################################################
##### Essential settings #####
###############################################################################
include(FetchContent)

################################
# GTEST AND GMOCK
################################
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG 973323ed64a05b128418e7eab67016db5ba049df # main branch on 2026/06/30
        SYSTEM
        EXCLUDE_FROM_ALL
)

################################
# NLOHMANN JSON
################################
# Use URL to avoid downloading large repository as recommended here:
# https://json.nlohmann.me/integration/cmake/#fetchcontent
FetchContent_Declare(
        nlohmann-json
        URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz
        URL_HASH SHA3_224=0ad805c53f1769489a9c6bdeefd0a36c19f74239785cdb9c4344e8c1
        SYSTEM
        EXCLUDE_FROM_ALL
)

###############################
# ANTLR CPP RUNTIME FOR THE SPARQL PARSER
###############################
if (BUILD_SHARED_LIBS)
  set(ANTLR_BUILD_STATIC OFF CACHE BOOL "Only build shared antlr4 library")
else()
  set(ANTLR_BUILD_SHARED OFF CACHE BOOL "Only build static antlr4 library")
endif()

set(ANTLR_BUILD_CPP_TESTS OFF CACHE BOOL "don't try to build googletest twice")
FetchContent_Declare(
        antlr
        GIT_REPOSITORY https://github.com/antlr/antlr4.git
        GIT_TAG cc82115a4e7f53d71d9d905caa2c2dfa4da58899 # 4.13.12
        SOURCE_SUBDIR runtime/Cpp
        SYSTEM
        EXCLUDE_FROM_ALL
)

#################################
# Range v3 (for C++-17 backwards compatibility)
################################

FetchContent_Declare(
        range-v3
        #TODO<joka921> Merge this fork into the `qlever` GitHub organisation as soon as it exists.
        GIT_REPOSITORY https://github.com/joka921/range-v3
        # This branch removes some differences in the interface between `range-v3` and `std::ranges` s.t.
        # the former can be used as an (almost) drop-in replacement for the latter.
        GIT_TAG 0e2a41b61694e823df1b7d77174b139253c7ac39 # branch fork-for-qlever
        SYSTEM
        EXCLUDE_FROM_ALL
)

#################################
# libspatialjoin
################################
FetchContent_Declare(
        spatialjoin
        GIT_REPOSITORY https://github.com/ad-freiburg/spatialjoin
        GIT_TAG d1170ee08f0b932c73ca53af75338eacb3e043f6
        SYSTEM
        EXCLUDE_FROM_ALL
)
# disable bzip2 and zlib support in spatialjoin, we don't need it
add_compile_definitions(SPATIALJOIN_NO_BZIP2=1 SPATIALJOIN_NO_ZLIB=1)

################################
# Threading
################################
find_package(Threads REQUIRED)

#################################
# ICU (for proper collation
################################
option(QLEVER_NO_UNICODE "Build QLever without a dependency on ICU (International Components for Unicode). When ON, `find_package(ICU)` is skipped, the ICU libraries are not linked, and the `QLEVER_NO_UNICODE` macro is defined. In this mode all string comparisons, sorting, case conversion and normalization operate on plain ASCII bytes instead of proper Unicode collation. This is intended for minimal or embedded builds where ICU is unavailable; do not enable it if you need correct Unicode handling." OFF)
if (QLEVER_NO_UNICODE)
  add_compile_definitions(QLEVER_NO_UNICODE)
endif()

if (NOT QLEVER_NO_UNICODE)
    find_package(ICU 60 REQUIRED COMPONENTS uc i18n)
endif()

###################################
# JEMALLOC
###################################

find_package(jemalloc QUIET)
if (TARGET jemalloc::jemalloc)
    MESSAGE(STATUS "Use jemalloc that was installed via conan")
    link_libraries(jemalloc::jemalloc)

elseif (${JEMALLOC_MANUALLY_INSTALLED})
    link_libraries(jemalloc)
else ()
    find_package(PkgConfig)
    pkg_check_modules(JEMALLOC jemalloc)

    pkg_search_module(JEMALLOC jemalloc)
    if (${JEMALLOC_FOUND})
        include_directories(${JEMALLOC_INCLUDE_DIRS})
        link_libraries(${JEMALLOC_LIBRARIES})
    else ()
        message(WARNING "Jemalloc could not be found via
    pkg-config. If you are sure that you have installed jemalloc on your system
    (e.g. via `apt install libjemalloc-dev` on Ubuntu), you might try rerunning
    cmake with `-DJEMALLOC_MANUALLY_INSTALLED=True`. This is currently necessary
    on Ubuntu 18.04, where pkg-config does not find jemalloc. Continuing without jemalloc,
    this will impact the performance, most notably of the IndexBuilder")
    endif ()
endif ()

### io_uring (optional, for asynchronous batch vocabulary I/O)
option(USE_IO_URING "Enable io_uring support for batch vocabulary I/O (requires liburing)" ON)
if (USE_IO_URING)
    find_package(PkgConfig)
    pkg_check_modules(URING QUIET liburing)
    if (URING_FOUND)
        MESSAGE(STATUS "Found liburing, enabling io_uring support")
        add_compile_definitions(QLEVER_HAS_IO_URING)
        include_directories(${URING_INCLUDE_DIRS})
        link_libraries(${URING_LIBRARIES})
    else ()
        MESSAGE(STATUS "USE_IO_URING is ON but liburing was not found, building without io_uring support")
    endif ()
else ()
    MESSAGE(STATUS "USE_IO_URING is OFF, building without io_uring support")
endif ()

### ZSTD
find_package(ZSTD QUIET)
if (TARGET zstd::libzstd_static)
    MESSAGE(STATUS "Use zstd that was installed via conan")
    link_libraries(zstd::libzstd_static)
else ()
    link_libraries(zstd)
endif ()


######################################
# BOOST
######################################
# `Boost::filesystem` is only needed in the C++17 backports mode, where
# `ql::filesystem` aliases `boost::filesystem` (see `backports/filesystem.h`).
# Without the backports we use `std::filesystem` and must not depend on
# `boost::filesystem`, so only request the component when the backports are on.
if (QLEVER_CPP_17_BACKPORTS_ENABLED)
  set(QLEVER_BOOST_FILESYSTEM_COMPONENT filesystem)
else()
  set(QLEVER_BOOST_FILESYSTEM_COMPONENT "")
endif()
if (REDUCED_FEATURE_SET_FOR_CPP17)
  # The reduced feature set (which also implies compatibility with older Boost versions) doesn't use `Boost::url`
  # which is only available in Boost versions >= 1.81
  find_package(Boost 1.71 COMPONENTS iostreams program_options container ${QLEVER_BOOST_FILESYSTEM_COMPONENT} REQUIRED CONFIG)
else()
  find_package(Boost 1.81 COMPONENTS iostreams program_options url container ${QLEVER_BOOST_FILESYSTEM_COMPONENT} REQUIRED CONFIG)
endif()
include_directories(${Boost_INCLUDE_DIR})


######################################
# SSL
######################################
find_package(OpenSSL REQUIRED)

##############################################
# Define a set of common third-party and self made libraries that are used almost
# as frequently as the standard library or as a  drop-in replacement for some of
# its functionality.To enable the usage in all parts of QLever, call the following
# function `qlever_target_link_libraries` for all libraries and executables. It
# is a drop-in replacement for `target_link_libraries` that additionally links
# against the common libraries.
function(qlever_target_link_libraries target)
    # Only link `Boost::filesystem` in the C++17 backports mode (see the `BOOST`
    # section above); without the backports `ql::filesystem` is `std::filesystem`
    # and `boost::filesystem` must not be a dependency.
    if (QLEVER_CPP_17_BACKPORTS_ENABLED)
        set(QLEVER_BOOST_FILESYSTEM_LIB Boost::filesystem)
    else()
        set(QLEVER_BOOST_FILESYSTEM_LIB "")
    endif()
    target_link_libraries(${target} PUBLIC ${ARGN}
            absl::base absl::compare absl::flat_hash_map absl::flat_hash_set
            absl::strings absl::str_format absl::status absl::statusor absl::time
            absl::hash absl::synchronization
            re2 OpenSSL::SSL OpenSSL::Crypto GTest::gtest GTest::gmock
            fsst nlohmann_json::nlohmann_json Boost::container
            ${QLEVER_BOOST_FILESYSTEM_LIB} uriparser)
    if (NOT QLEVER_NO_UNICODE)
        target_link_libraries(${target} PUBLIC ICU::uc ICU::i18n)
    endif()

    # memorySize is a utility library for defining memory sizes.
    if (NOT ${target} STREQUAL "memorySize")
        target_link_libraries(${target} PUBLIC memorySize)
    endif ()

    # If this is a shared library, then require that all of its symbols can
    # already be resolved when it is linked, see `QLEVER_NO_UNDEFINED_SYMBOLS`.
    if (QLEVER_NO_UNDEFINED_SYMBOLS)
        target_link_options(${target} PRIVATE
                $<$<STREQUAL:$<TARGET_PROPERTY:TYPE>,SHARED_LIBRARY>:-Wl,--no-undefined>)
    endif ()
endfunction()


set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -Wall -Wextra")
# GCC 11–15 emit spurious `-Wmaybe-uninitialized` for template instantiations
# that cross the system-header boundary (e.g. absl containers instantiated with
# user types, std::variant). Suppress for the affected version range so the
# flag can be re-enabled easily once GCC fixes the false-positive behavior.
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND
    CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "11" AND
    CMAKE_CXX_COMPILER_VERSION VERSION_LESS "16")
    # False positives from template instantiation crossing system-header boundaries.
    add_compile_options(-Wno-maybe-uninitialized)
    # Unreliable on large translation units; formatting is enforced by clang-format anyway.
    add_compile_options(-Wno-misleading-indentation)
endif()
# Enable the specification of additional compiler flags manually from the commandline
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ADDITIONAL_COMPILER_FLAGS}")


set(VOCAB_UNCOMPRESSED_IN_MEMORY OFF CACHE BOOL "Store QLever's vocabulary uncompressed and completely in RAM")
if (${VOCAB_UNCOMPRESSED_IN_MEMORY})
    add_compile_definitions(QLEVER_VOCAB_UNCOMPRESSED_IN_MEMORY)
endif ()

# Enable the specification of additional linker flags manually from the commandline
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${ADDITIONAL_LINKER_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${ADDITIONAL_LINKER_FLAGS}")


if (${PERFTOOLS_PROFILER})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lprofiler")
    message(STATUS "Adding -lprofiler (make sure your have google-perftools installed.)")
endif ()

if (${ALLOW_SHUTDOWN})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DALLOW_SHUTDOWN")
    message(STATUS "Adding -DALLOW_SHUTDOWN")
endif ()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")

################################
# CTRE, Compile-Time-Regular-Expressions
################################
FetchContent_Declare(
        ctre
        GIT_REPOSITORY https://github.com/hanickadot/compile-time-regular-expressions.git
        GIT_TAG e34c26ba149b9fd9c34aa0f678e39739641a0d1e # v3.10.0
        SYSTEM
        EXCLUDE_FROM_ALL
)

################################
# ABSEIL
################################
set(ABSL_ENABLE_INSTALL ON)
set(ABSL_USE_SYSTEM_INCLUDES ON CACHE BOOL "Treat `abseil` headers as system headers, i.e. don't emit warnings from them" FORCE)
set(ABSL_PROPAGATE_CXX_STD ON)
FetchContent_Declare(
        abseil
        GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
        GIT_TAG 255c84dadd029fd8ad25c5efb5933e47beaa00c7  # tag 20260107.1 (LTS 2026-01-07)
        EXCLUDE_FROM_ALL
)

################################
# S2 Geometry
################################
set(BUILD_TESTS OFF CACHE BOOL "no tests for s2")
FetchContent_Declare(
        s2
        GIT_REPOSITORY https://github.com/google/s2geometry.git
        GIT_TAG a37aba69f14af676e9605cd9515c8aca6cef8342  # tag v0.13.1
        SYSTEM
        EXCLUDE_FROM_ALL
)


if (USE_PARALLEL)
    include(FindOpenMP)
    if (OPENMP_FOUND)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
        add_compile_definitions(_PARALLEL_SORT)
    endif ()
endif ()

OPTION(_NO_TIMING_TESTS "Disable timing tests on platforms where `sleep` is unreliable" OFF)
if (_NO_TIMING_TESTS)
    add_compile_definitions(_QLEVER_NO_TIMING_TESTS)
endif ()

if (USE_TREE_BASED_CACHE)
    add_compile_definitions(_QLEVER_USE_TREE_BASED_CACHE)
endif ()

if (RUN_EXPENSIVE_TESTS)
    message(STATUS "Running expensive unit tests. This is only recommended in release builds")
    add_compile_definitions(QLEVER_RUN_EXPENSIVE_TESTS)
endif ()

if (ENABLE_EXPENSIVE_CHECKS)
    message(STATUS "Enabling checks that potentially have a significant runtime overhead")
    add_compile_definitions(AD_ENABLE_EXPENSIVE_CHECKS)
endif ()

set(QUERY_CANCELLATION_MODE "ENABLED" CACHE STRING "Option to allow disabling cancellation checks partially or completely to reduce the overhead of this mechanism during query computation.")
# Hint for cmake gui, but not actually enforced
set_property(CACHE QUERY_CANCELLATION_MODE PROPERTY STRINGS "ENABLED" "NO_WATCH_DOG" "DISABLED")
# So enforce this ourselves
if (QUERY_CANCELLATION_MODE AND NOT QUERY_CANCELLATION_MODE MATCHES "ENABLED|NO_WATCH_DOG|DISABLED")
    message(FATAL_ERROR "Invalid value for QUERY_CANCELLATION_MODE '${QUERY_CANCELLATION_MODE}'. Please remove the option entirely or change it to ENABLED, NO_WATCH_DOG or DISABLED.")
endif ()
add_compile_definitions(QUERY_CANCELLATION_MODE=${QUERY_CANCELLATION_MODE})

################################
# FSST
################################
FetchContent_Declare(
        fsst
        GIT_REPOSITORY https://github.com/cwida/fsst.git
        GIT_TAG b228af6356196095eaf9f8f5654b0635f969661e # main branch from 27th May 2025
        SYSTEM
        EXCLUDE_FROM_ALL
)


################################
# RE2
################################
set(RE2_BUILD_TESTING OFF CACHE BOOL "enable testing for RE2" FORCE)
FetchContent_Declare(
        re2
        GIT_REPOSITORY https://github.com/google/re2.git
        GIT_TAG 972a15cedd008d846f1a39b2e88ce48d7f166cbd  # main branch from January 23 2026
        GIT_SHALLOW TRUE
        OVERRIDE_FIND_PACKAGE
        SYSTEM
        EXCLUDE_FROM_ALL
)


################################
# uriparser
################################
FetchContent_Declare(
        uriparser
        GIT_REPOSITORY https://github.com/uriparser/uriparser.git
        GIT_TAG 04d8b8df5e0c6bf6c06e472540c015943a613bd2 # 1.0.0
        GIT_SHALLOW TRUE
        OVERRIDE_FIND_PACKAGE
        SYSTEM
        EXCLUDE_FROM_ALL
)

set(URIPARSER_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(URIPARSER_BUILD_DOCS OFF CACHE BOOL "" FORCE)
set(URIPARSER_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(URIPARSER_BUILD_WCHAR_T OFF CACHE BOOL "" FORCE)

################################
# OpenTelemetry C++ SDK
################################
set(WITH_OTLP_HTTP OFF CACHE BOOL "" FORCE)
set(WITH_OTLP_GRPC OFF CACHE BOOL "" FORCE)
set(WITH_PROMETHEUS ON CACHE BOOL "" FORCE)
# Disable gzip compression of the builtin `/metrics` which we don't use.
# Removes the need for zlib as a dependency.
set(ENABLE_COMPRESSION OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK OFF CACHE BOOL "" FORCE)
set(WITH_FUNC_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_EXAMPLES OFF CACHE BOOL "" FORCE)
set(OPENTELEMETRY_INSTALL OFF CACHE BOOL "" FORCE)
set(WITH_STL ON CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(WITH_ABI_VERSION_2 ON CACHE BOOL "" FORCE)
set(WITH_ABI_VERSION_1 OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
        opentelemetry-cpp
        GIT_REPOSITORY https://github.com/open-telemetry/opentelemetry-cpp
        GIT_TAG 2d80af1b1d26e300d9c0f7f51fa360f22c773523 # v1.26.0
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL
        SYSTEM
)

################################
# Apply FetchContent
################################
# On Emscripten, gtest's CMake compiles its own libraries with `GTEST_HAS_PTHREAD=1`,
# but its headers "auto-detect" =0, an ODR mismatch, so we have to set this explicitly.
# See https://github.com/google/googletest/issues/5015
if (EMSCRIPTEN)
    add_compile_definitions(GTEST_HAS_PTHREAD=1)

    # With Emscripten, the DWARF debug info that `-g` puts into the object
    # files (which is all that CMake's `Debug`/`RelWithDebInfo` flags do) only
    # ends up in the final `.wasm` when `-g` is ALSO passed at link time.
    # Add it, so that debug build types produce binaries that can be debugged
    # and profiled with source information in the browser dev tools.
    add_link_options("$<$<CONFIG:Debug,RelWithDebInfo>:-g>")
endif ()
FetchContent_MakeAvailable(googletest ctre abseil fsst nlohmann-json antlr range-v3 spatialjoin uriparser opentelemetry-cpp s2 re2)

# Depending on `BUILD_SHARED_LIBS`, the `antlr4` build creates either a shared or
# a static library target. Define the alias `antlr4_umbrella` such that the rest
# of the build can link against the correct one without further case
# distinctions.
if (BUILD_SHARED_LIBS)
    target_compile_options(antlr4_shared PRIVATE -fPIC -Wno-all -Wno-extra -Wno-error -Wno-deprecated-declarations)
    add_library(antlr4_umbrella ALIAS antlr4_shared)
else ()
    target_compile_options(antlr4_static PRIVATE -Wno-all -Wno-extra -Wno-error -Wno-deprecated-declarations)
    add_library(antlr4_umbrella ALIAS antlr4_static)
endif ()
# Disable some warnings in RE2 and GTEST
target_compile_options(s2 PRIVATE -Wno-sign-compare -Wno-unused-parameter -Wno-class-memaccess -Wno-comment -Wno-redundant-move -Wno-unknown-warning-option -Wno-maybe-uninitialized -Wno-class-memaccess -Wno-unused-but-set-variable -Wno-unused-function -Wno-nullability-completeness)
target_compile_options(re2 PRIVATE -Wno-unused-parameter -Wno-deprecated-declarations)
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(gtest PRIVATE -Wno-maybe-uninitialized)
endif ()
include_directories(SYSTEM ${ctre_SOURCE_DIR}/single-header)
target_compile_options(fsst PRIVATE -Wno-extra -Wno-all -Wno-error)
target_compile_options(fsst12 PRIVATE -Wno-extra -Wno-all -Wno-error)
include_directories(SYSTEM ${fsst_SOURCE_DIR})
include_directories(SYSTEM ${range-v3_SOURCE_DIR}/include)
# Only required because a lot of classes that do not explicitly link against antlr4_static use the headers.
include_directories(SYSTEM "${antlr_SOURCE_DIR}/runtime/Cpp/runtime/src")
target_compile_options(civetweb PRIVATE -Wno-missing-field-initializers)

message(STATUS ---)
message(STATUS "CXX_FLAGS are : " ${CMAKE_CXX_FLAGS})
message(STATUS "CXX_FLAGS_RELEASE are : " ${CMAKE_CXX_FLAGS_RELEASE})
message(STATUS "CXX_FLAGS_DEBUG are : " ${CMAKE_CXX_FLAGS_DEBUG})
message(STATUS "IMPORTANT: Make sure you have selected the desired CMAKE_BUILD_TYPE")
message(STATUS "CMAKE_BUILD_TYPE is ${CMAKE_BUILD_TYPE}")
message(STATUS ---)

###############################################################################
##### Actual project configuration #####
###############################################################################

include_directories(src)

# Run the script `CompilationInfo.cmake` that creates the file `CompilationInfo.cpp`
# with the current git hash and the current time and date. When specifying
# `-DDONT_UPDATE_COMPILATION_INFO=true` as an argument to `cmake`, the compilation info is
# never updated. This is useful during development to avoid a relinking of the binaries for
# every compilation.
# The first output which is never created is necessary s.t. the command is never cached and
# always rerun. It is omitted when `DONT_UPDATE_COMPILATION_INFO` is set, so the command is
# cached and the compilation info is not regenerated on every build.
set(COMPILATION_INFO_EXTRA_OUTPUT "")
if (NOT DONT_UPDATE_COMPILATION_INFO)
    set(COMPILATION_INFO_EXTRA_OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/FileThatNeverExists.cpp")
endif ()
add_custom_command(OUTPUT ${COMPILATION_INFO_EXTRA_OUTPUT}
        "${CMAKE_CURRENT_BINARY_DIR}/CompilationInfo.cpp"
        COMMAND cmake
        -DCXX_COMPILER_ID=${CMAKE_CXX_COMPILER_ID}
        -DCXX_COMPILER_VERSION=${CMAKE_CXX_COMPILER_VERSION}
        -DCXX_STANDARD=${CMAKE_CXX_STANDARD}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/CompilationInfo.cmake)

set(LOG_LEVEL_FATAL FATAL)
set(LOG_LEVEL_ERROR ERROR)
set(LOG_LEVEL_WARN WARN)
set(LOG_LEVEL_INFO INFO)
set(LOG_LEVEL_DEBUG DEBUG)
set(LOG_LEVEL_TIMING TIMING)
set(LOG_LEVEL_TRACE TRACE)

set(LOGLEVEL DEBUG CACHE STRING
    "The maximum log level QLever is compiled with. Less verbose log levels can be chosen at runtime via the 'log-level' runtime parameter."
)
set_property(CACHE LOGLEVEL PROPERTY STRINGS FATAL ERROR WARN INFO DEBUG TIMING TRACE)
add_compile_definitions(LOGLEVEL=${LOG_LEVEL_${LOGLEVEL}})


##################################################
# Warnings about incorrect combination of CMake variables

if (LOGLEVEL MATCHES "FATAL|ERROR|WARN|INFO" AND QUERY_CANCELLATION_MODE EQUAL "ENABLED")
  message(WARNING "`QUERY_CANCELLATION_MODE` is set to `ENABLED`, but the log level is set to ${LOGLEVEL}; therefore, cancellation check messages, which have log level `DEBUG`, are not printed (but there is a dedicated thread that decides when to print those messages)")
endif ()

add_subdirectory(src/parser)
add_subdirectory(src/engine)
add_subdirectory(src/index)
add_subdirectory(src/util)
add_subdirectory(src/rdfTypes)
add_subdirectory(src/global)
add_subdirectory(src/libqlever)

##################################################
# Precompiled headers
#
# PRECOMPILED_HEADER_FILES_EXTERNAL: stable third-party headers that never change
# and are expensive to parse. Applied to every library target.
set(PRECOMPILED_HEADER_FILES_EXTERNAL
    <absl/strings/str_cat.h>
    <absl/container/flat_hash_map.h>
    <absl/container/flat_hash_set.h>
    <range/v3/all.hpp>
    # JSON_USE_IMPLICIT_CONVERSIONS=0 is set globally above, so the bare
    # nlohmann header already sees the correct flag value.
    <nlohmann/json.hpp>
)

# PRECOMPILED_HEADER_FILES_WITH_BEAST: extends EXTERNAL with boost networking and
# iostreams. Applied to targets that transitively include beast.h or
# boost::iostreams (engine, index, http). Without this, the index target would
# still parse beast.h (~8 s) on every TU that reaches it via
# Operation.h → QueryExecutionContext.h → ResponseMiddleware.h → beast.h, even
# though it is already covered for engine TUs.
set(PRECOMPILED_HEADER_FILES_WITH_BEAST
    <util/http/beast.h>
    <boost/iostreams/filter/gzip.hpp>
    <boost/iostreams/filtering_stream.hpp>
    ${PRECOMPILED_HEADER_FILES_EXTERNAL})

set(PRECOMPILED_HEADER_FILES_ENGINE
    src/util/HashMap.h
    src/engine/Operation.h
    src/engine/QueryExecutionTree.h
    ${PRECOMPILED_HEADER_FILES_WITH_BEAST})

set(PRECOMPILED_HEADER_FILES_PARSER
    src/engine/sparqlExpressions/AggregateExpression.h
    ${ctre_SOURCE_DIR}/single-header/ctre-unicode.hpp
    ${antlr_SOURCE_DIR}/runtime/Cpp/runtime/src/antlr4-runtime.h
    ${PRECOMPILED_HEADER_FILES_EXTERNAL})

set(PRECOMPILED_HEADER_FILES_SPARQL_EXPRESSIONS
   src/engine/sparqlExpressions/SparqlExpression.h
   ${PRECOMPILED_HEADER_FILES_WITH_BEAST})

if (USE_PRECOMPILED_HEADERS)
    target_precompile_headers(sparqlParser PRIVATE ${PRECOMPILED_HEADER_FILES_PARSER})
    target_precompile_headers(engine PRIVATE ${PRECOMPILED_HEADER_FILES_ENGINE})
    target_precompile_headers(sparqlExpressions PRIVATE ${PRECOMPILED_HEADER_FILES_SPARQL_EXPRESSIONS})
    # Note: index TUs that include `Operation.h` transitively pull in `beast.h`
    # and `boost::iostreams` via `ResponseMiddleware.h` / `CompressorStream.h`.
    target_precompile_headers(index PRIVATE ${PRECOMPILED_HEADER_FILES_WITH_BEAST})
    target_precompile_headers(qlever_util PRIVATE ${PRECOMPILED_HEADER_FILES_EXTERNAL})
    target_precompile_headers(mediaTypes PRIVATE ${PRECOMPILED_HEADER_FILES_EXTERNAL})
    if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
        target_precompile_headers(http PRIVATE ${PRECOMPILED_HEADER_FILES_WITH_BEAST})
    endif ()
    target_precompile_headers(rdfTypes PRIVATE ${PRECOMPILED_HEADER_FILES_EXTERNAL})
    target_precompile_headers(rdfEscaping PRIVATE ${PRECOMPILED_HEADER_FILES_EXTERNAL})
endif ()

add_subdirectory(benchmark)

enable_testing()
option(SINGLE_TEST_BINARY "Link all unit tests into a single binary. This is useful e.g. for code coverage tools and greatly reduces the total disk space required when building all unit tests with debug symbols" OFF)
add_subdirectory(test)

# Add the library with the constants declared in `CompilationInfo.h` and defined
# in `CompilationInfo.cpp` created by `CompilationInfo.cmake`.
add_library(compilationInfo ${CMAKE_CURRENT_BINARY_DIR}/CompilationInfo.cpp)
qlever_target_link_libraries(compilationInfo)

OPTION(_EMSCRIPTEN_NO_INDEXBUILDER_AND_SERVER "Don't build some targets when compiling with Emscripten." OFF)

if (NOT _EMSCRIPTEN_NO_INDEXBUILDER_AND_SERVER)
    add_executable(qlever-index src/index/IndexBuilderMain.cpp)
    qlever_target_link_libraries(qlever-index qlever index ${CMAKE_THREAD_LIBS_INIT} Boost::program_options compilationInfo global)

    add_executable(qlever-server src/ServerMain.cpp)
    qlever_target_link_libraries(qlever-server engine server ${CMAKE_THREAD_LIBS_INIT} Boost::program_options compilationInfo)
    if (USE_PRECOMPILED_HEADERS)
        target_precompile_headers(qlever-server REUSE_FROM engine)
    endif()
endif()

add_executable(LibQLeverExample src/libqlever/LibQLeverExample.cpp)
qlever_target_link_libraries(LibQLeverExample sparqlParser engine qlever_util index qlever)

add_executable(VocabularyMergerMain src/VocabularyMergerMain.cpp)
qlever_target_link_libraries(VocabularyMergerMain index sparqlParser ${CMAKE_THREAD_LIBS_INIT})

add_executable(PrintIndexVersionMain src/PrintIndexVersionMain.cpp)
qlever_target_link_libraries(PrintIndexVersionMain qlever_util)

if (NOT _EMSCRIPTEN_NO_INDEXBUILDER_AND_SERVER)
    install(TARGETS
        qlever-index
        qlever-server
        RUNTIME DESTINATION bin
    )
endif()

###############################################################
# CPack packaging
###############################################################

# Get version from git describe for CPack
include(GitVersion.cmake)

set(CPACK_PACKAGE_NAME "qlever-bin")

# Set CPACK_PACKAGE_VERSION according to Debian packaging naming rules
# If git describe output begins with v + semver:
if (PROJECT_VERSION MATCHES "^v([0-9]+\\.[0-9]+\\.[0-9]+)(-[0-9]+)?(-g[0-9a-f]+)?$")

    # CMAKE_MATCH_1 is "major.minor.patch"
    set(TAG_VERSION "${CMAKE_MATCH_1}")

    # CMAKE_MATCH_2 is "-commits_ahead" (or empty)
    if (CMAKE_MATCH_2)
        string(SUBSTRING "${CMAKE_MATCH_2}" 1 -1 COMMITS_AHEAD)
        set(CPACK_PACKAGE_VERSION "${TAG_VERSION}+${COMMITS_AHEAD}")
    else()
        set(CPACK_PACKAGE_VERSION "${TAG_VERSION}")
    endif()

else()
    # Fallback for non-tagged builds: Use commit hash
    if (PROJECT_VERSION MATCHES "-g([0-9a-f]+)$")
        set(HASH "${CMAKE_MATCH_1}")
    else()
        set(HASH "${PROJECT_VERSION}")
    endif()
    set(CPACK_PACKAGE_VERSION "0.0.0+${HASH}")
endif()

# Debian package revision number (e.g., the "1" in "0.5.42-1~plucky~25.04").
# Override with: cmake -DPACKAGE_REVISION=2
set(PACKAGE_REVISION "1" CACHE STRING "Debian package revision number")

# Version suffix for distribution-specific packages (e.g., "~plucky~25.04").
# Auto-detected from /etc/os-release for Ubuntu and Debian.
# For derived distros, override with: cmake -DVERSION_SUFFIX="~noble~24.04"
set(DEFAULT_VERSION_SUFFIX "")
if (EXISTS "/etc/os-release")
    file(STRINGS "/etc/os-release" OS_RELEASE_CONTENTS)

    set(_OS_ID "")
    set(_OS_VERSION_ID "")
    set(_OS_VERSION_CODENAME "")

    foreach(line ${OS_RELEASE_CONTENTS})
        if (line MATCHES "^ID=\"?([^\"]*)\"?$")
            set(_OS_ID "${CMAKE_MATCH_1}")
        elseif (line MATCHES "^VERSION_ID=\"?([^\"]*)\"?$")
            set(_OS_VERSION_ID "${CMAKE_MATCH_1}")
        elseif (line MATCHES "^VERSION_CODENAME=\"?([^\"]*)\"?$")
            set(_OS_VERSION_CODENAME "${CMAKE_MATCH_1}")
        endif()
    endforeach()

    if (_OS_VERSION_CODENAME AND _OS_VERSION_ID)
        if (_OS_ID STREQUAL "ubuntu")
            set(DEFAULT_VERSION_SUFFIX "~${_OS_VERSION_CODENAME}~${_OS_VERSION_ID}")
        elseif (_OS_ID STREQUAL "debian")
            set(DEFAULT_VERSION_SUFFIX "~${_OS_VERSION_CODENAME}~deb${_OS_VERSION_ID}")
        endif()
    endif()
endif()

set(VERSION_SUFFIX "${DEFAULT_VERSION_SUFFIX}" CACHE STRING "Distribution-specific version suffix")

set(CPACK_PACKAGE_VERSION "${CPACK_PACKAGE_VERSION}-${PACKAGE_REVISION}${VERSION_SUFFIX}")

set(CPACK_PACKAGE_CONTACT "mundhahj@tf.uni-freiburg.de")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/ad-freiburg/qlever")

set(CPACK_OUTPUT_FILE_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/packages")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)

set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)

set(CPACK_VERBATIM_VARIABLES YES)

include(CPack)
