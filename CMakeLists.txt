cmake_minimum_required(VERSION 3.27)
project(QLever C CXX)

# C/C++ Versions
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)
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
add_definitions("-DBOOST_ASIO_DISABLE_AWAITABLE_FRAME_RECYCLING")

# Coroutines require an additional compiler flag that is called differently
# on clang and g++
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11.0.0")
        MESSAGE(FATAL_ERROR "G++ versions older than 11.0 are not supported by QLever")
    else ()
        add_compile_options(-fcoroutines)
    endif ()

elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "16.0.0")
        MESSAGE(FATAL_ERROR "Clang++ versions older than 16.0 are not supported by QLever")
    endif ()
else ()
    MESSAGE(FATAL_ERROR "QLever currently only supports the G++ or LLVM-Clang++ compilers. Found ${CMAKE_CXX_COMPILER_ID}")
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

# Add colored output for Ninja
if ("${CMAKE_GENERATOR}" STREQUAL "Ninja")
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fcolor-diagnostics)
    endif ()
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
        GIT_TAG 33af80a883ddc33d9c0fac0a5b4578301efb18de # main branch on 2024/05/16
)

################################
# NLOHMANN JSON
################################
# Use URL to avoid downloading large repository as recommended here:
# https://json.nlohmann.me/integration/cmake/#fetchcontent
FetchContent_Declare(
        nlohmann-json
        URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
        URL_HASH SHA3_224=b9237e8e8242e915de5be631aa944369c38a377c5e79aa505ba741a9
)

###############################
# ANTLR CPP RUNTIME FOR THE SPARQL PARSER
###############################
set(ANTLR_BUILD_CPP_TESTS OFF CACHE BOOL "don't try to build googletest twice")
set(ANTLR_BUILD_SHARED OFF CACHE BOOL "We're only interested in the static library")
FetchContent_Declare(
        antlr
        GIT_REPOSITORY https://github.com/antlr/antlr4.git
        GIT_TAG cc82115a4e7f53d71d9d905caa2c2dfa4da58899 # 4.13.12
        SOURCE_SUBDIR runtime/Cpp
)

#################################
# Range v3 (for C++-17 backwards compatibility)
################################
FetchContent_Declare(
        range-v3
        GIT_REPOSITORY https://github.com/joka921/range-v3
        GIT_TAG 1dc0b09abab1bdc7d085a78754abd5c6e37a5d0c # 0.12.0
)



################################
# Threading
################################
find_package(Threads REQUIRED)

#################################
# ICU (for proper collation
################################
find_package(ICU 60 REQUIRED COMPONENTS uc i18n)

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
find_package(Boost 1.81 COMPONENTS iostreams program_options url REQUIRED)
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
    target_link_libraries(${target} ${ARGN} absl::flat_hash_map
            absl::flat_hash_set absl::strings absl::str_format ICU::uc
            ICU::i18n OpenSSL::SSL OpenSSL::Crypto GTest::gtest GTest::gmock stxxl fsst nlohmann_json::nlohmann_json)

    # memorySize is a utility library for defining memory sizes.
    if (NOT ${target} STREQUAL "memorySize")
        target_link_libraries(${target} memorySize)
    endif ()
endfunction()


set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -Wall -Wextra")
# Enable the specification of additional compiler flags manually from the commandline
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ADDITIONAL_COMPILER_FLAGS}")

# Enable the manual usage of the C++ 17 backports (currently `range-v3` instead
# of `std::ranges` and the `std::enable_if_t` based expansion of the concept
# macros from `range-v3`.
set(USE_CPP_17_BACKPORTS OFF CACHE BOOL "Use the C++17 backports (range-v3 and enable_if_t instead of std::ranges and concepts)")
if (${USE_CPP_17_BACKPORTS})
    add_definitions("-DQLEVER_CPP_17 -DCPP_CXX_CONCEPTS=0")
endif()

# Enable the specification of additional linker flags manually from the commandline
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${ADDITIONAL_LINKER_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${ADDITIONAL_LINKER_FLAGS}")


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
        GIT_TAG eb9577aae3515d14e6c5564f9aeb046d2e7c1124 # v3.9.0
)

################################
# ABSEIL
################################
set(ABSL_ENABLE_INSTALL ON)
set(ABSL_USE_SYSTEM_INCLUDES ON)
set(ABSL_PROPAGATE_CXX_STD ON)
FetchContent_Declare(
        abseil
        GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
        GIT_TAG 93ac3a4f9ee7792af399cebd873ee99ce15aed08  # 2024-05-16
)

################################
# S2 Geometry
################################
set(BUILD_TESTS OFF CACHE BOOL "no tests for s2")
FetchContent_Declare(
        s2
        GIT_REPOSITORY https://github.com/google/s2geometry.git
        GIT_TAG 5b5eccd54a08ae03b4467e79ffbb076d0b5f221e  #version 0.11.1
        SYSTEM
)


if (USE_PARALLEL)
    include(FindOpenMP)
    if (OPENMP_FOUND)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
        add_definitions("-D_PARALLEL_SORT")
    endif ()
endif ()

OPTION(_NO_TIMING_TESTS "Disable timing tests on platforms where `sleep` is unreliable" OFF)
if (_NO_TIMING_TESTS)
    add_definitions("-D_QLEVER_NO_TIMING_TESTS")
endif ()

if (USE_TREE_BASED_CACHE)
    add_definitions("-D_QLEVER_USE_TREE_BASED_CACHE")
endif ()

if (RUN_EXPENSIVE_TESTS)
    message(STATUS "Running expensive unit tests. This is only recommended in release builds")
    add_definitions("-DQLEVER_RUN_EXPENSIVE_TESTS")
endif ()

if (ENABLE_EXPENSIVE_CHECKS)
    message(STATUS "Enabling checks that potentially have a significant runtime overhead")
    add_definitions("-DAD_ENABLE_EXPENSIVE_CHECKS")
endif ()

set(QUERY_CANCELLATION_MODE "ENABLED" CACHE STRING "Option to allow disabling cancellation checks partially or completely to reduce the overhead of this mechanism during query computation.")
# Hint for cmake gui, but not actually enforced
set_property(CACHE QUERY_CANCELLATION_MODE PROPERTY STRINGS "ENABLED" "NO_WATCH_DOG" "DISABLED")
# So enforce this ourselves
if (QUERY_CANCELLATION_MODE AND NOT QUERY_CANCELLATION_MODE MATCHES "ENABLED|NO_WATCH_DOG|DISABLED")
    message(FATAL_ERROR "Invalid value for QUERY_CANCELLATION_MODE '${QUERY_CANCELLATION_MODE}'. Please remove the option entirely or change it to ENABLED, NO_WATCH_DOG or DISABLED.")
endif ()
add_definitions("-DQUERY_CANCELLATION_MODE=${QUERY_CANCELLATION_MODE}")

################################
# STXXL
################################
# Disable GNU parallel as it prevents build on Ubuntu 14.04
set(USE_GNU_PARALLEL OFF CACHE BOOL "Don't use gnu parallel" FORCE)
set(USE_OPENMP OFF CACHE BOOL "Don't use OPENMP as default" FORCE)

set(STXXL_CXX_FLAGS "${STXXL_CXX_FLAGS} -Wno-deprecated-declarations")
FetchContent_Declare(
        stxxl
        GIT_REPOSITORY https://github.com/ad-freiburg/stxxl.git
        GIT_TAG 449a8fed095ffd5c10560714b446895e54c65486
)

################################
# FSST
################################
FetchContent_Declare(
        fsst
        #GIT_REPOSITORY https://github.com/cwida/fsst.git
        #GIT_TAG c8719ef0aa3740da9685ad2738bb9c8ecc327944
        #FSST Currently has a struct name `Encoder` that leads to ODR violations with the `Encoder`class of S2.
        # I (joka921) have filed bug reports for both of them and a PR for fsst, but for now we use our own fork
        # of Fsst.
        GIT_REPOSITORY https://github.com/joka921/fsst.git
        GIT_TAG 43fb2d1756f5a5d0e85e765c7b51f5e3be8cc83f
)


################################
# RE2
################################
set(RE2_BUILD_TESTING OFF CACHE BOOL "enable testing for RE2" FORCE)
FetchContent_Declare(
        re2
        GIT_REPOSITORY https://github.com/google/re2.git
        GIT_TAG bc0faab533e2b27b85b8ad312abf061e33ed6b5d # v.2023-11-01
        GIT_SHALLOW TRUE
        OVERRIDE_FIND_PACKAGE
)

################################
# Apply FetchContent
################################
FetchContent_MakeAvailable(googletest ctre abseil re2 stxxl fsst s2 nlohmann-json antlr range-v3)
# Disable some warnings in RE2, STXXL, and GTEST
target_compile_options(s2 PRIVATE -Wno-sign-compare -Wno-unused-parameter -Wno-class-memaccess -Wno-comment -Wno-redundant-move -Wno-unknown-warning-option -Wno-maybe-uninitialized -Wno-class-memaccess -Wno-unused-but-set-variable -Wno-unused-function)
target_compile_options(re2 PRIVATE -Wno-unused-parameter)
target_compile_options(stxxl PRIVATE -Wno-deprecated-declarations)
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(gtest PRIVATE -Wno-maybe-uninitialized)
endif ()
include_directories(${ctre_SOURCE_DIR}/single-header)
target_compile_options(fsst PRIVATE -Wno-extra -Wno-all -Wno-error)
target_compile_options(fsst12 PRIVATE -Wno-extra -Wno-all -Wno-error)
include_directories(${fsst_SOURCE_DIR})
include_directories(${range-v3_SOURCE_DIR}/include)
target_compile_options(antlr4_static PRIVATE -Wno-all -Wno-extra -Wno-error -Wno-deprecated-declarations)
# Only required because a lot of classes that do not explicitly link against antlr4_static use the headers.
include_directories(SYSTEM "${antlr_SOURCE_DIR}/runtime/Cpp/runtime/src")

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
if (NOT DONT_UPDATE_COMPILATION_INFO)
    # The first output which is never created is necessary s.t. the command is never cached and
    # always rerun.
    add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/FileThatNeverExists.cpp"
            "${CMAKE_CURRENT_BINARY_DIR}/CompilationInfo.cpp"
            COMMAND cmake -P ${CMAKE_CURRENT_SOURCE_DIR}/CompilationInfo.cmake)
else ()
    add_custom_command(OUTPUT
            "${CMAKE_CURRENT_BINARY_DIR}/CompilationInfo.cpp"
            COMMAND cmake -P ${CMAKE_CURRENT_SOURCE_DIR}/CompilationInfo.cmake)
endif ()

set(LOG_LEVEL_FATAL FATAL)
set(LOG_LEVEL_ERROR ERROR)
set(LOG_LEVEL_WARN WARN)
set(LOG_LEVEL_INFO INFO)
set(LOG_LEVEL_DEBUG DEBUG)
set(LOG_LEVEL_TIMING TIMING)
set(LOG_LEVEL_TRACE TRACE)


if (CMAKE_BUILD_TYPE MATCHES DEBUG)
    set(LOGLEVEL DEBUG CACHE STRING "The loglevel")
else ()
    set(LOGLEVEL INFO CACHE STRING "The loglevel")
endif ()
set_property(CACHE LOGLEVEL PROPERTY STRINGS FATAL ERROR WARN INFO DEBUG TIMING TRACE)
add_definitions(-DLOGLEVEL=${LOG_LEVEL_${LOGLEVEL}})


##################################################
# Warnings about incorrect combination of CMake variables

if (LOGLEVEL MATCHES "FATAL|ERROR" AND QUERY_CANCELLATION_MODE EQUAL "ENABLED")
    message(WARNING "Log level is not printing logs with level WARN, which is necessary when QUERY_CANCELLATION_MODE=ENABLED for it to work properly")
endif ()

##################################################
# Precompiled headers
set(PRECOMPILED_HEADER_FILES_ENGINE src/util/HashMap.h src/engine/Operation.h src/engine/QueryExecutionTree.h)
set(PRECOMPILED_HEADER_FILES_PARSER src/engine/sparqlExpressions/AggregateExpression.h ${ctre_SOURCE_DIR}/single-header/ctre-unicode.hpp ${antlr_SOURCE_DIR}/runtime/Cpp/runtime/src/antlr4-runtime.h)

add_subdirectory(src/parser)
target_precompile_headers(parser PRIVATE ${PRECOMPILED_HEADER_FILES_PARSER})
add_subdirectory(src/engine)
target_precompile_headers(engine PRIVATE ${PRECOMPILED_HEADER_FILES_ENGINE})
add_subdirectory(src/index)
add_subdirectory(src/util)
add_subdirectory(src/global)
add_subdirectory(benchmark)

enable_testing()
option(SINGLE_TEST_BINARY "Link all unit tests into a single binary. This is useful e.g. for code coverage tools" OFF)
add_subdirectory(test)

# Add the library with the constants declared in `CompilationInfo.h` and defined
# in `CompilationInfo.cpp` created by `CompilationInfo.cmake`.
add_library(compilationInfo ${CMAKE_CURRENT_BINARY_DIR}/CompilationInfo.cpp)
qlever_target_link_libraries(compilationInfo)

add_executable(IndexBuilderMain src/index/IndexBuilderMain.cpp)
qlever_target_link_libraries(IndexBuilderMain index ${CMAKE_THREAD_LIBS_INIT} Boost::program_options compilationInfo)

add_executable(ServerMain src/ServerMain.cpp)
qlever_target_link_libraries(ServerMain engine ${CMAKE_THREAD_LIBS_INIT} Boost::program_options compilationInfo)
target_precompile_headers(ServerMain REUSE_FROM engine)

add_executable(VocabularyMergerMain src/VocabularyMergerMain.cpp)
qlever_target_link_libraries(VocabularyMergerMain index ${CMAKE_THREAD_LIBS_INIT})

add_executable(PrintIndexVersionMain src/PrintIndexVersionMain.cpp)
qlever_target_link_libraries(PrintIndexVersionMain util)
