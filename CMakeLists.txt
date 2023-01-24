cmake_minimum_required(VERSION 3.16)
project(QLever C CXX)

# C/C++ Versions
set (CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Coroutines require an additional compiler flag that is called differently
# on clang and g++
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-fcoroutines HAS_COROUTINES)
if (HAS_COROUTINES)
    add_compile_options(-fcoroutines)
else()
    add_compile_options(-fcoroutines-ts)
endif()

## Build targets for address sanitizer
# AddressSanitize
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

if (("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU") AND
   (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "12") AND
   (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "12.2"))
    message(STATUS "Adding -Wno-error for g++12.0 because of the many false positive warnings from stdlib headers")
    add_compile_options(-Wno-error)
    else()
endif ()

###############################################################################
##### Essential settings #####
###############################################################################

################################
# GTEST AND GMOCK
################################
add_subdirectory(third_party/googletest EXCLUDE_FROM_ALL)
include_directories(third_party/googletest/googletest/include)
include_directories(third_party/googletest/googlemock/include)

###############################
# ANTLR CPP RUNTIME FOR THE SPARQL PARSER
###############################
set(ANTLR_BUILD_CPP_TESTS OFF CACHE BOOL "don't try to build googletest twice")
add_subdirectory(third_party/antlr4/runtime/Cpp EXCLUDE_FROM_ALL )
target_compile_options(antlr4_static PRIVATE -Wno-all -Wno-extra -Wno-unqualified-std-cast-call -Wno-error -Wno-deprecated-declarations)
include_directories(SYSTEM third_party/antlr4/runtime/Cpp/runtime/src )

################################
# Threading
################################
find_package(Threads REQUIRED)

#################################
# ICU (for proper collation
################################
find_package(ICU 60 REQUIRED COMPONENTS uc i18n)
include_directories(${ICU_INCLUDE_DIR})

###################################
# JEMALLOC
###################################
if (${JEMALLOC_MANUALLY_INSTALLED})
  link_libraries(jemalloc)
else()
  find_package(PkgConfig)
  pkg_check_modules (JEMALLOC jemalloc)

  pkg_search_module(JEMALLOC jemalloc)
  if (${JEMALLOC_FOUND})
    include_directories(${JEMALLOC_INCLUDE_DIRS})
    link_libraries(${JEMALLOC_LIBRARIES})
  else ()
    message(FATAL_ERROR "Jemalloc is required, but could not be found via
    pkg-config. If you are sure that you have installed jemalloc on your system
    (e.g. via `apt install libjemalloc-dev` on Ubuntu), you might try rerunning
    cmake with `-DJEMALLOC_MANUALLY_INSTALLED=True`. This is currently necessary
    on Ubuntu 18.04, where pkg-config does not find jemalloc.")
  endif()
endif()


######################################
# BOOST
######################################
find_package(Boost 1.74 COMPONENTS iostreams program_options REQUIRED)
include_directories(${Boost_INCLUDE_DIR})


######################################
# SSL
######################################
find_package(OpenSSL REQUIRED)


set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -Wall -Wextra")
# Enable the specification of additional compiler flags manually from the commandline
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ADDITIONAL_COMPILER_FLAGS}")

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
# NLOHNMANN-JSON
################################
# Header only, nothing to include
include_directories(third_party/json/)

################################
# CTRE, Compile-Time-Regular-Expressions
################################
# Header only, nothing to include
include_directories(third_party/ctre/include)

################################
# ABSEIL
################################
set(BUILD_TESTING OFF CACHE BOOL "Don't build tests for abseil" FORCE)
add_subdirectory(third_party/abseil-cpp  EXCLUDE_FROM_ALL)
include_directories(SYSTEM third_party/abseil-cpp/)

if (USE_PARALLEL)
    include(FindOpenMP)
    if (OPENMP_FOUND)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
        add_definitions("-D_PARALLEL_SORT")
    endif ()
endif()

if (USE_TREE_BASED_CACHE)
    add_definitions("-D_QLEVER_USE_TREE_BASED_CACHE")
endif()

if (RUN_EXPENSIVE_TESTS)
    message(STATUS "Running expensive unit tests. This is only recommended in release builds")
    add_definitions("-DQLEVER_RUN_EXPENSIVE_TESTS")
endif()

################################
# STXXL
################################
# Disable GNU parallel as it prevents build on Ubuntu 14.04
set(USE_GNU_PARALLEL OFF CACHE BOOL "Don't use gnu parallel" FORCE)
set(USE_OPENMP OFF CACHE BOOL "Don't use OPENMP as default" FORCE)

add_subdirectory(third_party/stxxl EXCLUDE_FROM_ALL)

# apply STXXL CXXFLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
include_directories(SYSTEM ${STXXL_INCLUDE_DIRS})

################################
# RE2
################################
# RE2 has a lot of unused-parameter warnings, we will deactivate
# these for the subproject
set(LOCAL_CXX_BACKUP_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter")

set(RE2_BUILD_TESTING OFF CACHE BOOL "enable testing for RE2" FORCE)
add_subdirectory(third_party/re2 EXCLUDE_FROM_ALL)
target_compile_options(re2 PRIVATE -Wno-unused-but-set-variable)
include_directories(SYSTEM third_party/re2)

# reinstate original flags including all warnings
set(CMAKE_CXX_FLAGS "${LOCAL_CXX_BACKUP_FLAGS}")

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
# with the current git hash and the curent time and date. When specifying
# `-DDONT_UPDATE_COMPILATION_INFO=true` as an argument to `cmake`, the compilation info is
# never updated. This is useful during development to avoid a relinking of the binaries for
# every compilation.
if (NOT DONT_UPDATE_COMPILATION_INFO)
    # The first output which is never created is necessary s.t. the command is never cached and
    # always rerun.
    add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/FileThatNeverExists.cpp"
                              "${CMAKE_CURRENT_BINARY_DIR}/CompilationInfo.cpp"
                       COMMAND cmake -P ${CMAKE_CURRENT_SOURCE_DIR}/CompilationInfo.cmake)
else()
    add_custom_command(OUTPUT
            "${CMAKE_CURRENT_BINARY_DIR}/CompilationInfo.cpp"
            COMMAND cmake -P ${CMAKE_CURRENT_SOURCE_DIR}/CompilationInfo.cmake)
endif()

set(LOG_LEVEL_FATAL 0)
set(LOG_LEVEL_ERROR 1)
set(LOG_LEVEL_WARN 2)
set(LOG_LEVEL_INFO 3)
set(LOG_LEVEL_DEBUG 4)
set(LOG_LEVEL_TIMING 5)
set(LOG_LEVEL_TRACE 6)


if(CMAKE_BUILD_TYPE MATCHES DEBUG)
  set(LOGLEVEL DEBUG CACHE STRING "The loglevel")
else()
  set(LOGLEVEL INFO CACHE STRING "The loglevel")
endif()
set_property(CACHE LOGLEVEL PROPERTY STRINGS FATAL ERROR WARN INFO DEBUG TIMING TRACE)
add_definitions(-DLOGLEVEL=${LOG_LEVEL_${LOGLEVEL}})

##################################################
# Precompiled headers
set(PRECOMPILED_HEADER_FILES_ENGINE src/util/HashMap.h src/engine/Operation.h src/engine/QueryExecutionTree.h)
set(PRECOMPILED_HEADER_FILES_PARSER src/engine/sparqlExpressions/AggregateExpression.h third_party/ctre/include/ctre/ctre.h third_party/antlr4/runtime/Cpp/runtime/src/antlr4-runtime.h)

add_subdirectory(src/parser)
target_precompile_headers(parser PRIVATE ${PRECOMPILED_HEADER_FILES_PARSER})
add_subdirectory(src/engine)
target_precompile_headers(engine PRIVATE ${PRECOMPILED_HEADER_FILES_ENGINE})
add_subdirectory(src/index)
add_subdirectory(src/util)
enable_testing()
add_subdirectory(test)

configure_file(src/web/index.html index.html)
configure_file(src/web/style.css style.css)
configure_file(src/web/script.js script.js)

# Add the library with the constants declared in `CompilationInfo.h` and defined
# in `CompilationInfo.cpp` created by `CompilationInfo.cmake`.
add_library(compilationInfo ${CMAKE_CURRENT_BINARY_DIR}/CompilationInfo.cpp)

add_executable(IndexBuilderMain src/index/IndexBuilderMain.cpp)
target_link_libraries(IndexBuilderMain index ${CMAKE_THREAD_LIBS_INIT} boost_program_options)

add_executable(CreatePatternsMain src/index/CreatePatternsMain.cpp src/util/ConstexprSmallString.h)
target_link_libraries(CreatePatternsMain index ${CMAKE_THREAD_LIBS_INIT})

add_executable(ServerMain src/ServerMain.cpp)
target_link_libraries (ServerMain engine ${CMAKE_THREAD_LIBS_INIT} boost_program_options)
target_precompile_headers(ServerMain REUSE_FROM engine)

add_executable(PrefixHeuristicEvaluatorMain src/PrefixHeuristicEvaluatorMain.cpp)
target_link_libraries (PrefixHeuristicEvaluatorMain index ${CMAKE_THREAD_LIBS_INIT})

add_executable(TurtleParserMain src/TurtleParserMain.cpp)
target_link_libraries(TurtleParserMain parser ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_map)

add_executable(VocabularyMergerMain src/VocabularyMergerMain.cpp)
target_link_libraries(VocabularyMergerMain index ${CMAKE_THREAD_LIBS_INIT})

add_executable(PermutationExporterMain src/index/PermutationExporterMain.cpp)
target_link_libraries(PermutationExporterMain index ${CMAKE_THREAD_LIBS_INIT})
