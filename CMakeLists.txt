cmake_minimum_required(VERSION 2.8.4)
project(QLever C CXX)

# C/C++ Versions
set (CMAKE_C_STANDARD 11)
set (CMAKE_C_STANDARD_REQUIRED ON)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

# Check compiler versions:
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  # Check version. If empty, warn. If too old, error out:
  if ("${CMAKE_CXX_COMPILER_VERSION}" STREQUAL "")
    message(WARNING "GCC Compiler version is unknown, proceed at your own risk!")
  elseif (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
    message(FATAL_ERROR "GCC compiler version must be at least 4.8!")
  endif()
endif()

###############################################################################
##### Essential settings #####
###############################################################################

################################
# Threading
################################
find_package (Threads REQUIRED)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -Wall -Wextra -Wno-missing-field-initializers -DGTEST_HAS_TR1_TUPLE=0 -DGTEST_USE_OWN_TR1_TUPLE=0")

if (${PERFTOOLS_PROFILER})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lprofiler")
    message(STATUS "Adding -lprofiler (make sure your have google-perftools installed.)")
endif()

if (${ALLOW_SHUTDOWN})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DALLOW_SHUTDOWN")
    message(STATUS "Adding -DALLOW_SHUTDOWN")
endif()


set(LOG_LEVEL_INFO 3)
set(LOG_LEVEL_DEBUG 4)
set(LOG_LEVEL_TRACE 5)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DLOGLEVEL=${LOG_LEVEL_DEBUG}")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DLOGLEVEL=${LOG_LEVEL_DEBUG}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DLOGLEVEL=${LOG_LEVEL_DEBUG}")

################################
# GTEST
################################
add_subdirectory(third_party/googletest/googletest)
include_directories(third_party/googletest/googletest/include)
################################
# STXXL
################################
# Disable GNU parallel as it prevents build on Ubuntu 14.04
set(USE_GNU_PARALLEL OFF CACHE BOOL "Don't use gnu parallel" FORCE)
set(USE_OPENMP OFF CACHE BOOL "Don't use OpenMP" FORCE)
add_subdirectory(third_party/stxxl)
# apply STXXL CXXFLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
# add STXXL includes path
include_directories(SYSTEM ${STXXL_INCLUDE_DIRS})

message(STATUS ---)
message(STATUS "CXX_FLAGS are : " ${CMAKE_CXX_FLAGS})
message(STATUS "CXX_FLAGS_RELEASE are : " ${CMAKE_CXX_FLAGS_RELEASE})
message(STATUS "CXX_FLAGS_DEBUG are : " ${CMAKE_CXX_FLAGS_DEBUG})
message(STATUS "IMPORTANT: Make sure you have selected the desired CMAKE_BUILD_TYPE")
message(STATUS ---)

###############################################################################
##### Actual project configuration #####
###############################################################################

add_subdirectory(src/parser)
add_subdirectory(src/engine)
add_subdirectory(src/index)
#add_subdirectory(src/experiments)
add_subdirectory(test)



configure_file(src/web/index.html index.html)
configure_file(src/web/style.css style.css)
configure_file(src/web/script.js script.js)


add_executable(IndexBuilderMain src/index/IndexBuilderMain.cpp)
target_link_libraries(IndexBuilderMain index ${CMAKE_THREAD_LIBS_INIT})

add_executable(SparqlEngineMain src/SparqlEngineMain.cpp)
target_link_libraries (SparqlEngineMain engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(ServerMain src/ServerMain.cpp)
target_link_libraries (ServerMain engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(WriteIndexListsMain src/WriteIndexListsMain.cpp)
target_link_libraries (WriteIndexListsMain engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(MetaDataConverterMain src/MetaDataConverterMain.cpp)
target_link_libraries (MetaDataConverterMain metaConverter ${CMAKE_THREAD_LIBS_INIT})

#add_executable(TextFilterComparison src/experiments/TextFilterComparison.cpp)
#target_link_libraries (TextFilterComparison experiments)

enable_testing()
add_test(SparqlParserTest test/SparqlParserTest)
add_test(StringUtilsTest test/StringUtilsTest)
add_test(LRUCacheTest test/LRUCacheTest)
add_test(FileTest test/FileTest)
add_test(Simple8bTest test/Simple8bTest)
add_test(VocabularyTest test/VocabularyTest)
add_test(ExternalVocabularyTest test/ExternalVocabularyTest)
add_test(TsvParserTest test/TsvParserTest)
add_test(NTriplesParserTest test/NTriplesParserTest)
add_test(ContextFileParserTest test/ContextFileParserTest)
add_test(IndexMetaDataTest test/IndexMetaDataTest)
add_test(IndexTest test/IndexTest)
add_test(EngineTest test/EngineTest)
add_test(FTSAlgorithmsTest test/FTSAlgorithmsTest)
add_test(QueryPlannerTest test/QueryPlannerTest)
add_test(ConversionsTest test/ConversionsTest)
add_test(SparsehashTest test/SparsehashTest)
add_test(VocabularyGeneratorTest test/VocabularyGeneratorTest)
add_test(MmapVectorTest test/MmapVectorTest)
