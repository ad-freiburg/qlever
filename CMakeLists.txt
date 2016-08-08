cmake_minimum_required(VERSION 2.8.4)
project(SparqlEngineDraft)



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

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -Wextra")

set(LOG_LEVEL_INFO 3)
set(LOG_LEVEL_DEBUG 4)
set(LOG_LEVEL_TRACE 5)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DLOGLEVEL=${LOG_LEVEL_INFO}")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DLOGLEVEL=${LOG_LEVEL_TRACE}")




################################
# GTEST
################################
add_subdirectory(third_party/googletest/googletest)
include_directories(third_party/googletest/googletest/include
                      third_party/googletest/googletest/)
################################
# STXXL
################################
add_subdirectory(third_party/stxxl)
# apply STXXL CXXFLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
# add STXXL includes path
include_directories(${STXXL_INCLUDE_DIRS})

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
target_link_libraries(IndexBuilderMain index)

add_executable(SparqlEngineMain src/SparqlEngineMain.cpp)
target_link_libraries (SparqlEngineMain engine)

add_executable(ServerMain src/ServerMain.cpp)
target_link_libraries (ServerMain engine)

add_executable(WriteIndexListsMain src/WriteIndexListsMain.cpp)
target_link_libraries (WriteIndexListsMain engine)

#add_executable(TextFilterComparison src/experiments/TextFilterComparison.cpp)
#target_link_libraries (TextFilterComparison experiments)

enable_testing()
add_test(SparqlParserTest test/SparqlParserTest)
add_test(StringUtilsTest test/StringUtilsTest)
add_test(LRUCacheTest test/LRUCacheTest)
add_test(QueryGraphTest test/QueryGraphTest)
add_test(QueryExecutionTreeTest test/QueryExecutionTreeTest)
add_test(FileTest test/FileTest)
add_test(Simple8bTest test/Simple8bTest)
add_test(VocabularyTest test/VocabularyTest)
add_test(TsvParserTest test/TsvParserTest)
add_test(NTriplesParserTest test/NTriplesParserTest)
add_test(ContextFileParserTest test/ContextFileParserTest)
add_test(IndexMetaDataTest test/IndexMetaDataTest)
add_test(IndexTest test/IndexTest)
add_test(EngineTest test/EngineTest)
add_test(FTSAlgorithmsTest test/FTSAlgorithmsTest)
add_test(QueryPlannerTest test/QueryPlannerTest)
