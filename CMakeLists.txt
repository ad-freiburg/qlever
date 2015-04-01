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

set(LOG_LEVEL 4)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O3 -Wall -DLOGLEVEL=${LOG_LEVEL}")


################################
# GTEST
################################
add_subdirectory(third_party/googletest)
include_directories(third_party/googletest/include
                      third_party/googletest/)
################################
# STXXL
################################
add_subdirectory(third_party/stxxl)
# apply STXXL CXXFLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
# add STXXL includes path
include_directories(${STXXL_INCLUDE_DIRS})

###############################################################################
##### Actual project configuration #####
###############################################################################

add_subdirectory(src/parser)
add_subdirectory(src/engine)
add_subdirectory(src/index)
add_subdirectory(test)


configure_file(src/index.html index.html)
configure_file(src/style.css style.css)
configure_file(src/script.js script.js)


add_executable(IndexBuilderMain src/index/IndexBuilderMain.cpp)
target_link_libraries(IndexBuilderMain index)

add_executable(SparqlEngineMain src/SparqlEngineMain.cpp)
target_link_libraries (SparqlEngineMain engine)

add_executable(ServerMain src/ServerMain.cpp)
target_link_libraries (ServerMain engine)


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