################################
# STXXL
################################

# apply STXXL CXXFLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
# add STXXL includes path
include_directories(${STXXL_INCLUDE_DIRS})

add_executable(SparqlParserTest SparqlParserTest.cpp)
add_dependencies(SparqlParserTest googletest parser)
target_link_libraries(SparqlParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(StringUtilsTest StringUtilsTest.cpp)
add_dependencies(StringUtilsTest googletest)
target_link_libraries(StringUtilsTest ${GTEST_LIBRARY} -pthread)

add_executable(LRUCacheTest LRUCacheTest.cpp)
add_dependencies(LRUCacheTest googletest)
target_link_libraries(LRUCacheTest ${GTEST_LIBRARY} -pthread)

add_executable(QueryGraphTest QueryGraphTest.cpp)
add_dependencies(QueryGraphTest googletest parser engine index)
target_link_libraries(QueryGraphTest ${GTEST_LIBRARY} parser engine
                      index -pthread)

add_executable(QueryExecutionTreeTest QueryExecutionTreeTest.cpp)
add_dependencies(QueryExecutionTreeTest googletest)
target_link_libraries(QueryExecutionTreeTest ${GTEST_LIBRARY} parser engine
                      index -pthread)

add_executable(FileTest FileTest.cpp)
add_dependencies(FileTest googletest)
target_link_libraries(FileTest ${GTEST_LIBRARY} -pthread)

add_executable(Simple8bTest Simple8bTest.cpp)
add_dependencies(Simple8bTest googletest)
target_link_libraries(Simple8bTest ${GTEST_LIBRARY} -pthread)

add_executable(VocabularyTest VocabularyTest.cpp)
add_dependencies(VocabularyTest googletest index)
target_link_libraries(VocabularyTest ${GTEST_LIBRARY} index -pthread)

add_executable(TsvParserTest TsvParserTest.cpp)
add_dependencies(TsvParserTest googletest parser)
target_link_libraries(TsvParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(NTriplesParserTest NTriplesParserTest.cpp)
add_dependencies(NTriplesParserTest googletest parser)
target_link_libraries(NTriplesParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(ContextFileParserTest ContextFileParserTest.cpp)
add_dependencies(ContextFileParserTest googletest parser)
target_link_libraries(ContextFileParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(IndexMetaDataTest IndexMetaDataTest.cpp)
add_dependencies(IndexMetaDataTest googletest index)
target_link_libraries(IndexMetaDataTest ${GTEST_LIBRARY} index -pthread)

add_executable(IndexTest IndexTest.cpp)
add_dependencies(IndexTest googletest index)
target_link_libraries(IndexTest ${GTEST_LIBRARY} index parser -pthread)

add_library(tests
						SparqlParserTest
						StringUtilsTest
            LRUCacheTest
            QueryGraphTest
            QueryExecutionTreeTest
            Simple8bTest
            FileTest
            VocabularyTest
            TsvParserTest
            ContextFileParserTest
            IndexMetaDataTest
            IndexTest
            )