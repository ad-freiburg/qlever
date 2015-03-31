################################
# STXXL
################################

# apply STXXL CXXFLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
# add STXXL includes path
include_directories(${STXXL_INCLUDE_DIRS})

add_executable(SparqlParserTest SparqlParserTest.cpp)
target_link_libraries(SparqlParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(StringUtilsTest StringUtilsTest.cpp)
target_link_libraries(StringUtilsTest ${GTEST_LIBRARY} -pthread)

add_executable(LRUCacheTest LRUCacheTest.cpp)
target_link_libraries(LRUCacheTest ${GTEST_LIBRARY} -pthread)

add_executable(QueryGraphTest QueryGraphTest.cpp)
target_link_libraries(QueryGraphTest ${GTEST_LIBRARY} parser engine
                      index -pthread)

add_executable(QueryExecutionTreeTest QueryExecutionTreeTest.cpp)
target_link_libraries(QueryExecutionTreeTest ${GTEST_LIBRARY} engine
                      index parser ${STXXL_LIBRARIES} -pthread)

add_executable(FileTest FileTest.cpp)
target_link_libraries(FileTest ${GTEST_LIBRARY} -pthread)

add_executable(Simple8bTest Simple8bTest.cpp)
target_link_libraries(Simple8bTest ${GTEST_LIBRARY} -pthread)

add_executable(VocabularyTest VocabularyTest.cpp)
target_link_libraries(VocabularyTest ${GTEST_LIBRARY} index -pthread)

add_executable(TsvParserTest TsvParserTest.cpp)
target_link_libraries(TsvParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(NTriplesParserTest NTriplesParserTest.cpp)
target_link_libraries(NTriplesParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(ContextFileParserTest ContextFileParserTest.cpp)
target_link_libraries(ContextFileParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(IndexMetaDataTest IndexMetaDataTest.cpp)
target_link_libraries(IndexMetaDataTest ${GTEST_LIBRARY} index -pthread)

add_executable(IndexTest IndexTest.cpp)
target_link_libraries(IndexTest ${GTEST_LIBRARY} index parser ${STXXL_LIBRARIES} -pthread)

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