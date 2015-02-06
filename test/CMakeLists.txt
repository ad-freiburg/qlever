add_executable(SparqlParserTest SparqlParserTest.cpp)
add_dependencies(SparqlParserTest googletest)
target_link_libraries(SparqlParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(StringUtilsTest StringUtilsTest.cpp)
add_dependencies(StringUtilsTest googletest)
target_link_libraries(StringUtilsTest ${GTEST_LIBRARY} -pthread)

add_executable(LRUCacheTest LRUCacheTest.cpp)
add_dependencies(LRUCacheTest googletest)
target_link_libraries(LRUCacheTest ${GTEST_LIBRARY} -pthread)

add_executable(QueryGraphTest QueryGraphTest.cpp)
add_dependencies(QueryGraphTest googletest)
target_link_libraries(QueryGraphTest ${GTEST_LIBRARY} parser engine -pthread)

add_executable(QueryExecutionTreeTest QueryExecutionTreeTest.cpp)
add_dependencies(QueryExecutionTreeTest googletest)
target_link_libraries(QueryExecutionTreeTest ${GTEST_LIBRARY} parser engine -pthread)

add_library(tests
            SparqlParserTest
            StringUtilsTest
            LRUCacheTest
            QueryGraphTest
            QueryExecutionTreeTest
            )