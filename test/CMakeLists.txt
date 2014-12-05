add_executable(SparqlParserTest SparqlParserTest.cpp)
add_dependencies(SparqlParserTest googletest)
target_link_libraries(SparqlParserTest ${GTEST_LIBRARY} parser -pthread)

add_executable(StringUtilsTest StringUtilsTest.cpp)
add_dependencies(StringUtilsTest googletest)
target_link_libraries(StringUtilsTest ${GTEST_LIBRARY} -pthread)

add_library(tests
            SparqlParserTest
            StringUtilsTest
            )