add_executable(SparqlParserTest SparqlParserTest.cpp)
target_link_libraries(SparqlParserTest gtest_main parser -pthread)

add_executable(StringUtilsTest StringUtilsTest.cpp)
target_link_libraries(StringUtilsTest gtest_main -pthread)

add_executable(LRUCacheTest LRUCacheTest.cpp)
target_link_libraries(LRUCacheTest gtest_main -pthread)

add_executable(FileTest FileTest.cpp)
target_link_libraries(FileTest gtest_main -pthread)

add_executable(Simple8bTest Simple8bTest.cpp)
target_link_libraries(Simple8bTest gtest_main -pthread)

add_executable(VocabularyTest VocabularyTest.cpp)
target_link_libraries(VocabularyTest gtest_main index -pthread)

add_executable(ExternalVocabularyTest ExternalVocabularyTest.cpp)
target_link_libraries(ExternalVocabularyTest gtest_main index -pthread)

add_executable(TsvParserTest TsvParserTest.cpp)
target_link_libraries(TsvParserTest gtest_main parser -pthread)

add_executable(NTriplesParserTest NTriplesParserTest.cpp)
target_link_libraries(NTriplesParserTest gtest_main parser -pthread)

add_executable(ContextFileParserTest ContextFileParserTest.cpp)
target_link_libraries(ContextFileParserTest gtest_main parser -pthread)

add_executable(IndexMetaDataTest IndexMetaDataTest.cpp)
target_link_libraries(IndexMetaDataTest gtest_main index -pthread)

add_executable(IndexTest IndexTest.cpp)
target_link_libraries(IndexTest gtest_main index -pthread)

add_executable(FTSAlgorithmsTest FTSAlgorithmsTest.cpp)
target_link_libraries(FTSAlgorithmsTest gtest_main index -pthread)

add_executable(EngineTest EngineTest.cpp)
target_link_libraries(EngineTest gtest_main engine -pthread)

add_executable(QueryPlannerTest QueryPlannerTest.cpp)
target_link_libraries(QueryPlannerTest gtest_main engine -pthread)

add_executable(ConversionsTest ConversionsTest.cpp)
target_link_libraries(ConversionsTest gtest_main engine -pthread)

add_executable(SparsehashTest SparsehashTest.cpp)
target_link_libraries(SparsehashTest gtest_main -pthread)

add_executable(GroupByTest GroupByTest.cpp)
target_link_libraries(GroupByTest gtest_main engine -pthread)

add_library(tests
            SparqlParserTest
            StringUtilsTest
            LRUCacheTest
            Simple8bTest
            FileTest
            VocabularyTest
            TsvParserTest
            ContextFileParserTest
            IndexMetaDataTest
            IndexTest
            EngineTest
            FTSAlgorithmsTest
            QueryPlannerTest
            ConversionsTest
            SparsehashTest
            GroupByTest
            )
