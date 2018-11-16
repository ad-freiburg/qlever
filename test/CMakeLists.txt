################################
# NLOHNMANN-JSON
################################
# Header only, nothing to include
include_directories(../third_party/json/include/)

add_executable(SparqlParserTest SparqlParserTest.cpp)
target_link_libraries(SparqlParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})

add_executable(StringUtilsTest StringUtilsTest.cpp)
target_link_libraries(StringUtilsTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(LRUCacheTest LRUCacheTest.cpp)
target_link_libraries(LRUCacheTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(FileTest FileTest.cpp)
target_link_libraries(FileTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(Simple8bTest Simple8bTest.cpp)
target_link_libraries(Simple8bTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(VocabularyTest VocabularyTest.cpp)
target_link_libraries(VocabularyTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(ExternalVocabularyTest ExternalVocabularyTest.cpp)
target_link_libraries(ExternalVocabularyTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(TsvParserTest TsvParserTest.cpp)
target_link_libraries(TsvParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})

add_executable(NTriplesParserTest NTriplesParserTest.cpp)
target_link_libraries(NTriplesParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})

add_executable(ContextFileParserTest ContextFileParserTest.cpp)
target_link_libraries(ContextFileParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})

add_executable(IndexMetaDataTest IndexMetaDataTest.cpp)
target_link_libraries(IndexMetaDataTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(IndexTest IndexTest.cpp)
target_link_libraries(IndexTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(FTSAlgorithmsTest FTSAlgorithmsTest.cpp)
target_link_libraries(FTSAlgorithmsTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(EngineTest EngineTest.cpp)
target_link_libraries(EngineTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(QueryPlannerTest QueryPlannerTest.cpp)
target_link_libraries(QueryPlannerTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(ConversionsTest ConversionsTest.cpp)
target_link_libraries(ConversionsTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(HashMapTest HashMapTest.cpp)
target_link_libraries(HashMapTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(HashSetTest HashSetTest.cpp)
target_link_libraries(HashSetTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(GroupByTest GroupByTest.cpp)
target_link_libraries(GroupByTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(VocabularyGeneratorTest VocabularyGeneratorTest.cpp)
target_link_libraries(VocabularyGeneratorTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(HasPredicateScanTest HasPredicateScanTest.cpp)
target_link_libraries(HasPredicateScanTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(MmapVectorTest MmapVectorTest.cpp)
target_link_libraries(MmapVectorTest gtest_main -pthread)
