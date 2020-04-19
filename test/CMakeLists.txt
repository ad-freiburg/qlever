add_executable(SparqlParserTest SparqlParserTest.cpp)
add_test(SparqlParserTest SparqlParserTest)
target_link_libraries(SparqlParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})

add_executable(StringUtilsTest StringUtilsTest.cpp)
add_test(StringUtilsTest StringUtilsTest)
target_link_libraries(StringUtilsTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(LRUCacheTest CacheTest.cpp)
add_test(LRUCacheTest LRUCacheTest)
target_link_libraries(LRUCacheTest gtest_main ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_map)

add_executable(FileTest FileTest.cpp)
add_test(FileTest FileTest)
target_link_libraries(FileTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(Simple8bTest Simple8bTest.cpp)
add_test(Simple8bTest Simple8bTest)
target_link_libraries(Simple8bTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(VocabularyTest VocabularyTest.cpp)
add_test(VocabularyTest VocabularyTest)
target_link_libraries(VocabularyTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(ExternalVocabularyTest ExternalVocabularyTest.cpp)
add_test(ExternalVocabularyTest ExternalVocabularyTest)
target_link_libraries(ExternalVocabularyTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(TsvParserTest TsvParserTest.cpp)
add_test(TsvParserTest TsvParserTest)
target_link_libraries(TsvParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})

add_executable(NTriplesParserTest NTriplesParserTest.cpp)
add_test(NTriplesParserTest NTriplesParserTest)
target_link_libraries(NTriplesParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})

add_executable(ContextFileParserTest ContextFileParserTest.cpp)
add_test(ContextFileParserTest ContextFileParserTest)
target_link_libraries(ContextFileParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})

add_executable(IndexMetaDataTest IndexMetaDataTest.cpp)
add_test(IndexMetaDataTest IndexMetaDataTest)
target_link_libraries(IndexMetaDataTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(IndexTest IndexTest.cpp)
add_test(IndexTest IndexTest)
target_link_libraries(IndexTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(FTSAlgorithmsTest FTSAlgorithmsTest.cpp)
add_test(FTSAlgorithmsTest FTSAlgorithmsTest)
target_link_libraries(FTSAlgorithmsTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(EngineTest EngineTest.cpp)
add_test(EngineTest EngineTest)
target_link_libraries(EngineTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(QueryPlannerTest QueryPlannerTest.cpp)
add_test(QueryPlannerTest QueryPlannerTest)
target_link_libraries(QueryPlannerTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(ConversionsTest ConversionsTest.cpp)
add_test(ConversionsTest ConversionsTest)
target_link_libraries(ConversionsTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(HashMapTest HashMapTest.cpp)
add_test(HashMapTest HashMapTest)
target_link_libraries(HashMapTest gtest_main ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_map)

add_executable(HashSetTest HashSetTest.cpp)
add_test(HashSetTest HashSetTest)
target_link_libraries(HashSetTest gtest_main ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_set)

add_executable(GroupByTest GroupByTest.cpp)
add_test(GroupByTest GroupByTest)
target_link_libraries(GroupByTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(VocabularyGeneratorTest VocabularyGeneratorTest.cpp)
add_test(VocabularyGeneratorTest VocabularyGeneratorTest)
target_link_libraries(VocabularyGeneratorTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})

add_executable(HasPredicateScanTest HasPredicateScanTest.cpp)
add_test(HasPredicateScanTest HasPredicateScanTest)
target_link_libraries(HasPredicateScanTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(MmapVectorTest MmapVectorTest.cpp)
add_test(MmapVectorTest MmapVectorTest)
target_link_libraries(MmapVectorTest gtest_main -pthread)

add_executable(BufferedVectorTest BufferedVectorTest.cpp)
add_test(BufferedVectorTest BufferedVectorTest)
target_link_libraries(BufferedVectorTest gtest_main -pthread)

add_executable(UnionTest UnionTest.cpp)
add_test(UnionTest UnionTest)
target_link_libraries(UnionTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(TokenTest TokenTest.cpp)
add_test(TokenTest TokenTest)
target_link_libraries(TokenTest parser re2 gtest_main -pthread)

add_executable(TurtleParserTest TurtleParserTest.cpp)
add_test(TurtleParserTest TurtleParserTest)
target_link_libraries(TurtleParserTest parser absl::flat_hash_map re2 gtest_main -pthread)

add_executable(MultiColumnJoinTest MultiColumnJoinTest.cpp)
add_test(MultiColumnJoinTest MultiColumnJoinTest)
target_link_libraries(MultiColumnJoinTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(IdTableTest IdTableTest.cpp)
add_test(IdTableTest IdTableTest)
target_link_libraries(IdTableTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(TransitivePathTest TransitivePathTest.cpp)
add_test(TransitivePathTest TransitivePathTest)
target_link_libraries(TransitivePathTest engine gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(SparqlLexerTest SparqlLexerTest.cpp)
add_test(SparqlLexerTest SparqlLexerTest)
target_link_libraries(SparqlLexerTest parser gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(Utf8RegexTest Utf8RegexTest.cpp)
add_test(Utf8RegexTest Utf8RegexTest)
target_link_libraries(Utf8RegexTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(BatchedPipelineTest BatchedPipelineTest.cpp)
add_test(BatchedPipelineTest BatchedPipelineTest)
target_link_libraries(BatchedPipelineTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})

add_executable(TupleHelpersTest TupleHelpersTest.cpp)
add_test(TupleHelpersTest TupleHelpersTest)
target_link_libraries(TupleHelpersTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(StringSortComparatorTest StringSortComparatorTest.cpp)
add_test(StringSortComparatorTest StringSortComparatorTest)
target_link_libraries(StringSortComparatorTest gtest_main ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})

add_executable(PriorityQueueTest PriorityQueueTest.cpp)
add_test(PriorityQueueTest PriorityQueueTest)
target_link_libraries(PriorityQueueTest gtest_main absl::flat_hash_map ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})

add_executable(SynchronizedTest SynchronizedTest.cpp)
add_test(SynchronizedTest SynchronizedTest)
target_link_libraries(SynchronizedTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(BindTest BindTest.cpp)
add_test(BindTest BindTest)
target_link_libraries(BindTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(LimitedAllocatorTest LimitedAllocatorTest.cpp)
add_test(LimitedAllocatorTest LimitedAllocatorTest)
target_link_libraries(LimitedAllocatorTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
