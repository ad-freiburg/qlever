add_executable(SparqlParserTest SparqlParserTest.cpp)
add_test(SparqlParserTest SparqlParserTest)
target_link_libraries(SparqlParserTest gtest_main parser sparqlExpressions ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})

add_executable(StringUtilsTest StringUtilsTest.cpp)
add_test(StringUtilsTest StringUtilsTest)
target_link_libraries(StringUtilsTest gtest_main ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})

add_executable(LRUCacheTest CacheTest.cpp)
add_test(LRUCacheTest LRUCacheTest)
target_link_libraries(LRUCacheTest gtest_main ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_map)

add_executable(ConcurrentCacheTest ConcurrentCacheTest.cpp)
add_test(ConcurrentCacheTest ConcurrentCacheTest)
target_link_libraries(ConcurrentCacheTest gtest_main ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_map)


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
target_link_libraries(ConversionsTest gtest_main  ${CMAKE_THREAD_LIBS_INIT})

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

add_executable(TokenTest TokenTest.cpp TokenTestCtreHelper.cpp)
add_test(TokenTest TokenTest)
target_link_libraries(TokenTest parser re2 gtest_main -pthread ${ICU_LIBRARIES})

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
target_link_libraries(SparqlLexerTest parser gtest_main ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})

set(TEST_COMPILE_TIME_REGEXES OFF CACHE BOOL "Include the tests for the Utf8Regex.")
if (TEST_COMPILE_TIME_REGEXES)
  add_executable(Utf8RegexTest Utf8RegexTest.cpp)
  add_test(Utf8RegexTest Utf8RegexTest)
  target_link_libraries(Utf8RegexTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
endif (TEST_COMPILE_TIME_REGEXES)

add_executable(BatchedPipelineTest BatchedPipelineTest.cpp)
add_test(BatchedPipelineTest BatchedPipelineTest)
target_link_libraries(BatchedPipelineTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

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

add_executable(MinusTest MinusTest.cpp)
add_test(MinusTest MinusTest)
target_link_libraries(MinusTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(SortPerformanceEstimatorTest SortPerformanceEstimatorTest.cpp)
target_link_libraries(SortPerformanceEstimatorTest gtest_main SortPerformanceEstimator ${CMAKE_THREAD_LIBS_INIT})
# this test runs for quite some time and might have spurious failures! If this is undesired, comment it out
#add_test(SortPerformanceEstimatorTest SortPerformanceEstimatorTest)

add_executable(SparqlAntlrParserTest SparqlAntlrParserTest.cpp)
target_link_libraries(SparqlAntlrParserTest gtest_main parser sparqlExpressions ${CMAKE_THREAD_LIBS_INIT})
add_test(SparqlAntlrParserTest SparqlAntlrParserTest)

add_executable(SerializerTest SerializerTest.cpp)
target_link_libraries(SerializerTest gtest_main absl::flat_hash_map ${CMAKE_THREAD_LIBS_INIT})
add_test(SerializerTest SerializerTest)

add_executable(ParametersTest ParametersTest.cpp)
add_test(ParametersTest ParametersTest)
target_link_libraries(ParametersTest gtest_main absl::flat_hash_map  ${CMAKE_THREAD_LIBS_INIT})


add_executable(ZstdCompressionTest ZstdCompressionTest.cpp)
target_link_libraries(ZstdCompressionTest gtest_main absl::flat_hash_map zstd ${cmake_thread_libs_init})
add_test(ZstdCompressionTest ZstdCompressionTest )

add_executable(TaskQueueTest TaskQueueTest.cpp)
target_link_libraries(TaskQueueTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
add_test(TaskQueueTest TaskQueueTest)


add_executable(SetOfIntervalsTest SetOfIntervalsTest.cpp)
add_test(SetOfIntervalsTest SetOfIntervalsTest)
target_link_libraries(SetOfIntervalsTest gtest_main sparqlExpressions ${CMAKE_THREAD_LIBS_INIT})

add_executable(TypeTraitsTest TypeTraitsTest.cpp)
add_test(TypeTraitsTest TypeTraitsTest)
target_link_libraries(TypeTraitsTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(SparqlExpressionTest SparqlExpressionTest.cpp)
add_test(SparqlExpressionTest SparqlExpressionTest)
target_link_libraries(SparqlExpressionTest gtest_main sparqlExpressions index ${CMAKE_THREAD_LIBS_INIT})

add_executable(OStringStreamTest OStringStreamTest.cpp)
add_test(OStringStreamTest OStringStreamTest)
target_link_libraries(OStringStreamTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(StreamableBodyTest StreamableBodyTest.cpp)
add_test(StreamableBodyTest StreamableBodyTest)
target_link_libraries(StreamableBodyTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(StreamableGeneratorTest StreamableGeneratorTest.cpp)
add_test(StreamableGeneratorTest StreamableGeneratorTest)
target_link_libraries(StreamableGeneratorTest gtest_main ${CMAKE_THREAD_LIBS_INIT})

add_executable(AcceptHeaderTest AcceptHeaderTest.cpp)
target_link_libraries(AcceptHeaderTest gtest_main mediaTypes httpParser ${CMAKE_THREAD_LIBS_INIT})
add_test(AcceptHeaderTest AcceptHeaderTest)
