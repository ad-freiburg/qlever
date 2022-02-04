include(GoogleTest)

add_executable(SparqlParserTest SparqlParserTest.cpp)
target_link_libraries(SparqlParserTest gtest_main parser sparqlExpressions ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})
gtest_discover_tests(SparqlParserTest)

add_executable(StringUtilsTest StringUtilsTest.cpp)
target_link_libraries(StringUtilsTest gtest_main ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})
gtest_discover_tests(StringUtilsTest)

add_executable(LRUCacheTest CacheTest.cpp)
target_link_libraries(LRUCacheTest gtest_main ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_map)
gtest_discover_tests(LRUCacheTest)

add_executable(ConcurrentCacheTest ConcurrentCacheTest.cpp)
target_link_libraries(ConcurrentCacheTest gtest_main ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_map)
gtest_discover_tests(ConcurrentCacheTest)

add_executable(FileTest FileTest.cpp)
target_link_libraries(FileTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(FileTest)

add_executable(Simple8bTest Simple8bTest.cpp)
target_link_libraries(Simple8bTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(Simple8bTest)

add_executable(VocabularyTest VocabularyTest.cpp)
target_link_libraries(VocabularyTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(VocabularyTest)

add_executable(ExternalVocabularyTest ExternalVocabularyTest.cpp)
target_link_libraries(ExternalVocabularyTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(ExternalVocabularyTest)

add_executable(TsvParserTest TsvParserTest.cpp)
target_link_libraries(TsvParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(TsvParserTest)

add_executable(ContextFileParserTest ContextFileParserTest.cpp)
target_link_libraries(ContextFileParserTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})
gtest_discover_tests(ContextFileParserTest)

add_executable(IndexMetaDataTest IndexMetaDataTest.cpp)
target_link_libraries(IndexMetaDataTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(IndexMetaDataTest)

add_executable(IndexTest IndexTest.cpp)
target_link_libraries(IndexTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(IndexTest)

add_executable(FTSAlgorithmsTest FTSAlgorithmsTest.cpp)
target_link_libraries(FTSAlgorithmsTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(FTSAlgorithmsTest)

add_executable(EngineTest EngineTest.cpp)
target_link_libraries(EngineTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(EngineTest)

add_executable(QueryPlannerTest QueryPlannerTest.cpp)
target_link_libraries(QueryPlannerTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(QueryPlannerTest)

add_executable(ConversionsTest ConversionsTest.cpp)
target_link_libraries(ConversionsTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(ConversionsTest)

add_executable(HashMapTest HashMapTest.cpp)
target_link_libraries(HashMapTest gtest_main ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_map)
gtest_discover_tests(HashMapTest)

add_executable(HashSetTest HashSetTest.cpp)
target_link_libraries(HashSetTest gtest_main ${CMAKE_THREAD_LIBS_INIT} absl::flat_hash_set)
gtest_discover_tests(HashSetTest)

add_executable(GroupByTest GroupByTest.cpp)
target_link_libraries(GroupByTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(GroupByTest)

add_executable(VocabularyGeneratorTest VocabularyGeneratorTest.cpp)
target_link_libraries(VocabularyGeneratorTest gtest_main index ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(VocabularyGeneratorTest)

add_executable(HasPredicateScanTest HasPredicateScanTest.cpp)
target_link_libraries(HasPredicateScanTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(HasPredicateScanTest)

add_executable(MmapVectorTest MmapVectorTest.cpp)
target_link_libraries(MmapVectorTest gtest_main -pthread)
gtest_discover_tests(MmapVectorTest)

add_executable(BufferedVectorTest BufferedVectorTest.cpp)
target_link_libraries(BufferedVectorTest gtest_main -pthread)
gtest_discover_tests(BufferedVectorTest)

add_executable(UnionTest UnionTest.cpp)
target_link_libraries(UnionTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(UnionTest)

add_executable(TokenTest TokenTest.cpp TokenTestCtreHelper.cpp)
target_link_libraries(TokenTest parser re2 gtest_main -pthread ${ICU_LIBRARIES})
gtest_discover_tests(TokenTest)

add_executable(TurtleParserTest TurtleParserTest.cpp)
target_link_libraries(TurtleParserTest parser absl::flat_hash_map re2 gtest_main -pthread)
gtest_discover_tests(TurtleParserTest)

add_executable(MultiColumnJoinTest MultiColumnJoinTest.cpp)
target_link_libraries(MultiColumnJoinTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(MultiColumnJoinTest)

add_executable(IdTableTest IdTableTest.cpp)
target_link_libraries(IdTableTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(IdTableTest)

add_executable(TransitivePathTest TransitivePathTest.cpp)
target_link_libraries(TransitivePathTest engine gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(TransitivePathTest)

add_executable(SparqlLexerTest SparqlLexerTest.cpp)
target_link_libraries(SparqlLexerTest parser gtest_main ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})
gtest_discover_tests(SparqlLexerTest)

add_executable(BatchedPipelineTest BatchedPipelineTest.cpp)
target_link_libraries(BatchedPipelineTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(BatchedPipelineTest)

add_executable(TupleHelpersTest TupleHelpersTest.cpp)
target_link_libraries(TupleHelpersTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(TupleHelpersTest)

add_executable(StringSortComparatorTest StringSortComparatorTest.cpp)
target_link_libraries(StringSortComparatorTest gtest_main ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})
gtest_discover_tests(StringSortComparatorTest)

add_executable(PriorityQueueTest PriorityQueueTest.cpp)
target_link_libraries(PriorityQueueTest gtest_main absl::flat_hash_map ${CMAKE_THREAD_LIBS_INIT} ${ICU_LIBRARIES})
gtest_discover_tests(PriorityQueueTest)

add_executable(SynchronizedTest SynchronizedTest.cpp)
target_link_libraries(SynchronizedTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(SynchronizedTest)

add_executable(LimitedAllocatorTest LimitedAllocatorTest.cpp)
target_link_libraries(LimitedAllocatorTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(LimitedAllocatorTest)

add_executable(MinusTest MinusTest.cpp)
target_link_libraries(MinusTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(MinusTest)

add_executable(SortPerformanceEstimatorTest SortPerformanceEstimatorTest.cpp)
target_link_libraries(SortPerformanceEstimatorTest gtest_main SortPerformanceEstimator ${CMAKE_THREAD_LIBS_INIT})
# this test runs for quite some time and might have spurious failures! If this is undesired, comment it out
#gtest_discover_tests(SortPerformanceEstimatorTest)

add_executable(SparqlAntlrParserTest SparqlAntlrParserTest.cpp)
target_link_libraries(SparqlAntlrParserTest gmock_main parser sparqlExpressions ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(SparqlAntlrParserTest)

add_executable(SerializerTest SerializerTest.cpp)
target_link_libraries(SerializerTest gtest_main absl::flat_hash_map ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(SerializerTest)

add_executable(ParametersTest ParametersTest.cpp)
target_link_libraries(ParametersTest gtest_main absl::flat_hash_map ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(ParametersTest)

add_executable(ZstdCompressionTest ZstdCompressionTest.cpp)
target_link_libraries(ZstdCompressionTest gtest_main absl::flat_hash_map zstd ${cmake_thread_libs_init})
gtest_discover_tests(ZstdCompressionTest)

add_executable(TaskQueueTest TaskQueueTest.cpp)
target_link_libraries(TaskQueueTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(TaskQueueTest)

add_executable(SetOfIntervalsTest SetOfIntervalsTest.cpp)
target_link_libraries(SetOfIntervalsTest gtest_main sparqlExpressions ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(SetOfIntervalsTest)

add_executable(TypeTraitsTest TypeTraitsTest.cpp)
target_link_libraries(TypeTraitsTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(TypeTraitsTest)

add_executable(SparqlExpressionTest SparqlExpressionTest.cpp)
target_link_libraries(SparqlExpressionTest gtest_main sparqlExpressions index ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(SparqlExpressionTest)

add_executable(StreamableBodyTest StreamableBodyTest.cpp)
target_link_libraries(StreamableBodyTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(StreamableBodyTest)

add_executable(StreamableGeneratorTest StreamableGeneratorTest.cpp)
target_link_libraries(StreamableGeneratorTest gtest_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(StreamableGeneratorTest)

add_executable(AcceptHeaderTest AcceptHeaderTest.cpp)
target_link_libraries(AcceptHeaderTest gtest_main mediaTypes httpParser ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(AcceptHeaderTest)

add_executable(RdfEscapingTest RdfEscapingTest.cpp)
target_link_libraries(RdfEscapingTest gtest_main parser ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(RdfEscapingTest)

add_executable(CompactStringVectorTest CompactStringVectorTest.cpp)
target_link_libraries(CompactStringVectorTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(CompactStringVectorTest)

add_executable(SparqlDataTypesTest SparqlDataTypesTest.cpp)
target_link_libraries(SparqlDataTypesTest gmock_main engine ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(SparqlDataTypesTest)

add_executable(ContentEncodingHelperTest ContentEncodingHelperTest.cpp)
target_link_libraries(ContentEncodingHelperTest gtest_main ${CMAKE_THREAD_LIBS_INIT})
gtest_discover_tests(ContentEncodingHelperTest)
