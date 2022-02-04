include(GoogleTest)

# Usage: `addAndLinkTest(basename, [additionalLibraries...]`
# Add a GTest/GMock test case that is called `basename` and compiled from a file called
# `basename.cpp`. All tests are linked against `gmock_main` and the threading library.
# additional libraries against which the test case has to be linked can be specified as
# additional arguments after the `basename`
function(addAndLinkTest basename)
    add_executable(${basename} "${basename}.cpp")
    target_link_libraries(${basename} ${ARGN} gmock_main ${CMAKE_THREAD_LIBS_INIT})
    gtest_discover_tests(${basename} ${basename})
endfunction()

addAndLinkTest(SparqlParserTest parser sparqlExpressions ${ICU_LIBRARIES})

addAndLinkTest(StringUtilsTest ${ICU_LIBRARIES})

addAndLinkTest(CacheTest absl::flat_hash_map)

addAndLinkTest(ConcurrentCacheTest absl::flat_hash_map)

addAndLinkTest(FileTest)

addAndLinkTest(Simple8bTest)

addAndLinkTest(VocabularyTest index)

addAndLinkTest(ExternalVocabularyTest index)

addAndLinkTest(TsvParserTest parser)

addAndLinkTest(ContextFileParserTest parser ${ICU_LIBRARIES})

addAndLinkTest(IndexMetaDataTest index)

addAndLinkTest(IndexTest index)

addAndLinkTest(FTSAlgorithmsTest index)

addAndLinkTest(EngineTest engine)

addAndLinkTest(QueryPlannerTest engine)

addAndLinkTest(ConversionsTest )

addAndLinkTest(HashMapTest absl::flat_hash_map)

addAndLinkTest(HashSetTest absl::flat_hash_set)

addAndLinkTest(GroupByTest engine)

addAndLinkTest(VocabularyGeneratorTest index)

addAndLinkTest(HasPredicateScanTest engine)

addAndLinkTest(MmapVectorTest -pthread)

addAndLinkTest(BufferedVectorTest -pthread)

addAndLinkTest(UnionTest engine)

addAndLinkTest(TokenTest parser re2 -pthread ${ICU_LIBRARIES})

addAndLinkTest(TurtleParserTest parser absl::flat_hash_map re2 -pthread)

addAndLinkTest(MultiColumnJoinTest engine)

addAndLinkTest(IdTableTest)

addAndLinkTest(TransitivePathTest engine)

addAndLinkTest(SparqlLexerTest parser ${ICU_LIBRARIES})

addAndLinkTest(BatchedPipelineTest)

addAndLinkTest(TupleHelpersTest)

addAndLinkTest(StringSortComparatorTest ${ICU_LIBRARIES})

addAndLinkTest(PriorityQueueTest absl::flat_hash_map ${ICU_LIBRARIES})

addAndLinkTest(SynchronizedTest)

addAndLinkTest(LimitedAllocatorTest)

addAndLinkTest(MinusTest engine)

# this test runs for quite some time and might have spurious failures!
#If this is undesired, comment it out
#addAndLinkTest(SortPerformanceEstimatorTest SortPerformanceEstimator)

addAndLinkTest(SparqlAntlrParserTest parser sparqlExpressions)

addAndLinkTest(SerializerTest absl::flat_hash_map)

addAndLinkTest(ParametersTest absl::flat_hash_map )

addAndLinkTest(ZstdCompressionTest absl::flat_hash_map zstd ${cmake_thread_libs_init})

addAndLinkTest(TaskQueueTest)

addAndLinkTest(SetOfIntervalsTest sparqlExpressions)

addAndLinkTest(TypeTraitsTest)

addAndLinkTest(SparqlExpressionTest sparqlExpressions index)

addAndLinkTest(OStringStreamTest)

addAndLinkTest(StreamableBodyTest)

addAndLinkTest(StreamableGeneratorTest)

addAndLinkTest(AcceptHeaderTest mediaTypes httpParser)

addAndLinkTest(RdfEscapingTest parser)

addAndLinkTest(CompactStringVectorTest)

addAndLinkTest(SparqlDataTypesTest engine)
