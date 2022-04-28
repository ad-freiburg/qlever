include(GoogleTest)

# Link binary ${basename} against `gmock_main`, the threading library,
# and all libraries that are specified as additional arguments.
function (linkTest basename)
    target_link_libraries(${basename} ${ARGN} gmock_main ${CMAKE_THREAD_LIBS_INIT})
endfunction()

# Add the exectutable ${basename} that is compiled from the source file
# "${basename}".cpp
function (addTest basename)
    add_executable(${basename} "${basename}.cpp")
endfunction()

# Usage: `linkAndDiscoverTest(basename, [additionalLibraries...]`
# Link the executable `basename` against `gmock_main`,threading library,
# and all `additionLibraries` which are passed as arguments.
# Then run `gtest_discover_tests` to add the tests cases from the executable.
# Typically you should use `addAndLinkTest` (below) but this function can be used,
# if a test binary requires multiple sources
function(linkAndDiscoverTest basename)
    linkTest(${basename} ${ARGN})
    gtest_discover_tests(${basename} ${basename})
endfunction()

# Usage: `addAndLinkTest(basename, [additionalLibraries...]`
# Add a GTest/GMock test case that is called `basename` and compiled from a file called
# `basename.cpp`. All tests are linked against `gmock_main` and the threading library.
# additional libraries against which the test case has to be linked can be specified as
# additional arguments after the `basename`
function(addLinkAndDiscoverTest basename)
    addTest(${basename})
    linkAndDiscoverTest(${basename} ${ARGN})
endfunction()

# Only compile and link the test, but do not run it.
# Usage: Same as for the two functions above.
function(addAndLinkTest basename)
    addTest(${basename})
    linkTest(${basename} ${ARGN})
endfunction()

addLinkAndDiscoverTest(SparqlParserTest parser sparqlExpressions ${ICU_LIBRARIES})

addLinkAndDiscoverTest(StringUtilsTest ${ICU_LIBRARIES})

addLinkAndDiscoverTest(CacheTest absl::flat_hash_map)

addLinkAndDiscoverTest(ConcurrentCacheTest absl::flat_hash_map)

addLinkAndDiscoverTest(FileTest)

addLinkAndDiscoverTest(Simple8bTest)

addLinkAndDiscoverTest(ContextFileParserTest parser ${ICU_LIBRARIES})

addLinkAndDiscoverTest(IndexMetaDataTest index)

addLinkAndDiscoverTest(IndexTest index)

addLinkAndDiscoverTest(FTSAlgorithmsTest index)

addLinkAndDiscoverTest(EngineTest engine)

addLinkAndDiscoverTest(QueryPlannerTest engine)

addLinkAndDiscoverTest(ConversionsTest absl::strings)

addLinkAndDiscoverTest(HashMapTest absl::flat_hash_map)

addLinkAndDiscoverTest(HashSetTest absl::flat_hash_set)

addLinkAndDiscoverTest(GroupByTest engine)

addLinkAndDiscoverTest(VocabularyGeneratorTest index)

addLinkAndDiscoverTest(HasPredicateScanTest engine)

addLinkAndDiscoverTest(MmapVectorTest)

addLinkAndDiscoverTest(BufferedVectorTest)

addLinkAndDiscoverTest(UnionTest engine)

add_executable(TokenTest TokenTest.cpp TokenTestCtreHelper.cpp)
linkAndDiscoverTest(TokenTest parser re2 ${ICU_LIBRARIES})

addLinkAndDiscoverTest(TurtleParserTest parser absl::flat_hash_map re2)

addLinkAndDiscoverTest(MultiColumnJoinTest engine)

addLinkAndDiscoverTest(IdTableTest)

addLinkAndDiscoverTest(TransitivePathTest engine)

addLinkAndDiscoverTest(SparqlLexerTest parser ${ICU_LIBRARIES})

addLinkAndDiscoverTest(BatchedPipelineTest)

addLinkAndDiscoverTest(TupleHelpersTest)

addLinkAndDiscoverTest(StringSortComparatorTest ${ICU_LIBRARIES})

addLinkAndDiscoverTest(PriorityQueueTest absl::flat_hash_map ${ICU_LIBRARIES})

addLinkAndDiscoverTest(SynchronizedTest)

addLinkAndDiscoverTest(LimitedAllocatorTest)

addLinkAndDiscoverTest(MinusTest engine)

# this test runs for quite some time and might have spurious failures!
# Therefore it is compiled, but not run. If you want to run it,
# change the following two lines.
addAndLinkTest(SortPerformanceEstimatorTest SortPerformanceEstimator)
#addLinkAndDiscoverTest(SortPerformanceEstimatorTest SortPerformanceEstimator)

addLinkAndDiscoverTest(SparqlAntlrParserTest parser sparqlExpressions)

addLinkAndDiscoverTest(SerializerTest absl::flat_hash_map)

addLinkAndDiscoverTest(ParametersTest absl::flat_hash_map)

addLinkAndDiscoverTest(ZstdCompressionTest absl::flat_hash_map zstd ${cmake_thread_libs_init})

addLinkAndDiscoverTest(TaskQueueTest)

addLinkAndDiscoverTest(SetOfIntervalsTest sparqlExpressions)

addLinkAndDiscoverTest(TypeTraitsTest)

addLinkAndDiscoverTest(SparqlExpressionTest sparqlExpressions index)

addLinkAndDiscoverTest(StreamableBodyTest)

addLinkAndDiscoverTest(StreamableGeneratorTest)

addLinkAndDiscoverTest(AcceptHeaderTest mediaTypes httpParser)

addLinkAndDiscoverTest(RdfEscapingTest parser)

addLinkAndDiscoverTest(CompactStringVectorTest)

addLinkAndDiscoverTest(SparqlDataTypesTest engine)

addLinkAndDiscoverTest(ContentEncodingHelperTest absl::strings)

addLinkAndDiscoverTest(VocabularyInMemoryTest vocabulary)

addLinkAndDiscoverTest(CompressedVocabularyTest vocabulary)

addLinkAndDiscoverTest(UnicodeVocabularyTest vocabulary ${ICU_LIBRARIES})

addLinkAndDiscoverTest(CombinedVocabularyTest vocabulary)

addLinkAndDiscoverTest(PrefixCompressorTest)

addLinkAndDiscoverTest(MilestoneIdTest absl::strings)

addLinkAndDiscoverTest(VocabularyOnDiskTest index)

addLinkAndDiscoverTest(VocabularyTest index)

addLinkAndDiscoverTest(IteratorTest)

addLinkAndDiscoverTest(PatternCreatorTest index)

addLinkAndDiscoverTest(BackgroundStxxlSorterTest ${STXXL_LIBRARIES})

addLinkAndDiscoverTest(ViewsTest)

addLinkAndDiscoverTest(ForwardTest)

addLinkAndDiscoverTest(CompressorStreamTest engine)

addLinkAndDiscoverTest(AsyncStreamTest)

addLinkAndDiscoverTest(TriplesViewTest)

addLinkAndDiscoverTest(BitUtilsTest)

addLinkAndDiscoverTest(NBitIntegerTest)

addLinkAndDiscoverTest(GeoSparqlHelpersTest util)

addLinkAndDiscoverTest(DateTest)

addLinkAndDiscoverTest(TripleObjectTest absl::strings)

addLinkAndDiscoverTest(ValueIdTest absl::flat_hash_set)

addLinkAndDiscoverTest(ValueIdComparatorsTest)
