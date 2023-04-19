include(GoogleTest)

# Link binary ${basename} against `gmock_main`, the threading library,
# and all libraries that are specified as additional arguments.
function (linkTest basename)
    qlever_target_link_libraries(${basename} ${ARGN} gmock_main ${CMAKE_THREAD_LIBS_INIT})
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

# Usage: `linkAndDiscoverTestSerial(basename, [additionalLibraries...]`
# Similar to `linkAndDiscoverTestSerial` but also requires that the test is run serially
# (without any of the other test cases running in parallel). This can be
# required e.g. if several tests cases write to the same file.
function(linkAndDiscoverTestSerial basename)
    linkTest(${basename} ${ARGN})
    gtest_discover_tests(${basename} ${basename} PROPERTIES RUN_SERIAL
            TRUE)
endfunction()

if (SINGLE_TEST_BINARY)
    message(STATUS "All tests are linked into a single executable `QLeverAllUnitTestsMain`")
    add_executable(QLeverAllUnitTestsMain)
    qlever_target_link_libraries(QLeverAllUnitTestsMain gmock_main ${CMAKE_THREAD_LIBS_INIT})
    gtest_discover_tests(QLeverAllUnitTestsMain QLeverAllUnitTestsMain PROPERTIES RUN_SERIAL
            TRUE)
else()
    message(STATUS "The tests are split over multiple binaries")

endif()
# Usage: `addAndLinkTest(basename, [additionalLibraries...]`
# Add a GTest/GMock test case that is called `basename` and compiled from a file called
# `basename.cpp`. All tests are linked against `gmock_main` and the threading library.
# additional libraries against which the test case has to be linked can be specified as
# additional arguments after the `basename`
function(addLinkAndDiscoverTest basename)
  if (SINGLE_TEST_BINARY)
      target_sources(QLeverAllUnitTestsMain PUBLIC ${basename}.cpp)
      qlever_target_link_libraries(QLeverAllUnitTestsMain ${ARGN})
  else()
    addTest(${basename})
    linkAndDiscoverTest(${basename} ${ARGN})
  endif()

endfunction()

# Usage: `addAndLinkTestSerial(basename, [additionalLibraries...]`
# Similar to `addAndLinkTest` but also requires that the test is run serially
# (without any of the other test cases running in parallel). This can be
# required e.g. if several tests cases write to the same file.
function(addLinkAndDiscoverTestSerial basename)
  if (SINGLE_TEST_BINARY)
    target_sources(QLeverAllUnitTestsMain PUBLIC ${basename}.cpp)
    qlever_target_link_libraries(QLeverAllUnitTestsMain ${ARGN})
  else()
    addTest(${basename})
    linkAndDiscoverTestSerial(${basename} ${ARGN})
  endif()
endfunction()

# Only compile and link the test, but do not run it.
# Usage: Same as for the two functions above.
function(addAndLinkTest basename)
    addTest(${basename})
    linkTest(${basename} ${ARGN})
endfunction()

addLinkAndDiscoverTest(ValueIdComparatorsTest)

addLinkAndDiscoverTest(SparqlParserTest parser engine sparqlExpressions ${ICU_LIBRARIES})

addLinkAndDiscoverTest(StringUtilsTest ${ICU_LIBRARIES})

addLinkAndDiscoverTest(CacheTest)

addLinkAndDiscoverTest(ConcurrentCacheTest)

# This test also seems to use the same filenames and should be fixed.
addLinkAndDiscoverTestSerial(FileTest)

addLinkAndDiscoverTest(Simple8bTest)

addLinkAndDiscoverTest(ContextFileParserTest parser ${ICU_LIBRARIES})

addLinkAndDiscoverTest(IndexMetaDataTest index)

# Stxxl currently always uses a file ./-stxxl.disk for all indices, which
# makes it impossible to run the test cases for the Index class in parallel.
# TODO<qup42, joka921> fix this
addLinkAndDiscoverTestSerial(IndexTest index)

addLinkAndDiscoverTest(FTSAlgorithmsTest index)

addLinkAndDiscoverTest(EngineTest engine)

addLinkAndDiscoverTest(JoinTest engine)

addLinkAndDiscoverTest(QueryPlannerTest engine)

addLinkAndDiscoverTest(ConversionsTest util)

addLinkAndDiscoverTest(HashMapTest)

addLinkAndDiscoverTest(HashSetTest)

addLinkAndDiscoverTestSerial(GroupByTest engine)

addLinkAndDiscoverTest(VocabularyGeneratorTest index)

addLinkAndDiscoverTest(HasPredicateScanTest engine)

addLinkAndDiscoverTest(MmapVectorTest)

# BufferedVectorTest also uses conflicting filenames.
addLinkAndDiscoverTestSerial(BufferedVectorTest)

addLinkAndDiscoverTest(UnionTest engine)

if (SINGLE_TEST_BINARY)
    target_sources(QLeverAllUnitTestsMain PUBLIC TokenTest.cpp TokenTestCtreHelper.cpp)
    qlever_target_link_libraries(QLeverAllUnitTestsMain parser re2 util ${ICU_LIBRARIES})
else()
    add_executable(TokenTest TokenTest.cpp TokenTestCtreHelper.cpp)
    linkAndDiscoverTest(TokenTest parser re2 util ${ICU_LIBRARIES})
endif()

addLinkAndDiscoverTest(TurtleParserTest parser re2)

addLinkAndDiscoverTest(MultiColumnJoinTest engine)

addLinkAndDiscoverTest(IdTableTest)

addLinkAndDiscoverTest(TransitivePathTest engine)

addLinkAndDiscoverTest(BatchedPipelineTest)

addLinkAndDiscoverTest(TupleHelpersTest)

addLinkAndDiscoverTest(StringSortComparatorTest ${ICU_LIBRARIES})

addLinkAndDiscoverTest(PriorityQueueTest ${ICU_LIBRARIES})

addLinkAndDiscoverTest(SynchronizedTest)

addLinkAndDiscoverTest(AllocatorWithLimitTest)

addLinkAndDiscoverTest(MinusTest engine)

# this test runs for quite some time and might have spurious failures!
# Therefore it is compiled, but not run. If you want to run it,
# change the following two lines.
addAndLinkTest(SortPerformanceEstimatorTest SortPerformanceEstimator)
#addLinkAndDiscoverTest(SortPerformanceEstimatorTest SortPerformanceEstimator)

addLinkAndDiscoverTestSerial(SparqlAntlrParserTest parser sparqlExpressions engine)

# The SerializerTest uses temporary files. The tests fail when multiple test
# cases are run in parallel. This should be fixed by using distinct filenames
# for each test case.
# TODO<qup42, joka921> fix this
addLinkAndDiscoverTestSerial(SerializerTest)

addLinkAndDiscoverTest(ParametersTest)

addLinkAndDiscoverTest(ZstdCompressionTest zstd ${cmake_thread_libs_init})

addLinkAndDiscoverTest(TaskQueueTest)

addLinkAndDiscoverTest(SetOfIntervalsTest sparqlExpressions)

addLinkAndDiscoverTest(TypeTraitsTest)

addLinkAndDiscoverTestSerial(SparqlExpressionTest sparqlExpressions index engine)

addLinkAndDiscoverTest(StreamableBodyTest http)

addLinkAndDiscoverTest(StreamableGeneratorTest)

addLinkAndDiscoverTest(AcceptHeaderTest mediaTypes httpParser)

addLinkAndDiscoverTest(RdfEscapingTest parser)

addLinkAndDiscoverTest(CompactStringVectorTest)

addLinkAndDiscoverTest(SparqlDataTypesTest engine)

addLinkAndDiscoverTest(ContentEncodingHelperTest http)

addLinkAndDiscoverTest(VocabularyInMemoryTest vocabulary)

addLinkAndDiscoverTest(CompressedVocabularyTest vocabulary)

addLinkAndDiscoverTest(UnicodeVocabularyTest vocabulary ${ICU_LIBRARIES})

addLinkAndDiscoverTest(CombinedVocabularyTest vocabulary)

addLinkAndDiscoverTest(PrefixCompressorTest)

addLinkAndDiscoverTest(MilestoneIdTest)

# The VocabularyOnDisk uses mmap which fails when the tests are run in
# parallel. This should be fixed by using distinct filenames for each test case.
# TODO<qup42, joka921> fix this
addLinkAndDiscoverTest(VocabularyOnDiskTest index)

addLinkAndDiscoverTest(VocabularyTest index)

addLinkAndDiscoverTest(IteratorTest)

# Here we also seem to have race conditions on the tests
addLinkAndDiscoverTestSerial(PatternCreatorTest index)

# Stxxl currently always uses a file ./-stxxl.disk for all indices, which
# makes it impossible to run the test cases for the Index class in parallel.
# TODO<qup42, joka921> fix this
addLinkAndDiscoverTestSerial(BackgroundStxxlSorterTest ${STXXL_LIBRARIES})

addLinkAndDiscoverTest(ViewsTest)

addLinkAndDiscoverTest(ForwardTest)

addLinkAndDiscoverTest(CompressorStreamTest engine)

addLinkAndDiscoverTest(AsyncStreamTest)

addLinkAndDiscoverTest(TriplesViewTest)

addLinkAndDiscoverTest(BitUtilsTest)

addLinkAndDiscoverTest(NBitIntegerTest)

addLinkAndDiscoverTest(GeoSparqlHelpersTest util)

addLinkAndDiscoverTest(HttpUtilsTest util http)

addLinkAndDiscoverTest(DateTest)

addLinkAndDiscoverTest(TripleComponentTest parser)

addLinkAndDiscoverTest(ValueIdTest)

addLinkAndDiscoverTest(LambdaHelpersTest)

addLinkAndDiscoverTest(ParseExceptionTest parser engine)

addLinkAndDiscoverTest(TransparentFunctorsTest)

addLinkAndDiscoverTest(SelectClauseTest parser engine)

addLinkAndDiscoverTestSerial(RelationalExpressionTest parser sparqlExpressions index engine)

addLinkAndDiscoverTest(CheckUsePatternTrickTest parser engine)

addLinkAndDiscoverTestSerial(RegexExpressionTest parser sparqlExpressions index engine parser)

addLinkAndDiscoverTestSerial(LocalVocabTest engine)

addLinkAndDiscoverTestSerial(ValuesTest engine)

addLinkAndDiscoverTestSerial(ServiceTest engine)

addLinkAndDiscoverTest(HttpTest boost_iostreams http)

addLinkAndDiscoverTest(CallFixedSizeTest)

addLinkAndDiscoverTest(ConstexprUtilsTest)

addLinkAndDiscoverTest(ResetWhenMovedTest)

addLinkAndDiscoverTest(TimerTest)

addLinkAndDiscoverTest(AlgorithmTest)

addLinkAndDiscoverTest(CompressedRelationsTest index)

addLinkAndDiscoverTest(ExceptionTest)

addLinkAndDiscoverTestSerial(RandomExpressionTest index)

addLinkAndDiscoverTestSerial(SortTest engine)

addLinkAndDiscoverTestSerial(OrderByTest engine)

addLinkAndDiscoverTestSerial(ValuesForTestingTest index)

addLinkAndDiscoverTestSerial(ExportQueryExecutionTreeTest index engine parser)

addLinkAndDiscoverTestSerial(AggregateExpressionTest parser sparqlExpressions index engine)

addLinkAndDiscoverTest(OnDestructionDontThrowDuringStackUnwindingTest)

addLinkAndDiscoverTest(ExceptionHandlingTest)

addLinkAndDiscoverTest(SparqlExpressionTypesTest)

addLinkAndDiscoverTest(LimitOffsetClauseTest)

addLinkAndDiscoverTest(OperationTest engine)

addLinkAndDiscoverTest(RuntimeInformationTest engine)

addLinkAndDiscoverTest(VariableToColumnMapTest engine)

addLinkAndDiscoverTest(CopyableUniquePtrTest)

addLinkAndDiscoverTest(JsonCustomConverterForThirdPartyTest)

addLinkAndDiscoverTest(BenchmarkConfigurationTest benchmark)

addLinkAndDiscoverTest(BenchmarkMeasurementContainerTest benchmark)

addLinkAndDiscoverTest(FindUndefRangesTest)

addLinkAndDiscoverTest(AddCombinedRowToTableTest)
