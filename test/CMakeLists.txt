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

# Usage: `linkAndDiscoverTestSerial(basename, [additionalLibraries...]`
# Similar to `linkAndDiscoverTestSerial` but also requires that the test is run serially
# (without any of the other test cases running in parallel). This can be
# required e.g. if several tests cases write to the same file.
function(linkAndDiscoverTestSerial basename)
    linkTest(${basename} ${ARGN})
    gtest_discover_tests(${basename} ${basename} PROPERTIES RUN_SERIAL
            TRUE)
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

# Usage: `addAndLinkTestSerial(basename, [additionalLibraries...]`
# Similar to `addAndLinkTest` but also requires that the test is run serially
# (without any of the other test cases running in parallel). This can be
# required e.g. if several tests cases write to the same file.
function(addLinkAndDiscoverTestSerial basename)
    addTest(${basename})
    linkAndDiscoverTestSerial(${basename} ${ARGN})
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

addLinkAndDiscoverTest(CacheTest absl::flat_hash_map)

addLinkAndDiscoverTest(ConcurrentCacheTest absl::flat_hash_map)

# This test also seems to use the same filenames and should be fixed.
addLinkAndDiscoverTestSerial(FileTest absl::strings)

addLinkAndDiscoverTest(Simple8bTest)

addLinkAndDiscoverTest(ContextFileParserTest parser ${ICU_LIBRARIES})

addLinkAndDiscoverTest(IndexMetaDataTest index)

# Stxxl currently always uses a file ./-stxxl.disk for all indices, which
# makes it impossible to run the test cases for the Index class in parallel.
# TODO<qup42, joka921> fix this
addLinkAndDiscoverTestSerial(IndexTest index)

addLinkAndDiscoverTest(FTSAlgorithmsTest index)

addLinkAndDiscoverTest(EngineTest engine)

addLinkAndDiscoverTest(QueryPlannerTest engine)

addLinkAndDiscoverTest(ConversionsTest util absl::strings)

addLinkAndDiscoverTest(HashMapTest absl::flat_hash_map)

addLinkAndDiscoverTest(HashSetTest absl::flat_hash_set)

addLinkAndDiscoverTestSerial(GroupByTest engine)

addLinkAndDiscoverTest(VocabularyGeneratorTest index)

addLinkAndDiscoverTest(HasPredicateScanTest engine)

addLinkAndDiscoverTest(MmapVectorTest absl::strings)

# BufferedVectorTest also uses conflicting filenames.
addLinkAndDiscoverTestSerial(BufferedVectorTest absl::strings)

addLinkAndDiscoverTest(UnionTest engine)

add_executable(TokenTest TokenTest.cpp TokenTestCtreHelper.cpp)
linkAndDiscoverTest(TokenTest parser re2 util ${ICU_LIBRARIES})

addLinkAndDiscoverTest(TurtleParserTest parser absl::flat_hash_map re2)

addLinkAndDiscoverTest(MultiColumnJoinTest engine)

addLinkAndDiscoverTest(IdTableTest absl::strings)

addLinkAndDiscoverTest(TransitivePathTest engine)

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

addLinkAndDiscoverTest(SparqlAntlrParserTest parser sparqlExpressions engine)

# The SerializerTest uses temporary files. The tests fail when multiple test
# cases are run in parallel. This should be fixed by using distinct filenames
# for each test case.
# TODO<qup42, joka921> fix this
addLinkAndDiscoverTestSerial(SerializerTest absl::flat_hash_map)

addLinkAndDiscoverTest(ParametersTest absl::flat_hash_map)

addLinkAndDiscoverTest(ZstdCompressionTest absl::flat_hash_map zstd ${cmake_thread_libs_init})

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

addLinkAndDiscoverTest(ContentEncodingHelperTest absl::strings http)

addLinkAndDiscoverTest(VocabularyInMemoryTest vocabulary)

addLinkAndDiscoverTest(CompressedVocabularyTest vocabulary)

addLinkAndDiscoverTest(UnicodeVocabularyTest vocabulary ${ICU_LIBRARIES})

addLinkAndDiscoverTest(CombinedVocabularyTest vocabulary)

addLinkAndDiscoverTest(PrefixCompressorTest)

addLinkAndDiscoverTest(MilestoneIdTest absl::strings)

# The VocabularyOnDisk uses mmap which fails when the tests are run in
# parallel. This should be fixed by using distinct filenames for each test case.
# TODO<qup42, joka921> fix this
addLinkAndDiscoverTest(VocabularyOnDiskTest index)

addLinkAndDiscoverTest(VocabularyTest index absl::strings)

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

addLinkAndDiscoverTest(TripleObjectTest absl::strings parser)

addLinkAndDiscoverTest(ValueIdTest absl::flat_hash_set)

addLinkAndDiscoverTest(LambdaHelpersTest)

addLinkAndDiscoverTest(ParseExceptionTest parser engine)

addLinkAndDiscoverTest(TransparentFunctorsTest)

addLinkAndDiscoverTest(SelectClauseTest parser engine)

addLinkAndDiscoverTestSerial(RelationalExpressionTest sparqlExpressions index engine)

addLinkAndDiscoverTest(CheckUsePatternTrickTest parser engine)

addLinkAndDiscoverTestSerial(RegexExpressionTest sparqlExpressions index engine)

addLinkAndDiscoverTest(LocalVocabTest engine)

addLinkAndDiscoverTest(HttpTest boost_iostreams http)

addLinkAndDiscoverTest(CallFixedSizeTest)

addLinkAndDiscoverTest(ConstexprUtilsTest)

addLinkAndDiscoverTest(ResetWhenMovedTest)
