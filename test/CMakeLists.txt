include(GoogleTest)

# Needed for creating the `testUil`-library.
add_subdirectory(util EXCLUDE_FROM_ALL)

# Link binary ${basename} against `gmock_main`, the threading library, the
# general test utilities and all libraries that are specified as additional
# arguments.
function(linkTest basename)
    qlever_target_link_libraries(${basename} ${ARGN} GTest::gtest GTest::gmock_main ${CMAKE_THREAD_LIBS_INIT} global)
endfunction()

# Add the executable ${basename} that is compiled from the source file
# "${basename}".cpp
function(addTest basename)
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
    gtest_discover_tests(${basename} ${basename} DISCOVERY_TIMEOUT 600 PROPERTIES LABELS "${basename}")
endfunction()

# Same as `linkAndDiscoverTest`, but marks the discovered tests as
# `RUN_SERIAL` so that ctest does not run them concurrently with other tests.
# Use this only for tests whose behaviour depends on wall-clock timing
# (e.g. `TimerTest`, `ProgressBarTest`), where concurrent load on the
# machine would otherwise make them flaky. Filename-clash parallelism
# issues should be fixed via per-test unique filenames instead.
function(linkAndDiscoverTestSerial basename)
    linkTest(${basename} ${ARGN})
    gtest_discover_tests(${basename} ${basename} DISCOVERY_TIMEOUT 600 PROPERTIES RUN_SERIAL
            TRUE LABELS "${basename}")
endfunction()

if (SINGLE_TEST_BINARY)
    message(STATUS "All tests are linked into a single executable `QLeverAllUnitTestsMain`")
    add_executable(QLeverAllUnitTestsMain)
    qlever_target_link_libraries(QLeverAllUnitTestsMain gtest gmock_main testUtil ${CMAKE_THREAD_LIBS_INIT})
    # The following line (as opposed to using `gtest_discover_tests`) has the effect that `ctest` will treat all the
    # unit tests as a single test case and will simply run the executable (using the main function from `gtest_main`.
    # This decreases the runtime overhead per unit tests in particular for sanitizer builds.
    add_test(NAME QLeverAllUnitTests COMMAND $<TARGET_FILE:QLeverAllUnitTestsMain>)
else ()
    message(STATUS "The tests are split over multiple binaries")

endif ()

# The shared implementation of the `addLinkAndDiscoverTest*` family below.
# `discoverFunction` is the name of the function used to link and discover the
# tests, i.e. either `linkAndDiscoverTest` or its serial variant
# `linkAndDiscoverTestSerial`.
function(addLinkAndDiscoverTestImpl discoverFunction basename)
    if (SINGLE_TEST_BINARY)
        target_sources(QLeverAllUnitTestsMain PUBLIC ${basename}.cpp)
        qlever_target_link_libraries(QLeverAllUnitTestsMain ${ARGN})
    else ()
        addTest(${basename})
        cmake_language(CALL ${discoverFunction} ${basename} ${ARGN})
    endif ()

endfunction()

# Usage: `addAndLinkTest[NoLibs](basename, [additionalLibraries...]`
# Add a GTest/GMock test case that is called `basename` and compiled from a file called
# `basename.cpp`. All tests are linked against `gmock_main` and the threading library.
# additional libraries against which the test case has to be linked can be specified as
# additional arguments after the `basename`

# This function links the test against `testUtil` (basically all of QLever).
function(addLinkAndDiscoverTest basename)
    addLinkAndDiscoverTestImpl(linkAndDiscoverTest ${basename} ${ARGN} testUtil)
endfunction()

# This function links only against Gtest + the explicitly specified libraries.
# It can be used for tests of standalone utils that don't require the rest of QLever.
function(addLinkAndDiscoverTestNoLibs basename)
    addLinkAndDiscoverTestImpl(linkAndDiscoverTest ${basename} ${ARGN})
endfunction()


# Add a GTest/GMock test case that is called `basename` and compiled from a file called
# `basename.cpp`. All tests are linked against `gmock_main` and the threading library.
# In contrast to `addLinkAndDiscoverTest` this doesn't let ctest run all subtests individually,
# but all at once to reduce overhead in the CI pipeline.
function(addLinkAndRunAsSingleTest basename)
    if (SINGLE_TEST_BINARY)
        target_sources(QLeverAllUnitTestsMain PUBLIC ${basename}.cpp)
        qlever_target_link_libraries(QLeverAllUnitTestsMain ${ARGN})
    else ()
        addTest(${basename})
        linkTest(${basename} testUtil ${ARGN})
        add_test(NAME ${basename} COMMAND ${basename})
    endif ()

endfunction()

# Serial variants of `addLinkAndDiscoverTest` / `addLinkAndDiscoverTestNoLibs`.
# See `linkAndDiscoverTestSerial` for the policy on when to use them.
function(addLinkAndDiscoverTestSerial basename)
    addLinkAndDiscoverTestImpl(linkAndDiscoverTestSerial ${basename} ${ARGN} testUtil)
endfunction()

function(addLinkAndDiscoverTestSerialNoLibs basename)
    addLinkAndDiscoverTestImpl(linkAndDiscoverTestSerial ${basename} ${ARGN})
endfunction()

# Only compile and link the test, but do not run it.
# Usage: Same as for the two functions above.
function(addAndLinkTest basename)
    addTest(${basename})
    linkTest(${basename} ${ARGN})
endfunction()

add_subdirectory(engine)
add_subdirectory(parser)
add_subdirectory(index)
add_subdirectory(backports)
add_subdirectory(rdfTypes)
add_subdirectory(joinAlgorithms)
add_subdirectory(libqlever)

OPTION(_DISABLE_EMSCRIPTEN_PROBLEMATIC_TESTS "Disable some tests that cause undefined symbol errors when building with Emscripten." OFF)

if (NOT _DISABLE_EMSCRIPTEN_PROBLEMATIC_TESTS)
    addLinkAndDiscoverTestSerial(BenchmarkMeasurementContainerTest benchmark testUtil)

    addLinkAndDiscoverTest(ResultTableColumnOperationsTest benchmark testUtil)

    # `ServerTest` links the `http`/`server` CMake targets, which are
    # entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
    if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
        addLinkAndDiscoverTest(ServerTest engine server)
    endif ()

    # `SparqlProtocolTest` links the `http`/`server` CMake targets, which are
    # entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
    if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
        addLinkAndDiscoverTest(SparqlProtocolTest engine)
    endif ()

    # `UrlParserTest` links the `http`/`server` CMake targets, which are
    # entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
    if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
        addLinkAndDiscoverTest(UrlParserTest)
    endif ()

    # `GraphStoreProtocolTest` links the `http`/`server` CMake targets, which are
    # entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
    if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
        addLinkAndDiscoverTest(GraphStoreProtocolTest engine server)
    endif ()

    # `ParsedRequestBuilderTest` links the `http`/`server` CMake targets, which are
    # entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
    if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
        addLinkAndDiscoverTest(ParsedRequestBuilderTest engine)
    endif ()

    addLinkAndDiscoverTest(ExceptionHandlingTest)
endif ()

addLinkAndDiscoverTest(ValueIdComparatorsTest util)

addLinkAndDiscoverTest(SparqlParserTest parser engine sparqlExpressions)

addLinkAndDiscoverTest(VariableCounterTest parser engine)

addLinkAndDiscoverTest(StringUtilsTest util)

addLinkAndDiscoverTestNoLibs(ConstexprSmallStringTest)

addLinkAndDiscoverTest(CryptographicHashUtilsTest util)

addLinkAndDiscoverTest(CacheTest)

addLinkAndDiscoverTestSerialNoLibs(ConcurrentCacheTest)

# This test also seems to use the same filenames and should be fixed.
addLinkAndDiscoverTest(FileTest)

addLinkAndDiscoverTest(Simple8bTest)

addLinkAndDiscoverTest(WordsAndDocsFileParserTest parser)

addLinkAndDiscoverTest(IndexMetaDataTest index)

addLinkAndDiscoverTest(IndexTest index)

addLinkAndDiscoverTest(LocatedTriplesTest index)

addLinkAndDiscoverTest(IdTripleTest index)

addLinkAndDiscoverTestNoLibs(ZipMergeUniqueViewTest)

addLinkAndDiscoverTestNoLibs(SortedSequenceTest)

addLinkAndDiscoverTest(DeltaTriplesTest index)

addLinkAndDiscoverTest(UpdateMetadataTest index)

addLinkAndDiscoverTest(IdTableUtilsTest engine)

addLinkAndDiscoverTest(JoinTest engine)

addLinkAndDiscoverTest(TextLimitOperationTest engine)

addLinkAndDiscoverTest(QueryPlannerTest engine)

addLinkAndDiscoverTest(QueryPlannerSpatialJoinTest engine)

addLinkAndDiscoverTestNoLibs(HashMapTest)

addLinkAndDiscoverTestNoLibs(StringPairHashMapTest)

addLinkAndDiscoverTest(HashSetTest)

addLinkAndDiscoverTest(GroupByTest engine)

addLinkAndDiscoverTest(VocabularyGeneratorTest index)

addLinkAndDiscoverTest(HasPredicateScanTest engine)

addLinkAndDiscoverTest(MmapVectorTest)

# BufferedVectorTest also uses conflicting filenames.
addLinkAndDiscoverTest(BufferedVectorTest)

addLinkAndDiscoverTest(UnionTest engine)

if (SINGLE_TEST_BINARY)
    target_sources(QLeverAllUnitTestsMain PUBLIC TokenTest.cpp TokenTestCtreHelper.cpp)
    qlever_target_link_libraries(QLeverAllUnitTestsMain parser re2 util)
else ()
    add_executable(TokenTest TokenTest.cpp TokenTestCtreHelper.cpp)
    linkAndDiscoverTest(TokenTest parser re2 util)
endif ()

addLinkAndDiscoverTest(RdfParserTest parser re2)

addLinkAndDiscoverTest(MultiColumnJoinTest engine)

addLinkAndDiscoverTest(IdTableTest util)

addLinkAndDiscoverTest(TransitivePathTest engine)

addLinkAndDiscoverTest(TransitivePathGraphSearchTest engine)

addLinkAndDiscoverTest(PathSearchTest engine)

addLinkAndDiscoverTest(TupleHelpersTest)

addLinkAndDiscoverTest(UriParserUriTest)

addLinkAndDiscoverTest(ParsedUriTest)

addLinkAndDiscoverTestNoLibs(StringSortComparatorTest)

addLinkAndDiscoverTestNoLibs(GenericCharTraitsTest)

addLinkAndDiscoverTest(PriorityQueueTest)

addLinkAndDiscoverTestNoLibs(SynchronizedTest)

addLinkAndDiscoverTest(AllocatorWithLimitTest)

addLinkAndDiscoverTestNoLibs(AlignedAllocatorTest)

addLinkAndDiscoverTest(MinusTest engine)

# this test runs for quite some time and might have spurious failures!
# Therefore it is compiled, but not run. If you want to run it,
# change the following two lines.
addAndLinkTest(SortPerformanceEstimatorTest SortPerformanceEstimator)
#addLinkAndDiscoverTest(SortPerformanceEstimatorTest SortPerformanceEstimator)

addLinkAndDiscoverTestNoLibs(SerializerTest)

addLinkAndDiscoverTestNoLibs(ParametersTest)

addLinkAndDiscoverTestNoLibs(ConstructDeduplicationModeTest)

addLinkAndDiscoverTest(ZstdCompressionTest zstd ${cmake_thread_libs_init})

addLinkAndDiscoverTest(TaskQueueTest)

addLinkAndDiscoverTest(SetOfIntervalsTest sparqlExpressions)

addLinkAndDiscoverTestNoLibs(TypeTraitsTest)

addLinkAndDiscoverTest(SparqlExpressionTest sparqlExpressions index engine)

addLinkAndDiscoverTest(SparqlExpressionMemberFunctionsTest sparqlExpressions index engine)

# `StreamableBodyTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(StreamableBodyTest http)
endif ()

# `StreamableGeneratorTest` directly exercises `cppcoro::generator`-based coroutine
# machinery, which is entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(StreamableGeneratorTest)
endif ()

addLinkAndDiscoverTestNoLibs(StringBatcherTest)

addLinkAndDiscoverTest(AcceptHeaderTest mediaTypes httpParser)

addLinkAndDiscoverTestNoLibs(CompactStringVectorTest parser)

addLinkAndDiscoverTest(SparqlDataTypesTest engine)

# `ContentEncodingHelperTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(ContentEncodingHelperTest http)
endif ()

addLinkAndDiscoverTest(PrefixCompressorTest)

addLinkAndDiscoverTest(VocabularyTest index)

addLinkAndDiscoverTestNoLibs(IteratorTest)

addLinkAndDiscoverTestNoLibs(ViewsTest)

addLinkAndDiscoverTestNoLibs(TakeUntilInclusiveViewTest)

addLinkAndDiscoverTest(ForwardTest)

# `CompressorStreamTest` directly exercises `cppcoro::generator`-based coroutine
# machinery, which is entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTestNoLibs(CompressorStreamTest Boost::iostreams)
endif ()

# `AsyncStreamTest` directly exercises `cppcoro::generator`-based coroutine
# machinery, which is entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTestNoLibs(AsyncStreamTest)
endif ()

addLinkAndDiscoverTest(BitUtilsTest)

addLinkAndDiscoverTest(NBitIntegerTest)

addLinkAndDiscoverTest(GeoPointTest)

addLinkAndDiscoverTest(GeoSparqlHelpersTest util)

# `HttpUtilsTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(HttpUtilsTest util http)
endif ()

addLinkAndDiscoverTest(DateYearDurationTest util parser)

addLinkAndDiscoverTest(DurationTest util)

addLinkAndDiscoverTest(TripleComponentTest parser)

addLinkAndDiscoverTest(ValueIdTest util)

addLinkAndDiscoverTestNoLibs(LambdaHelpersTest)

addLinkAndDiscoverTest(ParseExceptionTest parser engine)

addLinkAndDiscoverTest(TransparentFunctorsTest)

addLinkAndDiscoverTest(SelectClauseTest parser engine)

addLinkAndDiscoverTest(RelationalExpressionTest parser sparqlExpressions index engine)

addLinkAndDiscoverTest(CheckUsePatternTrickTest parser engine)

addLinkAndDiscoverTest(RegexExpressionTest parser sparqlExpressions index engine parser)

addLinkAndDiscoverTest(LocalVocabTest engine)

addLinkAndDiscoverTest(ValuesTest engine)

# `ServiceTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(ServiceTest engine)
endif ()

# `LoadTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(LoadTest engine)
endif ()

# HttpTest is disabled for Emscripten: emsdk 6.0.2 miscompiles the boost::beast
# websocket coroutine code under both Wasm EH modes (legacy: wasm-opt rejects an
# invalid `rethrow`; standard EH: the WebAssemblyLateEHPrepare backend pass segfaults).
if (NOT _DISABLE_EMSCRIPTEN_PROBLEMATIC_TESTS)
    # `HttpTest` links the `http`/`server` CMake targets, which are
    # entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
    if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
        addLinkAndDiscoverTest(HttpTest Boost::iostreams http)
    endif ()
endif ()

addLinkAndDiscoverTestNoLibs(CallFixedSizeTest)

addLinkAndDiscoverTestNoLibs(ConstexprUtilsTest)

addLinkAndDiscoverTestNoLibs(ResetWhenMovedTest)

addLinkAndDiscoverTestSerial(TimerTest)

addLinkAndDiscoverTest(AlgorithmTest)

addLinkAndDiscoverTest(CompressedRelationsTest index)

addLinkAndDiscoverTest(PrefilterExpressionIndexTest engine)

addLinkAndDiscoverTest(GetPrefilterExpressionFromSparqlExpressionTest sparqlExpressions index)

addLinkAndDiscoverTest(ExceptionTest)

addLinkAndDiscoverTest(RandomExpressionTest index)

addLinkAndDiscoverTest(NowDatetimeExpressionTest index)

addLinkAndDiscoverTest(LanguageExpressionsTest index)

addLinkAndDiscoverTest(SortTest engine)

addLinkAndDiscoverTest(OrderByTest engine)

addLinkAndDiscoverTest(ValuesForTestingTest index)

addLinkAndDiscoverTest(ExportQueryExecutionTreesTest index engine parser)

addLinkAndDiscoverTest(AggregateExpressionTest parser sparqlExpressions index engine)

addLinkAndDiscoverTest(OnDestructionDontThrowDuringStackUnwindingTest)

addLinkAndDiscoverTest(SparqlExpressionTypesTest sparqlExpressions util)

addLinkAndDiscoverTest(LimitOffsetClauseTest)

addLinkAndDiscoverTest(OperationTest engine)

addLinkAndDiscoverTest(RuntimeInformationTest engine index)

addLinkAndDiscoverTest(VariableToColumnMapTest parser)

addLinkAndDiscoverTest(CopyableUniquePtrTest)

addLinkAndDiscoverTest(JsonCustomConverterForThirdPartyTest)

addLinkAndDiscoverTest(ConfigManagerTest configManager)

addLinkAndDiscoverTest(ConfigOptionTest configManager)

addLinkAndDiscoverTest(ValidatorTest configManager)

addLinkAndDiscoverTest(ConfigOptionProxyTest configManager)

addLinkAndDiscoverTest(ConfigUtilTest configManager)

addLinkAndDiscoverTest(RandomTest)

addLinkAndDiscoverTest(FindUndefRangesTest engine)

addLinkAndDiscoverTest(AddCombinedRowToTableTest engine util)

addLinkAndDiscoverTest(CtreHelpersTest)

addLinkAndDiscoverTest(ComparisonWithNanTest)

addLinkAndDiscoverTestNoLibs(ThreadSafeQueueTest)

addLinkAndDiscoverTest(IdTableHelpersTest)

# `GeneratorTest` directly exercises `cppcoro::generator`-based coroutine
# machinery, which is entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(GeneratorTest)
endif ()

addLinkAndDiscoverTest(MemorySizeTest memorySize)

addLinkAndDiscoverTest(JsonUtilTest)

addLinkAndDiscoverTest(JoinAlgorithmsTest)

# `AsioHelpersTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(AsioHelpersTest)
endif ()
# With `-fwasm-exceptions`, clang's `correlated-propagation` pass (run at -O2/-O3)
# crashes on the coroutine `.resume` body in this file. Compiling just this
# translation unit at -O1 avoids the crashing pass.
if (EMSCRIPTEN)
    set_source_files_properties(AsioHelpersTest.cpp PROPERTIES COMPILE_OPTIONS "-O1")
endif ()

addLinkAndDiscoverTest(UniqueCleanupTest)

# `WebSocketSessionTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(WebSocketSessionTest http)
endif ()

addLinkAndDiscoverTest(QueryIdTest)

# `QueryHubTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(QueryHubTest http)
endif ()

# `QueryToSocketDistributorTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(QueryToSocketDistributorTest http)
endif ()

# `UpdateFetcherTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(UpdateFetcherTest http)
endif ()

# `MessageSenderTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(MessageSenderTest http)
endif ()

addLinkAndDiscoverTestNoLibs(CancellationHandleTest util)

addLinkAndDiscoverTestSerial(ProgressBarTest util)

addLinkAndDiscoverTest(CachingMemoryResourceTest)

addLinkAndDiscoverTestNoLibs(ParallelMultiwayMergeTest)

addLinkAndDiscoverTest(ParseableDurationTest)

addLinkAndDiscoverTest(ConstantsTest)

addLinkAndDiscoverTest(JThreadTest)

addLinkAndDiscoverTest(ChunkedForLoopTest)

addLinkAndDiscoverTest(FsstCompressorTest fsst)

addLinkAndDiscoverTest(CopyableSynchronizationTest)

# `LazyJsonParserTest` directly exercises `cppcoro::generator`-based coroutine
# machinery, which is entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(LazyJsonParserTest)
endif ()

# `GeneratorsTest` directly exercises `cppcoro::generator`-based coroutine
# machinery, which is entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(GeneratorsTest)
endif ()

addLinkAndDiscoverTest(FilterTest engine)

addLinkAndDiscoverTest(ResultTest engine)

addLinkAndDiscoverTest(BlankNodeManagerTest)

addLinkAndDiscoverTest(SparqlExpressionGeneratorsTest engine)

addLinkAndDiscoverTest(ExecuteUpdateTest engine)

addLinkAndDiscoverTest(ValueGetterTest engine)

addLinkAndDiscoverTest(LruCacheTest)

addLinkAndDiscoverTest(LruCacheWithStatisticsTest)

addLinkAndDiscoverTestNoLibs(InputRangeUtilsTest)

addLinkAndDiscoverTest(TripleSerializerTest)

addLinkAndDiscoverTest(GeometryInfoTest util)

addLinkAndDiscoverTest(QueryRewriteUtilTest engine)

addLinkAndDiscoverTest(HttpErrorTest engine)

addLinkAndDiscoverTest(TimeTracerTest)

addLinkAndDiscoverTestNoLibs(SourceLocationTest)

addLinkAndDiscoverTest(UnitOfMeasurementTest util)

# `MaterializedViewsTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(MaterializedViewsTest qlever engine server)
endif ()

# `MaterializedViewsStarRewriteTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(MaterializedViewsStarRewriteTest qlever engine server)
endif ()

addLinkAndDiscoverTestNoLibs(ConstexprMapTest)

addLinkAndDiscoverTestNoLibs(ParallelExecutorTest)

addLinkAndDiscoverTestNoLibs(IoUringManagerTest util)

addLinkAndDiscoverTestNoLibs(VariantRangeFilterTest)

addLinkAndDiscoverTest(ConstructTemplatePreprocessorTest engine)

addLinkAndDiscoverTest(ExternalValuesQueryTest engine)
addLinkAndDiscoverTest(ExternalValuesTest engine)

addLinkAndDiscoverTest(ConstructBatchEvaluatorTest engine)

addLinkAndDiscoverTest(ExportIdsTest engine parser)

addLinkAndDiscoverTestNoLibs(EnumWithStringsTest Boost::program_options)

addLinkAndDiscoverTest(ConstructTripleGeneratorTest engine mediaTypes)

addLinkAndDiscoverTest(FilesystemHelpersTest util)

addLinkAndDiscoverTest(LogTest)

addLinkAndDiscoverTest(QueryEventLogTest util)
