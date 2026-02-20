include(GoogleTest)

# Needed for creating the `testUil`-library.
add_subdirectory(util EXCLUDE_FROM_ALL)

function(strip_shared_lib_debug_symbols target)
    add_custom_command(
            TARGET ${target}
            POST_BUILD
            COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:${target}> $<TARGET_FILE:${target}>.debug
            COMMAND ${CMAKE_STRIP} --strip-debug $<TARGET_FILE:${target}>
            COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:${target}>.debug $<TARGET_FILE:${target}>
            COMMENT "Stripping shared lib debug symbols from ${target}"
    )
endfunction()

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

# Usage: `linkAndDiscoverTestSerial(basename, [additionalLibraries...]`
# Similar to `linkAndDiscoverTestSerial` but also requires that the test is run serially
# (without any of the other test cases running in parallel). This can be
# required e.g. if several tests cases write to the same file.
function(linkAndDiscoverTestSerial basename)
    linkTest(${basename} ${ARGN})
    gtest_discover_tests(${basename} ${basename} DISCOVERY_TIMEOUT 600 PROPERTIES RUN_SERIAL
            TRUE LABELS "${basename}")
endfunction()

if (SINGLE_TEST_BINARY)
    message(STATUS "All tests are linked into a single executable `QLeverAllUnitTestsMain`")
    add_executable(QLeverAllUnitTestsMain)
    qlever_target_link_libraries(QLeverAllUnitTestsMain gtest gmock_main testUtil qlever-shared-complete ${CMAKE_THREAD_LIBS_INIT})
    # The following line (as opposed to using `gtest_discover_tests`) has the effect that `ctest` will treat all the
    # unit tests as a single test case and will simply run the executable (using the main function from `gtest_main`.
    # This decreases the runtime overhead per unit tests in particular for sanitizer builds.
    add_test(NAME QLeverAllUnitTests COMMAND $<TARGET_FILE:QLeverAllUnitTestsMain>)
else ()
    message(STATUS "The tests are split over multiple binaries")

endif ()

# The implementation of `addLinkAndDiscoverTest` and `addLinkandDiscoverTestNoLibs` below.
function(addLinkAndDiscoverTestImpl basename)
    if (SINGLE_TEST_BINARY)
        target_sources(QLeverAllUnitTestsMain PUBLIC ${basename}.cpp)
        qlever_target_link_libraries(QLeverAllUnitTestsMain ${ARGN} )
    else ()
        addTest(${basename})
        linkAndDiscoverTest(${basename} ${ARGN})
    endif ()

endfunction()

# Usage: `addAndLinkTest[NoLibs](basename, [additionalLibraries...]`
# Add a GTest/GMock test case that is called `basename` and compiled from a file called
# `basename.cpp`. All tests are linked against `gmock_main` and the threading library.
# additional libraries against which the test case has to be linked can be specified as
# additional arguments after the `basename`

# This function links the test against `testUtil` (basically all of QLever).
function(addLinkAndDiscoverTest basename)
    addLinkAndDiscoverTestImpl(${basename} ${ARGN} testUtil qlever-shared-complete)
endfunction()

# This function links only against Gtest + the explicitly specified libraries.
# It can be used for tests of standalone utils that don't require the rest of QLever.
function(addLinkAndDiscoverTestNoLibs basename)
    addLinkAndDiscoverTestImpl(${basename} ${ARGN})
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
        linkTest(${basename} testUtil qlever-shared-complete ${ARGN})
        add_test(NAME ${basename} COMMAND ${basename})
    endif ()

endfunction()

# Usage: `addAndLinkTestSerial(basename, [additionalLibraries...]`
# Similar to `addAndLinkTest` but also requires that the test is run serially
# (without any of the other test cases running in parallel). This can be
# required e.g. if several tests cases write to the same file.
function(addLinkAndDiscoverTestSerial basename)
    if (SINGLE_TEST_BINARY)
        target_sources(QLeverAllUnitTestsMain PUBLIC ${basename}.cpp)
        qlever_target_link_libraries(QLeverAllUnitTestsMain ${ARGN})
    else ()
        addTest(${basename})
        linkAndDiscoverTestSerial(${basename} testUtil qlever-shared-complete ${ARGN})
    endif ()
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
    addLinkAndDiscoverTest(BenchmarkMeasurementContainerTest benchmark testUtil qlever-shared-complete)

    addLinkAndDiscoverTest(ResultTableColumnOperationsTest benchmark testUtil qlever-shared-complete)

    addLinkAndDiscoverTest(ServerTest)

    addLinkAndDiscoverTest(SparqlProtocolTest Boost::url)

    addLinkAndDiscoverTest(UrlParserTest Boost::url)

    addLinkAndDiscoverTest(GraphStoreProtocolTest)

    addLinkAndDiscoverTest(ParsedRequestBuilderTest)

    addLinkAndDiscoverTest(ExceptionHandlingTest)
endif ()

addLinkAndDiscoverTest(ValueIdComparatorsTest)

addLinkAndDiscoverTest(SparqlParserTest)

addLinkAndDiscoverTest(StringUtilsTest)

addLinkAndDiscoverTestNoLibs(ConstexprSmallStringTest)

addLinkAndDiscoverTest(CryptographicHashUtilsTest)

addLinkAndDiscoverTest(CacheTest)

addLinkAndDiscoverTestNoLibs(ConcurrentCacheTest)

# This test also seems to use the same filenames and should be fixed.
addLinkAndDiscoverTestSerial(FileTest)

addLinkAndDiscoverTest(Simple8bTest)

addLinkAndDiscoverTest(WordsAndDocsFileParserTest)

addLinkAndDiscoverTest(IndexMetaDataTest)

# We currently always use static file names for all indices, which
# makes it impossible to run the test cases for the Index class in parallel.
# TODO<qup42, joka921> fix this
addLinkAndDiscoverTestSerial(IndexTest)

addLinkAndDiscoverTest(LocatedTriplesTest)

addLinkAndDiscoverTestSerial(IdTripleTest)

addLinkAndDiscoverTestSerial(DeltaTriplesTest)

addLinkAndDiscoverTest(DeltaTriplesCountTest)

addLinkAndDiscoverTest(EngineTest)

addLinkAndDiscoverTest(JoinTest)

addLinkAndDiscoverTest(TextLimitOperationTest)

addLinkAndDiscoverTestSerial(QueryPlannerTest)

addLinkAndDiscoverTestSerial(QueryPlannerSpatialJoinTest)

addLinkAndDiscoverTestNoLibs(HashMapTest)

addLinkAndDiscoverTest(HashSetTest)

addLinkAndDiscoverTestSerial(GroupByTest)

addLinkAndDiscoverTest(VocabularyGeneratorTest)

addLinkAndDiscoverTest(HasPredicateScanTest)

addLinkAndDiscoverTest(MmapVectorTest)

# BufferedVectorTest also uses conflicting filenames.
addLinkAndDiscoverTestSerial(BufferedVectorTest)

addLinkAndDiscoverTest(UnionTest)

if (SINGLE_TEST_BINARY)
    target_sources(QLeverAllUnitTestsMain PUBLIC TokenTest.cpp TokenTestCtreHelper.cpp)
    qlever_target_link_libraries(QLeverAllUnitTestsMain parser re2 util)
else ()
    add_executable(TokenTest TokenTest.cpp TokenTestCtreHelper.cpp)
    linkAndDiscoverTest(TokenTest testUtil qlever-shared-complete)
endif ()

addLinkAndDiscoverTestSerial(RdfParserTest re2)

addLinkAndDiscoverTest(MultiColumnJoinTest)

addLinkAndDiscoverTest(IdTableTest)

addLinkAndDiscoverTest(TransitivePathTest)

addLinkAndDiscoverTest(PathSearchTest)

addLinkAndDiscoverTest(BatchedPipelineTest)

addLinkAndDiscoverTest(TupleHelpersTest)

addLinkAndDiscoverTestNoLibs(StringSortComparatorTest util)

addLinkAndDiscoverTest(PriorityQueueTest)

addLinkAndDiscoverTestNoLibs(SynchronizedTest)

addLinkAndDiscoverTest(AllocatorWithLimitTest)

addLinkAndDiscoverTest(MinusTest)

# this test runs for quite some time and might have spurious failures!
# Therefore it is compiled, but not run. If you want to run it,
# change the following two lines.
addAndLinkTest(SortPerformanceEstimatorTest SortPerformanceEstimator)
#addLinkAndDiscoverTest(SortPerformanceEstimatorTest SortPerformanceEstimator)

# The SerializerTest uses temporary files. The tests fail when multiple test
# cases are run in parallel. This should be fixed by using distinct filenames
# for each test case.
# TODO<qup42, joka921> fix this
addLinkAndDiscoverTestNoLibs(SerializerTest)

addLinkAndDiscoverTestNoLibs(ParametersTest)

addLinkAndDiscoverTest(ZstdCompressionTest zstd ${cmake_thread_libs_init})

addLinkAndDiscoverTest(TaskQueueTest)

addLinkAndDiscoverTest(SetOfIntervalsTest)

addLinkAndDiscoverTestNoLibs(TypeTraitsTest)

addLinkAndDiscoverTestSerial(SparqlExpressionTest)

addLinkAndDiscoverTest(StreamableBodyTest)

addLinkAndDiscoverTest(StreamableGeneratorTest)

addLinkAndDiscoverTestNoLibs(StringBatcherTest)

addLinkAndDiscoverTest(AcceptHeaderTest mediaTypes httpParser)

addLinkAndDiscoverTestNoLibs(CompactStringVectorTest)

addLinkAndDiscoverTest(SparqlDataTypesTest)

addLinkAndDiscoverTest(ContentEncodingHelperTest)

addLinkAndDiscoverTest(PrefixCompressorTest)

addLinkAndDiscoverTest(VocabularyTest)

addLinkAndDiscoverTestNoLibs(IteratorTest)

addLinkAndDiscoverTestNoLibs(ViewsTest)

addLinkAndDiscoverTestNoLibs(TakeUntilInclusiveViewTest)

addLinkAndDiscoverTest(ForwardTest)

addLinkAndDiscoverTest(CompressorStreamTest Boost::iostreams)

addLinkAndDiscoverTestNoLibs(AsyncStreamTest)

addLinkAndDiscoverTest(BitUtilsTest)

addLinkAndDiscoverTest(NBitIntegerTest)

addLinkAndDiscoverTest(GeoPointTest)

addLinkAndDiscoverTest(GeoSparqlHelpersTest)

addLinkAndDiscoverTest(HttpUtilsTest)

addLinkAndDiscoverTestSerial(DateYearDurationTest)

addLinkAndDiscoverTest(DurationTest)

addLinkAndDiscoverTest(TripleComponentTest)

addLinkAndDiscoverTest(ValueIdTest)

addLinkAndDiscoverTestNoLibs(LambdaHelpersTest)

addLinkAndDiscoverTest(ParseExceptionTest)

addLinkAndDiscoverTest(TransparentFunctorsTest)

addLinkAndDiscoverTest(SelectClauseTest)

addLinkAndDiscoverTestSerial(RelationalExpressionTest)

addLinkAndDiscoverTest(CheckUsePatternTrickTest)

addLinkAndDiscoverTestSerial(RegexExpressionTest)

addLinkAndDiscoverTestSerial(LocalVocabTest)

addLinkAndDiscoverTestSerial(ValuesTest)

addLinkAndDiscoverTestSerial(ServiceTest)

addLinkAndDiscoverTestSerial(LoadTest)

addLinkAndDiscoverTestSerial(HttpTest Boost::iostreams)

addLinkAndDiscoverTestNoLibs(CallFixedSizeTest)

addLinkAndDiscoverTestNoLibs(ConstexprUtilsTest)

addLinkAndDiscoverTestNoLibs(ResetWhenMovedTest)

addLinkAndDiscoverTest(TimerTest)

addLinkAndDiscoverTest(AlgorithmTest)

addLinkAndDiscoverTestSerial(CompressedRelationsTest)

addLinkAndDiscoverTestSerial(PrefilterExpressionIndexTest)

addLinkAndDiscoverTestSerial(GetPrefilterExpressionFromSparqlExpressionTest)

addLinkAndDiscoverTest(ExceptionTest)

addLinkAndDiscoverTestSerial(RandomExpressionTest)

addLinkAndDiscoverTestSerial(NowDatetimeExpressionTest)

addLinkAndDiscoverTestSerial(LanguageExpressionsTest)

addLinkAndDiscoverTestSerial(SortTest)

addLinkAndDiscoverTestSerial(OrderByTest)

addLinkAndDiscoverTestSerial(ValuesForTestingTest)

addLinkAndDiscoverTestSerial(ExportQueryExecutionTreesTest)

addLinkAndDiscoverTestSerial(AggregateExpressionTest)

addLinkAndDiscoverTest(OnDestructionDontThrowDuringStackUnwindingTest)

addLinkAndDiscoverTest(SparqlExpressionTypesTest)

addLinkAndDiscoverTest(LimitOffsetClauseTest)

addLinkAndDiscoverTest(OperationTest)

addLinkAndDiscoverTest(RuntimeInformationTest)

addLinkAndDiscoverTest(VariableToColumnMapTest)

addLinkAndDiscoverTest(CopyableUniquePtrTest)

addLinkAndDiscoverTest(JsonCustomConverterForThirdPartyTest)

addLinkAndDiscoverTest(ConfigManagerTest configManager)

addLinkAndDiscoverTest(ConfigOptionTest configManager)

addLinkAndDiscoverTest(ValidatorTest configManager)

addLinkAndDiscoverTest(ConfigOptionProxyTest configManager)

addLinkAndDiscoverTest(ConfigUtilTest configManager)

addLinkAndDiscoverTest(RandomTest)

addLinkAndDiscoverTest(FindUndefRangesTest)

addLinkAndDiscoverTest(AddCombinedRowToTableTest)

addLinkAndDiscoverTest(CtreHelpersTest)

addLinkAndDiscoverTest(ComparisonWithNanTest)

addLinkAndDiscoverTestNoLibs(ThreadSafeQueueTest)

addLinkAndDiscoverTest(IdTableHelpersTest)

addLinkAndDiscoverTest(GeneratorTest)

addLinkAndDiscoverTest(MemorySizeTest memorySize)

addLinkAndDiscoverTest(JsonUtilTest)

addLinkAndDiscoverTest(JoinAlgorithmsTest)

addLinkAndDiscoverTest(AsioHelpersTest)

addLinkAndDiscoverTest(UniqueCleanupTest)

addLinkAndDiscoverTest(WebSocketSessionTest)

addLinkAndDiscoverTest(QueryIdTest)

addLinkAndDiscoverTest(QueryHubTest)

addLinkAndDiscoverTest(QueryToSocketDistributorTest)

addLinkAndDiscoverTest(UpdateFetcherTest)

addLinkAndDiscoverTest(MessageSenderTest)

addLinkAndDiscoverTestNoLibs(CancellationHandleTest util)

addLinkAndDiscoverTest(ProgressBarTest)

addLinkAndDiscoverTest(CachingMemoryResourceTest)

addLinkAndDiscoverTestNoLibs(ParallelMultiwayMergeTest)

addLinkAndDiscoverTest(ParseableDurationTest)

addLinkAndDiscoverTest(ConstantsTest)

addLinkAndDiscoverTest(JThreadTest)

addLinkAndDiscoverTest(ChunkedForLoopTest)

addLinkAndDiscoverTest(FsstCompressorTest fsst)

addLinkAndDiscoverTest(CopyableSynchronizationTest)

addLinkAndDiscoverTest(LazyJsonParserTest)

addLinkAndDiscoverTest(GeneratorsTest)

addLinkAndDiscoverTest(FilterTest)

addLinkAndDiscoverTest(ResultTest)

addLinkAndDiscoverTest(BlankNodeManagerTest)

addLinkAndDiscoverTest(SparqlExpressionGeneratorsTest)

addLinkAndDiscoverTest(ExecuteUpdateTest)

addLinkAndDiscoverTest(ValueGetterTest)

addLinkAndDiscoverTest(LruCacheTest)

addLinkAndDiscoverTestNoLibs(InputRangeUtilsTest)

addLinkAndDiscoverTest(TripleSerializerTest)

addLinkAndDiscoverTest(GeometryInfoTest)

addLinkAndDiscoverTestSerial(QueryRewriteUtilTest)

addLinkAndDiscoverTestSerial(HttpErrorTest)

addLinkAndDiscoverTest(TimeTracerTest)

addLinkAndDiscoverTestNoLibs(SourceLocationTest)

addLinkAndDiscoverTest(UnitOfMeasurementTest)

addLinkAndDiscoverTest(MaterializedViewsTest)

addLinkAndDiscoverTestNoLibs(ConstexprMapTest)

addLinkAndDiscoverTestNoLibs(ParallelExecutorTest)
