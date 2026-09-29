# Re-compiling all the infrastructure all the time is a waste of time, so we
# pre-compile that part as a library.

OPTION(_NO_BENCHMARK "Disable benchmark targets, for example when building with Emscripten." OFF)

if (NOT _NO_BENCHMARK)
    # This part compiles 'just' the benchmark infrastructure and doesn't provide a main function.
    add_library(benchmark infrastructure/Benchmark.cpp infrastructure/BenchmarkMeasurementContainer.cpp
        infrastructure/BenchmarkToJson.cpp infrastructure/BenchmarkToString.cpp)
    qlever_target_link_libraries(benchmark Boost::program_options absl::str_format sparqlParser qlever_util configManager)

    # This provides the library of the benchmark infrastructure together with its default main function.
    add_library(benchmarkWithMain infrastructure/BenchmarkMain.cpp)
    qlever_target_link_libraries(benchmarkWithMain benchmark)

    # Link binary ${basename} against the benchmark library,
    # and all libraries that are specified as additional arguments.
    function (linkBenchmark basename)
        qlever_target_link_libraries(${basename} benchmarkWithMain ${ARGN})
    endfunction()

    # Compile the benchmark and link it.
    # Usage: addAndLinkBenchmark(baseName, [additionalLibraries...])
    #   - baseName: The name of the benchmark file without the .cpp ending.
    function(addAndLinkBenchmark baseName)
        add_executable(${baseName}  "${baseName}.cpp")
        linkBenchmark(${baseName} ${ARGN})
    endfunction()

    # Add benchmarks after here.
    addAndLinkBenchmark(BenchmarkExamples)

    addAndLinkBenchmark(JoinAlgorithmBenchmark testUtil memorySize)

    addAndLinkBenchmark(ParallelMergeBenchmark testUtil)

    addAndLinkBenchmark(GroupByHashMapBenchmark engine testUtil gtest gmock)

    addAndLinkBenchmark(SparqlExpressionBenchmark engine testUtil gtest gmock)
    addAndLinkBenchmark(FsstScratchBufferBenchmark fsst ${CMAKE_DL_LIBS})

    addAndLinkBenchmark(SerializerMicroBenchmark engine sparqlParser qlever_util)
    # The benchmark replaces the global `operator new`/`operator delete` by a
    # counting malloc/free pair. After inlining that `operator delete`, GCC
    # reports its `free` as a mismatch for pointers from `operator new`; a
    # `#pragma GCC diagnostic` in the file does not reach those inlined call
    # sites (GCC 11), so the (false positive) warning is disabled for this
    # target only.
    target_compile_options(SerializerMicroBenchmark PRIVATE
        $<$<CXX_COMPILER_ID:GNU>:-Wno-mismatched-new-delete>)

    addAndLinkBenchmark(PrefixCompressorBenchmark)

    addAndLinkBenchmark(VocabBatchLookupBenchmark vocabulary)

    addAndLinkBenchmark(VocabularyBatchLookupMicroBenchmark vocabulary)

    addAndLinkBenchmark(VocabularyBatchLookupEndToEndBenchmark vocabulary)

    addAndLinkBenchmark(CompressedVocabLookupBatchMicroBenchmark vocabulary
        fsst)

    addAndLinkBenchmark(CompressedVocabLookupBatchEndToEndBenchmark vocabulary
        fsst)

    addAndLinkBenchmark(SplitVocabLookupBatchMicroBenchmark vocabulary)

    addAndLinkBenchmark(SplitVocabLookupBatchEndToEndBenchmark vocabulary)

    addAndLinkBenchmark(PolymorphicVocabLookupBatchMicroBenchmark vocabulary)

    addAndLinkBenchmark(PolymorphicVocabLookupBatchEndToEndBenchmark vocabulary)

endif()
