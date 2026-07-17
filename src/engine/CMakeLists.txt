add_subdirectory(sparqlExpressions)
add_library(SortPerformanceEstimator SortPerformanceEstimator.cpp)
qlever_target_link_libraries(SortPerformanceEstimator parser)
add_library(engine
        QueryExecutionTree.cpp Operation.cpp Result.cpp
        IndexScan.cpp Join.cpp JoinImpl.cpp Sort.cpp
        Distinct.cpp OrderBy.cpp Filter.cpp
        QueryPlanner.cpp QueryPlanningCostFactors.cpp QueryRewriteUtils.cpp
        OptionalJoin.cpp CountAvailablePredicates.cpp GroupByImpl.cpp GroupBy.cpp HasPredicateScan.cpp
        Union.cpp MultiColumnJoin.cpp TransitivePathBase.cpp
        TransitivePathHashMap.cpp TransitivePathBinSearch.cpp Service.cpp
        Values.cpp Bind.cpp Minus.cpp RuntimeInformation.cpp CheckUsePatternTrick.cpp
        VariableToColumnMap.cpp ExportQueryExecutionTrees.cpp
        CartesianProductJoin.cpp TextIndexScanForWord.cpp TextIndexScanForEntity.cpp
        TextLimit.cpp LazyGroupBy.cpp GroupByHashMapOptimization.cpp SpatialJoin.cpp
        CountConnectedSubgraphs.cpp SpatialJoinAlgorithms.cpp PathSearch.cpp ExecuteUpdate.cpp
        Describe.cpp GraphStoreProtocol.cpp SpatialJoinParser.cpp SpatialJoinCachedIndex.cpp
        QueryExecutionContext.cpp ExistsJoin.cpp SparqlProtocol.cpp ParsedRequestBuilder.cpp
        NeutralOptional.cpp Load.cpp StripColumns.cpp NamedResultCache.cpp
        ExplicitIdTableOperation.cpp StringMapping.cpp MaterializedViews.cpp
        PermutationSelector.cpp ConstructTripleGenerator.cpp
        ConstructTemplatePreprocessor.cpp ConstructTripleInstantiator.cpp ConstructBatchEvaluator.cpp
        MaterializedViewsQueryAnalysis.cpp UpdateMetadata.cpp ExternalValues.cpp)

# `Boost::program_options` is not used inside `engine` itself, but the
# `qlever-server` target reuses the engine PCH (`target_precompile_headers
# REUSE_FROM engine`) and does link `Boost::program_options`. Linking it here
# too makes the engine PCH inherit `BOOST_PROGRAM_OPTIONS_DYN_LINK` (when
# Boost is shared) so the PCH matches qlever-server's compile flags and is
# not silently discarded by GCC with a `-Winvalid-pch` warning.
qlever_target_link_libraries(engine qlever_util index parser global sparqlExpressions SortPerformanceEstimator Boost::iostreams Boost::program_options s2 spatialjoin-dev pb_util pb_util_geo mediaTypes)

# When `REDUCED_FEATURE_SET_FOR_CPP17` is set, do not link `engine` against the
# `http` library (which requires C++20). This works because `http` is only used
# in two operations, `Service` and `Load`, which we disable when
# `REDUCED_FEATURE_SET_FOR_CPP17` is set.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    target_link_libraries(engine http)
endif()

# Don't build the `server` library at all when the server (and the tests
# linking it) are disabled for Emscripten: compiling `Server.cpp` crashes
# emsdk 6.0.2's clang in the `WebAssemblyLateEHPrepare` backend pass (the
# boost::beast HTTP session coroutine produces irreducible control flow
# containing EH pads — the same LLVM bug that disables `HttpTest`).
if (NOT _EMSCRIPTEN_NO_INDEXBUILDER_AND_SERVER)
    add_library(server Server.cpp)
    qlever_target_link_libraries(server engine qlever_util index parser global http
    SortPerformanceEstimator qlever metrics)
endif ()
