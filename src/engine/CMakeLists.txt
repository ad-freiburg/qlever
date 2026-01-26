add_subdirectory(sparqlExpressions)
add_library(SortPerformanceEstimator SortPerformanceEstimator.cpp)
qlever_target_link_libraries(SortPerformanceEstimator parser)
add_library(engine
        Engine.cpp QueryExecutionTree.cpp Operation.cpp Result.cpp LocalVocab.cpp
        IndexScan.cpp Join.cpp Sort.cpp
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
        PermutationSelector.cpp)

qlever_target_link_libraries(engine util index parser global sparqlExpressions SortPerformanceEstimator Boost::iostreams s2 spatialjoin-dev pb_util pb_util_geo)

# When `REDUCED_FEATURE_SET_FOR_CPP17` is set, do not link `engine` against the
# `http` library (which requires C++20). This works because `http` is only used
# in two operations, `Service` and `Load`, which we disable when
# `REDUCED_FEATURE_SET_FOR_CPP17` is set.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    target_link_libraries(engine http)
endif()

add_library(server Server.cpp)
qlever_target_link_libraries(server engine util index parser global http SortPerformanceEstimator)
