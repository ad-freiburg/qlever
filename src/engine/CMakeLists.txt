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
        Describe.cpp GraphStoreProtocol.cpp
        QueryExecutionContext.cpp ExistsJoin.cpp SparqlProtocol.cpp ParsedRequestBuilder.cpp
        NeutralOptional.cpp Load.cpp StripColumns.cpp)
qlever_target_link_libraries(engine util index parser sparqlExpressions SortPerformanceEstimator Boost::iostreams s2 spatialjoin-dev pb_util)

# The `http` library currently is C++20-only. It is only required by the `Service` and `Load`
# operations, which are disabled in C++-17 mode anyway. We therefore only link `engine` against
# `http` if we are in the full C++-20 mode.
if (NOT STRIP_FEATURES_CPP_17)
    target_link_libraries(engine http)
endif()

add_library(server Server.cpp)
qlever_target_link_libraries(server engine util index parser http SortPerformanceEstimator)

