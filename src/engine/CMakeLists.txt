add_subdirectory(sparqlExpressions)
add_library(SortPerformanceEstimator SortPerformanceEstimator.cpp SortPerformanceEstimator.h)
add_library(engine
        Engine.h
        IndexSequence.h
        QueryExecutionTree.h QueryExecutionTree.cpp
        Operation.h Operation.cpp
        ../global/Id.h
        ../global/Constants.h
        ../util/Socket.h
        Comparators.h
        ResultTable.h ResultTable.cpp
        QueryExecutionContext.h
        IndexScan.h IndexScan.cpp
        Join.h Join.cpp
        Sort.h Sort.cpp
        TextOperationWithoutFilter.h TextOperationWithoutFilter.cpp
        TextOperationWithFilter.h TextOperationWithFilter.cpp
        Distinct.h Distinct.cpp
        OrderBy.h OrderBy.cpp
        Filter.h Filter.cpp
        Server.h Server.cpp
        QueryPlanner.cpp QueryPlanner.h
        QueryPlanningCostFactors.cpp QueryPlanningCostFactors.h
        TwoColumnJoin.cpp TwoColumnJoin.h
        OptionalJoin.cpp OptionalJoin.h
        CountAvailablePredicates.cpp CountAvailablePredicates.h
        GroupBy.cpp GroupBy.h
        HasPredicateScan.cpp HasPredicateScan.h
        Union.cpp Union.h
        MultiColumnJoin.cpp MultiColumnJoin.h
        TransitivePath.cpp TransitivePath.h
        Values.cpp Values.h
        Bind.cpp Bind.h
        IdTable.h
        ../util/Random.h
        Minus.h Minus.cpp
        ResultType.h)

target_link_libraries(engine index parser sparqlExpressions SortPerformanceEstimator absl::flat_hash_set ${ICU_LIBRARIES})
