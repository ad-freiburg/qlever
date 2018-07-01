add_library(engine
        Engine.h Engine.cpp
        IndexSequence.h
        QueryExecutionTree.h QueryExecutionTree.cpp
        Operation.h
        ../global/Id.h
        ../global/Constants.h
        ../util/Socket.h
        Comparators.h
        ResultTable.h ResultTable.cpp
        QueryExecutionContext.h
        IndexScan.h IndexScan.cpp
        Join.h Join.cpp
        Sort.h Sort.cpp
        TextOperationForContexts.h TextOperationForContexts.cpp
        TextOperationWithoutFilter.h TextOperationWithoutFilter.cpp
        TextOperationWithFilter.h TextOperationWithFilter.cpp
        Distinct.h Distinct.cpp
        OrderBy.h OrderBy.cpp
        Filter.h Filter.cpp
        Server.h Server.cpp
        QueryPlanner.cpp QueryPlanner.h
        ScanningJoin.cpp ScanningJoin.h
        QueryPlanningCostFactors.cpp QueryPlanningCostFactors.h
        TwoColumnJoin.cpp TwoColumnJoin.h
        OptionalJoin.cpp OptionalJoin.h
        CountAvailablePredicates.cpp CountAvailablePredicates.h
        GroupBy.cpp GroupBy.h
        HasRelationScan.cpp HasRelationScan.h
)

target_link_libraries(engine index parser)
