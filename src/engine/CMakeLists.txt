add_subdirectory(sparqlExpressions)
add_library(SortPerformanceEstimator SortPerformanceEstimator.cpp SortPerformanceEstimator.h)
qlever_target_link_libraries(SortPerformanceEstimator)
add_library(engine
        Engine.cpp
        IndexSequence.h
        QueryExecutionTree.h QueryExecutionTree.cpp
        Operation.h Operation.cpp
        ../global/Id.h
        ../global/Constants.h
        ../util/Socket.h
        Comparators.h
        ResultTable.h ResultTable.cpp
        LocalVocab.h LocalVocab.cpp
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
        OptionalJoin.cpp OptionalJoin.h
        CountAvailablePredicates.cpp CountAvailablePredicates.h
        GroupBy.cpp GroupBy.h
        HasPredicateScan.cpp HasPredicateScan.h
        Union.cpp Union.h
        MultiColumnJoin.cpp MultiColumnJoin.h
        TransitivePath.cpp TransitivePath.h
        Service.cpp Service.h
        Values.cpp Values.h
        Bind.cpp Bind.h
        idTable/IdTable.h
        ../util/Random.h
        Minus.h Minus.cpp
        ResultType.h
        ../util/Parameters.h RuntimeInformation.cpp CheckUsePatternTrick.cpp CheckUsePatternTrick.h
        VariableToColumnMap.cpp ExportQueryExecutionTrees.cpp )

qlever_target_link_libraries(engine index parser sparqlExpressions http SortPerformanceEstimator ${ICU_LIBRARIES} boost_iostreams)
