add_subdirectory(sparqlExpressions)
add_library(SortPerformanceEstimator SortPerformanceEstimator.cpp)
qlever_target_link_libraries(SortPerformanceEstimator)
add_library(engine
        Engine.cpp QueryExecutionTree.cpp Operation.cpp ResultTable.cpp LocalVocab.cpp
        IndexScan.cpp Join.cpp Sort.cpp TextOperationWithoutFilter.cpp
        TextOperationWithFilter.cpp Distinct.cpp OrderBy.cpp Filter.cpp
        Server.cpp QueryPlanner.cpp QueryPlanningCostFactors.cpp
        OptionalJoin.cpp CountAvailablePredicates.cpp GroupBy.cpp HasPredicateScan.cpp
        Union.cpp MultiColumnJoin.cpp TransitivePath.cpp Service.cpp
        Values.cpp Bind.cpp Minus.cpp RuntimeInformation.cpp CheckUsePatternTrick.cpp
        VariableToColumnMap.cpp ExportQueryExecutionTrees.cpp )
qlever_target_link_libraries(engine util index parser sparqlExpressions http SortPerformanceEstimator Boost::iostreams)
