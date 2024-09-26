add_subdirectory(sparqlExpressions)
add_library(SortPerformanceEstimator SortPerformanceEstimator.cpp)
qlever_target_link_libraries(SortPerformanceEstimator)
add_library(engine
        Engine.cpp QueryExecutionTree.cpp Operation.cpp Result.cpp LocalVocab.cpp
        IndexScan.cpp Join.cpp Sort.cpp
        Distinct.cpp OrderBy.cpp Filter.cpp
        Server.cpp QueryPlanner.cpp QueryPlanningCostFactors.cpp
        OptionalJoin.cpp CountAvailablePredicates.cpp GroupBy.cpp HasPredicateScan.cpp
        Union.cpp MultiColumnJoin.cpp TransitivePathBase.cpp
        TransitivePathHashMap.cpp TransitivePathBinSearch.cpp Service.cpp
        Values.cpp Bind.cpp Minus.cpp RuntimeInformation.cpp CheckUsePatternTrick.cpp
        VariableToColumnMap.cpp ExportQueryExecutionTrees.cpp
        CartesianProductJoin.cpp TextIndexScanForWord.cpp TextIndexScanForEntity.cpp
        TextLimit.cpp LocalVocabEntry.cpp LazyGroupBy.cpp GroupByHashMapOptimization.cpp)
qlever_target_link_libraries(engine util index parser sparqlExpressions http SortPerformanceEstimator Boost::iostreams)
