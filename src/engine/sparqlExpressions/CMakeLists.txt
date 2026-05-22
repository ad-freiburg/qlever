add_library(sparqlExpressions
        SparqlExpressionValueGetters.cpp
        SetOfIntervals.cpp
        SparqlExpressionPimpl.cpp
        SampleExpression.cpp
        RelationalExpressions.cpp
        AggregateExpression.cpp
        StdevExpression.cpp
        RegexExpression.cpp
        NumericUnaryExpressions.cpp
        NumericBinaryExpressions.cpp
        DateExpressions.cpp
        StringExpressions.cpp
        IsSomethingExpressions.cpp
        ConditionalExpressions.cpp
        SparqlExpressionTypes.cpp
        SparqlExpression.cpp
        ConvertToDtypeConstructor.cpp
        RdfTermExpressions.cpp
        LangExpression.cpp
        CountStarExpression.cpp
        PrefilterExpressionIndex.cpp
        GeoExpression.cpp
        BlankNodeExpression.cpp
        GroupConcatExpression.cpp)

# SortPerformanceEstimator is needed by CountStarExpression for the
# sort-before-distinct timeout guard in COUNT(DISTINCT *).
qlever_target_link_libraries(sparqlExpressions util index SortPerformanceEstimator)
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    target_link_libraries(sparqlExpressions PUBLIC Boost::url)
endif()
