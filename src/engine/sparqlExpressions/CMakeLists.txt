add_library(sparqlExpressions
        SparqlExpressionValueGetters.cpp
        NaryExpression.cpp
        SetOfIntervals.cpp
        SparqlExpressionPimpl.cpp
        SampleExpression.cpp
        RelationalExpressions.cpp
        AggregateExpression.cpp
        RegexExpression.cpp
        NumericUnaryExpressions.cpp
        NumericBinaryExpressions.cpp
        DateExpressions.cpp
        StringExpressions.cpp
        IsSomethingExpressions.cpp
        ConditionalExpressions.cpp
        SparqlExpressionTypes.cpp
        SparqlExpression.cpp
        ConvertToNumericExpression.cpp
        RdfTermExpressions.cpp
        LangExpression.cpp
        CountStarExpression.cpp)

qlever_target_link_libraries(sparqlExpressions util index Boost::url)
