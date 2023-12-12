add_library(sparqlExpressions
        SparqlExpressionValueGetters.cpp
        NaryExpression.cpp
        SetOfIntervals.cpp
        SparqlExpressionPimpl.cpp
        SampleExpression.cpp
        RelationalExpressions.cpp AggregateExpression.cpp RegexExpression.cpp
        LangExpression.cpp NumericUnaryExpressions.cpp NumericBinaryExpressions.cpp DateExpressions.cpp StringExpressions.cpp
        ConditionalExpressions.cpp)

qlever_target_link_libraries(sparqlExpressions index Boost::url)
