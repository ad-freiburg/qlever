add_library(sparqlExpressions
        SparqlExpressionTypes.h
        SparqlExpression.h
        AggregateExpression.h
        GroupConcatExpression.h
        SparqlExpressionGenerators.h
        SparqlExpressionValueGetters.h SparqlExpressionValueGetters.cpp
        NaryExpression.h NaryExpression.cpp
        SetOfIntervals.h SetOfIntervals.cpp
        LiteralExpression.h GroupConcatExpression.h
        SparqlExpressionPimpl.h SparqlExpressionPimpl.cpp
        SampleExpression.h SampleExpression.cpp
        RelationalExpressions.cpp AggregateExpression.cpp RegexExpression.cpp
        LangExpression.cpp)

target_link_libraries(sparqlExpressions index)
