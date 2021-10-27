add_library(sparqlExpressions
        SparqlExpressionTypes.h
        SparqlExpression.h
        AggregateExpression.h
        GroupConcatExpression.h
        SparqlExpressionGenerators.h
        SparqlExpressionValueGetters.h SparqlExpressionValueGetters.cpp
        NaryExpression.h
        SetOfIntervals.h SetOfIntervals.cpp
        LiteralExpression.h GroupConcatExpression.h
        RelationalExpression.h RelationalExpression.cpp
        SampleExpression.h SampleExpression.cpp
        SparqlExpressionPimpl.h SparqlExpressionPimpl.cpp)

target_link_libraries(sparqlExpressions index)