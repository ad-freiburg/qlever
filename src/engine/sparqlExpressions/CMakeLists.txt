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
        SparqlExpressionPimpl.h SparqlExpressionPimpl.cpp
        SampleExpression.h SampleExpression.cpp NaryExpression.cpp AggregateExpression.cpp)

target_link_libraries(sparqlExpressions index)
