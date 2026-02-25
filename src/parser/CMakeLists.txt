
add_subdirectory(sparqlParser)
add_subdirectory(data)

add_library(basicParser RdfParser.cpp ParallelBuffer.cpp Tokenizer.cpp)
qlever_target_link_libraries(basicParser util rdfTypes)

add_library(parser
        sparqlParser/SparqlQleverVisitor.cpp
        SparqlParser.cpp
        ParsedQuery.cpp
        SparqlParserHelpers.cpp
#        TripleComponent.cpp
        GraphPatternOperation.cpp
        MagicServiceQuery.cpp
        NamedCachedResult.cpp
        PathQuery.cpp
        SpatialQuery.cpp
        PayloadVariables.cpp
        PropertyPath.cpp
        data/SparqlFilter.cpp
        SelectClause.cpp
        GraphPatternOperation.cpp
        GraphPattern.cpp
        DatasetClauses.cpp
        TextSearchQuery.cpp
        MaterializedViewQuery.cpp
        Quads.cpp
        UpdateTriples.cpp
        MaterializedViewQuery.cpp
        GraphPatternAnalysis.cpp
)
qlever_target_link_libraries(parser basicParser sparqlParser parserData sparqlExpressions rdfEscaping global util index rdfTypes)
