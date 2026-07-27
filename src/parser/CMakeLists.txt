
add_subdirectory(sparqlParser)
add_subdirectory(data)

add_library(basicParser Tokenizer.cpp LiteralOrIri.cpp WordsAndDocsFileParser.cpp AsyncBlockSource.cpp)
qlever_target_link_libraries(basicParser qlever_util rdfTypes parserData)

# TODO: Consider moving files that don't depend on the engine (sparqlExpressions,
# index) into basicParser or a new intermediate library to clean up the
# dependency graph.
add_library(parser
        sparqlParser/SparqlQleverVisitor.cpp
        SparqlParser.cpp
        ParsedQuery.cpp
        SparqlParserHelpers.cpp
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
        ExternalValuesQuery.cpp
        VariableCounter.cpp
)
qlever_target_link_libraries(parser basicParser sparqlParser parserData sparqlExpressions rdfEscaping global qlever_util index rdfTypes)
