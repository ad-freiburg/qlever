
add_subdirectory(sparqlParser)
add_subdirectory(data)

add_library(parser
        sparqlParser/SparqlQleverVisitor.cpp
        SparqlParser.cpp
        ParsedQuery.cpp
        RdfParser.cpp
        Tokenizer.cpp
        WordsAndDocsFileParser.cpp
        TurtleTokenId.h
        ParallelBuffer.cpp
        SparqlParserHelpers.cpp
        TripleComponent.cpp
        GraphPatternOperation.cpp
        MagicServiceQuery.cpp
        PathQuery.cpp
        SpatialQuery.cpp
        PayloadVariables.cpp
        PropertyPath.cpp
        data/SparqlFilter.cpp
        SelectClause.cpp
        GraphPatternOperation.cpp
        GraphPattern.cpp
        LiteralOrIri.cpp
        DatasetClauses.cpp
        TextSearchQuery.cpp
        Quads.cpp
)
qlever_target_link_libraries(parser sparqlParser parserData sparqlExpressions rdfEscaping re2::re2 util engine index rdfTypes)

