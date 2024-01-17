add_library(rdfEscaping RdfEscaping.h RdfEscaping.cpp)
qlever_target_link_libraries(rdfEscaping)

add_subdirectory(sparqlParser)
add_subdirectory(data)

add_library(parser
        sparqlParser/SparqlQleverVisitor.cpp
        SparqlParser.cpp
        ParsedQuery.cpp
        TurtleParser.cpp
        Tokenizer.cpp
        ContextFileParser.cpp
        TurtleTokenId.h
        ParallelBuffer.cpp
        SparqlParserHelpers.cpp
        TripleComponent.cpp
        GraphPatternOperation.cpp
        PropertyPath.cpp
        data/SparqlFilter.cpp
        SelectClause.cpp
        GraphPatternOperation.cpp
        # The `Variable.cpp` from the subdirectory is linked here because otherwise we get linking errors.
        GraphPattern.cpp data/VariableToColumnMapPrinters.cpp)
qlever_target_link_libraries(parser sparqlParser parserData sparqlExpressions rdfEscaping re2::re2 util engine)

