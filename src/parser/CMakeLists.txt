add_library(rdfEscaping RdfEscaping.h RdfEscaping.cpp)
qlever_target_link_libraries(rdfEscaping)

add_subdirectory(sparqlParser)
add_subdirectory(data)

add_library(parser
        sparqlParser/SparqlQleverVisitor.cpp
        SparqlParser.h SparqlParser.cpp
        ParsedQuery.h ParsedQuery.cpp
        ParseException.h
        TurtleParser.h TurtleParser.cpp
        Tokenizer.h Tokenizer.cpp
        ContextFileParser.cpp ContextFileParser.h
        ParallelParseBuffer.h
        TokenizerCtre.h TurtleTokenId.h
        ParallelBuffer.cpp
        SparqlParserHelpers.h SparqlParserHelpers.cpp
        TripleComponent.h TripleComponent.cpp
        GraphPatternOperation.cpp
        PropertyPath.h PropertyPath.cpp Alias.h data/SolutionModifiers.h
        data/LimitOffsetClause.h data/SparqlFilter.h data/SparqlFilter.cpp
        data/OrderKey.h data/GroupKey.h ParseException.cpp SelectClause.cpp
        SelectClause.h GraphPatternOperation.cpp GraphPatternOperation.h
        # The `Variable.cpp` from the subdirectory is linked here because otherwise we get linking errors.
        GraphPattern.cpp GraphPattern.h ConstructClause.h data/VariableToColumnMapPrinters.cpp)
qlever_target_link_libraries(parser sparqlParser parserData sparqlExpressions rdfEscaping re2 util engine)

