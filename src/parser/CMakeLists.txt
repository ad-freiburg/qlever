add_library(rdfEscaping RdfEscaping.h RdfEscaping.cpp)
target_link_libraries(rdfEscaping absl::flat_hash_map ${ICU_LIBRARIES})

add_subdirectory(sparqlParser)

add_library(parser
        SparqlParser.h SparqlParser.cpp
        ParsedQuery.h ParsedQuery.cpp
        ParseException.h
        TurtleParser.h TurtleParser.cpp
        Tokenizer.h Tokenizer.cpp
        ContextFileParser.cpp ContextFileParser.h
        ParallelParseBuffer.h
        SparqlLexer.h SparqlLexer.cpp
        TokenizerCtre.h TurtleTokenId.h
        ParallelBuffer.cpp
        SparqlParserHelpers.h SparqlParserHelpers.cpp
        TripleComponent.h
        PropertyPath.h PropertyPath.cpp Alias.h data/SolutionModifiers.h data/LimitOffsetClause.h data/SparqlFilter.h data/SparqlFilter.cpp data/OrderKey.h data/GroupKey.h ParseException.cpp)
target_link_libraries(parser sparqlParser sparqlExpressions rdfEscaping re2 absl::flat_hash_map util)
