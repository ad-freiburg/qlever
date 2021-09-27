add_library(sparqlParser SparqlQleverVisitor.h SparqlQleverVisitor.cpp
        generated/SparqlAutomaticBaseListener.h generated/SparqlAutomaticBaseListener.cpp
        generated/SparqlAutomaticLexer.h generated/SparqlAutomaticLexer.cpp
        generated/SparqlAutomaticListener.h generated/SparqlAutomaticListener.cpp
        generated/SparqlAutomaticParser.h generated/SparqlAutomaticParser.cpp
        generated/SparqlAutomaticVisitor.h generated/SparqlAutomaticVisitor.cpp)
target_link_libraries(sparqlParser antlr4_static sparqlExpressions rdfEscaping absl::flat_hash_map)
