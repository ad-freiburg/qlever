add_library(sparqlParser
        SparqlBaseListener.h SparqlBaseListener.cpp
        SparqlQleverVisitor.h SparqlQleverVisitor.cpp
        SparqlLexer.h SparqlLexer.cpp
        SparqlListener.h SparqlListener.cpp
        SparqlParser.h SparqlParser.cpp
        SparqlVisitor.h SparqlVisitor.cpp)

target_link_libraries(sparqlParser rdfEscaping antlr4_static absl::flat_hash_map)