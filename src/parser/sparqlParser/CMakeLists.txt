add_library(sparqlParser SparqlQleverVisitor.h SparqlQleverVisitor.cpp
generated/SparqlBaseListener.h generated/SparqlBaseListener.cpp
generated/SparqlLexer.h generated/SparqlLexer.cpp
generated/SparqlListener.h generated/SparqlListener.cpp
generated/SparqlParser.h generated/SparqlParser.cpp
generated/SparqlVisitor.h generated/SparqlVisitor.cpp)
target_link_libraries(sparqlParser antlr4_static rdfEscaping absl::flat_hash_map)
