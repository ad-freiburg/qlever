add_library(sparqlParser
        SparqlBaseListener.h SparqlBaseListener.cpp
        SparqlQleverVisitor.h SparqlQleverVisitor.cpp
        SparqlLexer.h SparqlLexer.cpp
        SparqlListener.h SparqlListener.cpp
        SparqlParser.h SparqlParser.cpp
        SparqlVisitor.h SparqlVisitor.cpp)

target_link_libraries(sparqlParser antlr4_static)