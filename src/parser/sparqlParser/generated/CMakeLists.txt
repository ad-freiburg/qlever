add_library(antlrSparqlParser
        SparqlBaseListener.h SparqlBaseListener.cpp
        SparqlLexer.h SparqlLexer.cpp
        SparqlListener.h SparqlListener.cpp
        SparqlParser.h SparqlParser.cpp
        SparqlVisitor.h SparqlVisitor.cpp)

target_link_libraries(antlrSparqlParser antlr4_static)