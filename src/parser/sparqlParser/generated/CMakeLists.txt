add_library(antlrSparqlParser
        SparqlAutomaticBaseListener.h SparqlAutomaticBaseListener.cpp
        SparqlAutomaticLexer.h SparqlAutomaticLexer.cpp
        SparqlAutomaticListener.h SparqlAutomaticListener.cpp
        SparqlAutomaticParser.h SparqlAutomaticParser.cpp
        SparqlAutomaticVisitor.h SparqlAutomaticVisitor.cpp)

target_link_libraries(antlrSparqlParser antlr4_static)