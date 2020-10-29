add_library(expressionParser
        SparqlLexer.h SparqlLexer.cpp
        SparqlParser.h SparqlParser.cpp
        SparqlCustomVisitor.h SparqlCustomVisitor.cpp)


add_subdirectory(expressionModel)