add_library(parser
              SparqlParser.h SparqlParser.cpp
              ParsedQuery.h ParsedQuery.cpp
              ParseException.h
              TsvParser.h TsvParser.cpp
              NTriplesParser.h NTriplesParser.cpp
              TurtleParser.h TurtleParser.cpp
            	ContextFileParser.cpp ContextFileParser.h)
