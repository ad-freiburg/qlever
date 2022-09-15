//
// Created by johannes on 16.05.21.
//

#include "SparqlParserHelpers.h"

#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {
using std::string;

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(string_view input)
    : input_{input}, visitor_{} {
  // The default in ANTLR is to log all errors to the console and to continue
  // the parsing. We need to turn parse errors into exceptions instead to
  // propagate them to the user.
  parser_.removeErrorListeners();
  parser_.addErrorListener(&errorListener_);
  lexer_.removeErrorListeners();
  lexer_.addErrorListener(&errorListener_);
}

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(string_view input,
                                   SparqlQleverVisitor::PrefixMap prefixes)
    : ParserAndVisitor{input} {
  visitor_.setPrefixMapManually(std::move(prefixes));
}
}  // namespace sparqlParserHelpers
