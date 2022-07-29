//
// Created by johannes on 16.05.21.
//

#include "SparqlParserHelpers.h"

#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {
using std::string;

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(string input)
    : input_{std::move(input)}, visitor_{} {
  parser_.setErrorHandler(std::make_shared<ThrowingErrorStrategy>());
  // The default in ANTLR is to log all errors to the console and to continue
  // the parsing. We need to turn parse errors into exceptions instead to
  // propagate them to the user.
  parser_.removeErrorListeners();
  parser_.addErrorListener(&errorListener_);
  lexer_.removeErrorListeners();
  lexer_.addErrorListener(&errorListener_);
}

// _____________________________________________________________________________
ParserAndVisitor::ParserAndVisitor(string input,
                                   SparqlQleverVisitor::PrefixMap prefixes)
    : ParserAndVisitor{std::move(input)} {
  visitor_.setPrefixMapManually(std::move(prefixes));
}
}  // namespace sparqlParserHelpers
