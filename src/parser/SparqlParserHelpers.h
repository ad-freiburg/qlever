//
// Created by johannes on 16.05.21.
//

#ifndef QLEVER_SPARQLPARSERHELPERS_H
#define QLEVER_SPARQLPARSERHELPERS_H

#include <memory>
#include <string>

#include "../engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "../util/antlr/ANTLRErrorHandling.h"
#include "./ParsedQuery.h"
#include "sparqlParser/SparqlQleverVisitor.h"
#include "sparqlParser/generated/SparqlAutomaticLexer.h"

namespace sparqlParserHelpers {

template <typename ResultOfParse>
struct ResultOfParseAndRemainingText {
  ResultOfParse resultOfParse_;
  std::string remainingText_;
  ResultOfParseAndRemainingText(ResultOfParse&& resultOfParse,
                                std::string&& remainingText)
      : resultOfParse_{std::move(resultOfParse)},
        remainingText_{std::move(remainingText)} {}
};

struct ParserAndVisitor {
 private:
  string input_;
  antlr4::ANTLRInputStream stream_{input_};
  SparqlAutomaticLexer lexer_{&stream_};
  antlr4::CommonTokenStream tokens_{&lexer_};
  ThrowingErrorListener errorListener_{};

 public:
  SparqlAutomaticParser parser_{&tokens_};
  SparqlQleverVisitor visitor_;
  explicit ParserAndVisitor(string input);
  ParserAndVisitor(string input, SparqlQleverVisitor::PrefixMap prefixes);

  template <typename ContextType>
  auto parseTypesafe(ContextType* (SparqlAutomaticParser::*F)(void)) {
    auto resultOfParse = visitor_.visit(std::invoke(F, parser_));

    auto remainingString =
        input_.substr(parser_.getCurrentToken()->getStartIndex());
    return ResultOfParseAndRemainingText{std::move(resultOfParse),
                                         std::string{remainingString}};
  }
};
}  // namespace sparqlParserHelpers

#endif  // QLEVER_SPARQLPARSERHELPERS_H
