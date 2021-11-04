//
// Created by johannes on 16.05.21.
//

#ifndef QLEVER_SPARQLPARSERHELPERS_H
#define QLEVER_SPARQLPARSERHELPERS_H

#include <memory>
#include <string>

#include "../engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "./ParsedQuery.h"

namespace sparqlParserHelpers {

template <typename ResultOfParse>
struct ResultOfParseAndRemainingText {
  ResultOfParseAndRemainingText(ResultOfParse&& resultOfParse,
                                std::string&& remainingText)
      : _resultOfParse{std::move(resultOfParse)},
        _remainingText{std::move(remainingText)} {}
  ResultOfParse _resultOfParse;
  std::string _remainingText;
};

// ____________________________________________________________________________
ResultOfParseAndRemainingText<sparqlExpression::SparqlExpressionPimpl>
parseExpression(const std::string& input);

// An `alias` in Sparql have the form (<expression> as ?variable).
ResultOfParseAndRemainingText<ParsedQuery::Alias> parseAlias(
    const std::string& input);
}  // namespace sparqlParserHelpers

#endif  // QLEVER_SPARQLPARSERHELPERS_H
