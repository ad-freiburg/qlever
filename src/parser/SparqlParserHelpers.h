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
  ad_utility::antlr_utility::ThrowingErrorListener<InvalidSparqlQueryException>
      errorListener_{};

  // Unescapes unicode sequences like \U01234567 and \u0123 in the input string
  // before beginning with actual parsing as the SPARQL standard mandates.
  static std::string unescapeUnicodeSequences(std::string input);

 public:
  SparqlAutomaticParser parser_{&tokens_};
  SparqlQleverVisitor visitor_;
  explicit ParserAndVisitor(
      string input,
      std::optional<ParsedQuery::DatasetClauses> datasetClauses = std::nullopt,
      SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
          SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False);
  ParserAndVisitor(
      string input, SparqlQleverVisitor::PrefixMap prefixes,
      std::optional<ParsedQuery::DatasetClauses> datasetClauses = std::nullopt,
      SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
          SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False);

  template <typename ContextType>
  auto parseTypesafe(ContextType* (SparqlAutomaticParser::*F)(void)) {
    auto resultOfParse = visitor_.visit(std::invoke(F, parser_));

    // The `startIndex()` denotes the index of a Unicode codepoint, but `input_`
    // is UTF-8 encoded.
    auto remainingString = ad_utility::getUTF8Substring(
        input_, parser_.getCurrentToken()->getStartIndex());
    return ResultOfParseAndRemainingText{std::move(resultOfParse),
                                         std::string{remainingString}};
  }
};
}  // namespace sparqlParserHelpers

#endif  // QLEVER_SPARQLPARSERHELPERS_H
