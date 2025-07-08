//  Copyright 2022-2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

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

// A class that combines a SPARQL parser + visitor. It is templated on the
// `Visitor`. The most common usage is the `ParserAndVisitor` class below, the
// only other usage Currently is in `Variable.cpp` where a custom visitor is
// used to check valid variable names without depending on the rather larger
// `SparqlQleverVisitor`.
template <typename Visitor>
struct ParserAndVisitorBase {
 private:
  string input_;
  antlr4::ANTLRInputStream stream_{input_};
  SparqlAutomaticLexer lexer_{&stream_};
  antlr4::CommonTokenStream tokens_{&lexer_};
  ad_utility::antlr_utility::ThrowingErrorListener<InvalidSparqlQueryException>
      errorListener_{};

 public:
  SparqlAutomaticParser parser_{&tokens_};
  Visitor visitor_;
  explicit ParserAndVisitorBase(std::string input, Visitor visitor = {})
      : input_{std::move(input)}, visitor_{std::move(visitor)} {
    // The default in ANTLR is to log all errors to the console and to continue
    // the parsing. We need to turn parse errors into exceptions instead to
    // propagate them to the user.
    parser_.removeErrorListeners();
    parser_.addErrorListener(&errorListener_);
    lexer_.removeErrorListeners();
    lexer_.addErrorListener(&errorListener_);
  }

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

// The actual `ParserAndVisitor` class that can be used to fully parse SPARQL
// using the automatically generated parser + the manually written
// `SparqlQLeverVisitor`.
struct ParserAndVisitor : public ParserAndVisitorBase<SparqlQleverVisitor> {
 private:
  // Unescapes unicode sequences like \U01234567 and \u0123 in the input string
  // before beginning with actual parsing as the SPARQL standard mandates.
  static std::string unescapeUnicodeSequences(std::string input);

  using Base = ParserAndVisitorBase<SparqlQleverVisitor>;

 public:
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
};
}  // namespace sparqlParserHelpers

#endif  // QLEVER_SPARQLPARSERHELPERS_H
