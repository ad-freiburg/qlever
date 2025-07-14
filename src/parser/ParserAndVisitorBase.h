//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_PARSERANDVISITORBASE_H
#define QLEVER_SRC_PARSER_PARSERANDVISITORBASE_H

#include <string>
#include <utility>

#include "sparqlParser/generated/SparqlAutomaticLexer.h"
#include "sparqlParser/generated/SparqlAutomaticParser.h"
#include "util/antlr/ANTLRErrorHandling.h"
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
// `Visitor`. The most common usage is the `ParserAndVisitor` class in
// `SparqlParserHelpers.h`, the only other usage currently is in `Variable.cpp`
// where a custom visitor is used to check valid variable names without
// depending on the rather larger `SparqlQleverVisitor`.
template <typename Visitor>
struct ParserAndVisitorBase {
 private:
  std::string input_;
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
}  // namespace sparqlParserHelpers
#endif  // QLEVER_SRC_PARSER_PARSERANDVISITORBASE_H
