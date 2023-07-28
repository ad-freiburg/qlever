// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2021 Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//   2022 Julian Mundhahs (mundhahj@tf.informatik.uni-freiburg.de)

#pragma once

#include <concepts>
#include <string>

#include "BaseErrorListener.h"
#include "ParserRuleContext.h"
#include "Recognizer.h"
#include "Token.h"
#include "absl/strings/str_cat.h"
#include "util/ParseException.h"

ExceptionMetadata generateMetadata(antlr4::Recognizer* recognizer,
                                   antlr4::Token* offendingToken, size_t line,
                                   size_t charPositionInLine);

ExceptionMetadata generateMetadata(antlr4::ParserRuleContext* ctx);

/*
antlr::ANTLRErrorListener that raises encountered syntaxErrors as
exceptions of type `GrammarParseException`. The line, position in the line and
antlr error message are included as exception cause.

For an example of a valid `GrammarParseException` see
`InvalidSparqlQueryException`.
*/
template <typename GrammarParseException>
requires std::derived_from<GrammarParseException, ParseException> &&
         std::constructible_from<GrammarParseException, std::string_view,
                                 std::optional<ExceptionMetadata>>
struct ThrowingErrorListener : public antlr4::BaseErrorListener {
  void syntaxError(antlr4::Recognizer* recognizer,
                   antlr4::Token* offendingSymbol, size_t line,
                   size_t charPositionInLine, const std::string& msg,
                   [[maybe_unused]] std::exception_ptr e) override {
    throw GrammarParseException{generateExceptionMessage(offendingSymbol, msg),
                                generateMetadata(recognizer, offendingSymbol,
                                                 line, charPositionInLine)};
  }

 private:
  std::string generateExceptionMessage(antlr4::Token* offendingSymbol,
                                       const std::string& msg) {
    if (!offendingSymbol) {
      return msg;
    } else if (offendingSymbol->getStartIndex() ==
               offendingSymbol->getStopIndex() + 1) {
      // This can only happen at the end of the query when a token is expected,
      // but none is found. The offending token is then empty.
      return absl::StrCat("Unexpected end of Query: ", msg);
    } else {
      return absl::StrCat("Token \"", offendingSymbol->getText(), "\": ", msg);
    }
  }
};
