// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2021 Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//   2022 Julian Mundhahs (mundhahj@tf.informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_ANTLR_ANTLRERRORHANDLING_H
#define QLEVER_SRC_UTIL_ANTLR_ANTLRERRORHANDLING_H

#include <concepts>
#include <string>

#include "BaseErrorListener.h"
#include "Recognizer.h"
#include "Token.h"
#include "absl/strings/str_cat.h"
#include "backports/concepts.h"
#include "util/ParseException.h"
#include "util/antlr/GenerateAntlrExceptionMetadata.h"

namespace ad_utility::antlr_utility {
namespace detail {
std::string generateExceptionMessage(antlr4::Token* offendingSymbol,
                                     const std::string& msg);
}  // namespace detail

/*
antlr::ANTLRErrorListener that raises encountered syntaxErrors as
exceptions of type `GrammarParseException`. The line, position in the line and
antlr error message are included as exception cause.

For an example of a valid `GrammarParseException` see
`InvalidSparqlQueryException`.
*/
CPP_template(typename GrammarParseException)(
    requires std::derived_from<GrammarParseException, ParseException> CPP_and
        std::constructible_from<
            GrammarParseException, std::string_view,
            std::optional<ExceptionMetadata>>) struct ThrowingErrorListener
    : public antlr4::BaseErrorListener {
  void syntaxError(antlr4::Recognizer* recognizer,
                   antlr4::Token* offendingSymbol, size_t line,
                   size_t charPositionInLine, const std::string& msg,
                   [[maybe_unused]] std::exception_ptr e) override {
    throw GrammarParseException{
        detail::generateExceptionMessage(offendingSymbol, msg),
        generateAntlrExceptionMetadata(recognizer, offendingSymbol, line,
                                       charPositionInLine)};
  }
};
}  // namespace ad_utility::antlr_utility

#endif  // QLEVER_SRC_UTIL_ANTLR_ANTLRERRORHANDLING_H
