// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2021 Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//   2022 Julian Mundhahs (mundhahj@tf.informatik.uni-freiburg.de)

#pragma once

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

/**
 * antlr::ANTLRErrorListener that raises encountered syntaxErrors as
 * ParseException. The line, position in the line and antlr error message are
 * included as exception cause.
 */
struct ThrowingErrorListener : public antlr4::BaseErrorListener {
  void syntaxError(antlr4::Recognizer* recognizer,
                   antlr4::Token* offendingSymbol, size_t line,
                   size_t charPositionInLine, const std::string& msg,
                   std::exception_ptr e) override;
};
