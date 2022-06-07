// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

#include "../../parser/ParseException.h"
#include "BaseErrorListener.h"
#include "DefaultErrorStrategy.h"
#include "RecognitionException.h"

struct ThrowingErrorStrategy : public antlr4::DefaultErrorStrategy {
  void reportError(antlr4::Parser*,
                   const antlr4::RecognitionException& e) override {
    throw antlr4::ParseCancellationException{
        e.what() + std::string{" at token \""} +
        e.getOffendingToken()->getText() + '"'};
  }
};

/**
 * antlr::ANTLRErrorListener that raises encountered syntaxErrors as
 * ParseException. The line, position in the line and antlr error message are
 * included as exception cause.
 */
struct ThrowingErrorListener : public antlr4::BaseErrorListener {
  void syntaxError([[maybe_unused]] antlr4::Recognizer* recognizer,
                   [[maybe_unused]] antlr4::Token* offendingSymbol, size_t line,
                   size_t charPositionInLine, const std::string& msg,
                   [[maybe_unused]] std::exception_ptr e) override {
    throw ParseException{
        absl::StrCat("line ", line, ":", charPositionInLine, " ", msg)};
  }
};
