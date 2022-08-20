// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2021 Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//   2022 Julian Mundhahs (mundhahj@tf.informatik.uni-freiburg.de)

#pragma once

#include <parser/ParseException.h>

#include <string>

#include "BaseErrorListener.h"
#include "CharStream.h"
#include "CommonTokenStream.h"
#include "DefaultErrorStrategy.h"
#include "ParserRuleContext.h"
#include "RecognitionException.h"
#include "Recognizer.h"
#include "TokenSource.h"

// TODO<qup42> whats up with this namespace?
namespace {
ExceptionMetadata generateMetadata(antlr4::Recognizer* recognizer,
                                   antlr4::Token* offendingToken,
                                   [[maybe_unused]] size_t line,
                                   [[maybe_unused]] size_t charPositionInLine) {
  // TODO<qup42> the source query differs from the real query. The SparqlLexer
  //  lowercases some parts and sometimes adds whitespaces.
  return {((antlr4::CommonTokenStream*)recognizer->getInputStream())
              ->getTokenSource()
              ->getInputStream()
              ->toString(),
          offendingToken->getStartIndex(), offendingToken->getStopIndex()};
}

[[maybe_unused]] ExceptionMetadata generateMetadata(
    antlr4::ParserRuleContext* ctx) {
  // TODO<qup42> see above
  return {ctx->getStart()->getInputStream()->toString(),
          ctx->getStart()->getStartIndex(), ctx->getStop()->getStopIndex()};
}
}  // namespace

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
        absl::StrCat("Token \"", offendingSymbol->getText(), "\" at line ",
                     line, ":", charPositionInLine, " ", msg),
        generateMetadata(recognizer, offendingSymbol, line,
                         charPositionInLine)};
  }
};
