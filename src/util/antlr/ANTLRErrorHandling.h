// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

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
// TODO<qup42> finalize and use/throw out
[[maybe_unused]] std::string colorError(const ExceptionMetadata& metadata) {
  if (metadata.startIndex < metadata.stopIndex) {
    auto first = metadata.query_.substr(0, metadata.startIndex);
    auto middle = metadata.query_.substr(
        metadata.startIndex, metadata.stopIndex - metadata.startIndex + 1);
    auto end = metadata.query_.substr(metadata.stopIndex + 1);

    return (first + "\x1b[1m\x1b[4m\x1b[31m" + middle + "\x1b[0m" + end);
  } else {
    return metadata.query_;
  }
}

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
