// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

#include "../../parser/ParseException.h"
#include "BaseErrorListener.h"
#include "CharStream.h"
#include "CommonTokenStream.h"
#include "DefaultErrorStrategy.h"
#include "ParserRuleContext.h"
#include "RecognitionException.h"
#include "Recognizer.h"
#include "TokenSource.h"

struct [[deprecated("Using ErrorHandlers is not recommend. Use ErrorListeners instead.")]] ThrowingErrorStrategy : public antlr4::DefaultErrorStrategy {
  void reportError(antlr4::Parser*,
                   const antlr4::RecognitionException& e) override {
    throw antlr4::ParseCancellationException{
        e.what() + std::string{" at token \""} +
        e.getOffendingToken()->getText() + '"'};
  }
};

namespace {
std::string colorError(const ExceptionMetadata& metadata) {
  if (metadata.startIndex > metadata.stopIndex) {
    auto first = metadata.query_.substr(0, metadata.startIndex);
    auto middle = metadata.query_.substr(
        metadata.startIndex, metadata.stopIndex - metadata.startIndex + 1);
    auto end = metadata.query_.substr(metadata.stopIndex + 1);

    //    return (first + "\x1b[1m\x1b[4m\x1b[31m" + middle +"\x1b[0m" + end);
    return (first + "**" + middle + "**" + end);
  } else {
    return metadata.query_;
  }
}

ExceptionMetadata generateMetadata(antlr4::Recognizer* recognizer,
                                   antlr4::Token* offendingToken, size_t line,
                                   size_t charPositionInLine) {
  return {((antlr4::CommonTokenStream*)recognizer->getInputStream())
              ->getTokenSource()
              ->getInputStream()
              ->toString(),
          offendingToken->getStartIndex(), offendingToken->getStopIndex()};
}

ExceptionMetadata generateMetadata(antlr4::ParserRuleContext* ctx) {
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
    // Try out displaying the error with ANSI Highlighting.
    std::cout << colorError(generateMetadata(recognizer, offendingSymbol, line,
                                             charPositionInLine))
              << std::endl;
    throw ParseException{
        absl::StrCat("line ", line, ":", charPositionInLine, " ", msg),
        generateMetadata(recognizer, offendingSymbol, line,
                         charPositionInLine)};
  }
};

class IVisitorErrorReporter {
 public:
  virtual ~IVisitorErrorReporter() = default;
  virtual void ReportError(antlr4::ParserRuleContext* ctx, std::string msg) = 0;
};

class QleverErrorReporter : public IVisitorErrorReporter {
 public:
  void ReportError(antlr4::ParserRuleContext* ctx,
                   [[maybe_unused]] std::string msg) override {
    //      std::cout << colorError(generateMetadata(ctx)) << std::endl;
    throw ParseException{absl::StrCat("line", ctx->getStart()->getLine(), ":",
                                      ctx->getStart()->getCharPositionInLine(), " ", msg),
                         generateMetadata(ctx)};
  }
};
