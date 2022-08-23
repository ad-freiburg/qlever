//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include <util/antlr/ANTLRErrorHandling.h>

#include "CharStream.h"
#include "CommonTokenStream.h"
#include "TokenSource.h"

// _____________________________________________________________________________
ExceptionMetadata generateMetadata(antlr4::Recognizer* recognizer,
                                   antlr4::Token* offendingToken,
                                   [[maybe_unused]] size_t line,
                                   [[maybe_unused]] size_t charPositionInLine) {
  return {((antlr4::CommonTokenStream*)recognizer->getInputStream())
              ->getTokenSource()
              ->getInputStream()
              ->toString(),
          offendingToken->getStartIndex(), offendingToken->getStopIndex()};
}

// _____________________________________________________________________________
ExceptionMetadata generateMetadata(antlr4::ParserRuleContext* ctx) {
  return {ctx->getStart()->getInputStream()->toString(),
          ctx->getStart()->getStartIndex(), ctx->getStop()->getStopIndex()};
}

// _____________________________________________________________________________
void ThrowingErrorListener::syntaxError(
    [[maybe_unused]] antlr4::Recognizer* recognizer,
    [[maybe_unused]] antlr4::Token* offendingSymbol, size_t line,
    size_t charPositionInLine, const std::string& msg,
    [[maybe_unused]] std::exception_ptr e) {
  throw ParseException{
      absl::StrCat("Token \"", offendingSymbol->getText(), "\" at line ", line,
                   ":", charPositionInLine, " ", msg),
      generateMetadata(recognizer, offendingSymbol, line, charPositionInLine)};
}
