//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "util/antlr/ANTLRErrorHandling.h"

// These are ANTLR headers.
#include <Lexer.h>
#include <Parser.h>

#include "util/Exception.h"

// _____________________________________________________________________________
ExceptionMetadata generateMetadata(antlr4::Parser* parser,
                                   antlr4::Token* offendingToken, size_t line,
                                   size_t charPositionInLine) {
  return {
      parser->getTokenStream()->getTokenSource()->getInputStream()->toString(),
      offendingToken->getStartIndex(), offendingToken->getStopIndex(), line,
      charPositionInLine};
}

// _____________________________________________________________________________
ExceptionMetadata generateMetadata(antlr4::Lexer* lexer, size_t line,
                                   size_t charPositionInLine) {
  return {lexer->getInputStream()->toString(), lexer->tokenStartCharIndex,
          lexer->tokenStartCharIndex, line, charPositionInLine};
}

// _____________________________________________________________________________
ExceptionMetadata generateMetadata(antlr4::Recognizer* recognizer,
                                   antlr4::Token* offendingToken, size_t line,
                                   size_t charPositionInLine) {
  // The abstract `antlr4::Recognizer` interface does not provide enough
  // information to generate the Metadata. For this reason we manually cast to
  // the subclasses. As of ANTLRv4 `antlr4::Parser` and `antlr4::Lexer` are the
  // only subclasses of `antlr4::Recognizer`.
  if (auto* parser = dynamic_cast<antlr4::Parser*>(recognizer)) {
    AD_CHECK(offendingToken != nullptr);
    return generateMetadata(parser, offendingToken, line, charPositionInLine);
  } else if (auto* lexer = dynamic_cast<antlr4::Lexer*>(recognizer)) {
    // If the recognizer is a Lexer this means that the error was a Lexer error.
    // In that case `offendingToken` is `nullptr`.
    return generateMetadata(lexer, line, charPositionInLine);
  } else {
    AD_FAIL();  // Should be unreachable.
  }
}

// _____________________________________________________________________________
ExceptionMetadata generateMetadata(antlr4::ParserRuleContext* ctx) {
  auto start = ctx->getStart();
  return {start->getInputStream()->toString(), ctx->getStart()->getStartIndex(),
          ctx->getStop()->getStopIndex(), start->getLine(),
          start->getCharPositionInLine()};
}

std::string generateExceptionMessage(antlr4::Token* offendingSymbol,
                                     const std::string& msg) {
  return (offendingSymbol)
             ? absl::StrCat("Token \"", offendingSymbol->getText(), "\": ", msg)
             : msg;
}

// _____________________________________________________________________________
void ThrowingErrorListener::syntaxError(antlr4::Recognizer* recognizer,
                                        antlr4::Token* offendingSymbol,
                                        size_t line, size_t charPositionInLine,
                                        const std::string& msg,
                                        [[maybe_unused]] std::exception_ptr e) {
  throw ParseException{
      generateExceptionMessage(offendingSymbol, msg),
      generateMetadata(recognizer, offendingSymbol, line, charPositionInLine)};
}
