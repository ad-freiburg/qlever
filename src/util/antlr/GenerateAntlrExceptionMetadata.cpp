//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors:
//   2022 Julian Mundhahs (mundhahj@tf.informatik.uni-freiburg.de)
//   2023 Andre Schlegel (schlegea@informatik.uni-freiburg.de)

// These are ANTLR headers.
#include "util/antlr/GenerateAntlrExceptionMetadata.h"

#include <Lexer.h>
#include <Parser.h>

#include "util/Exception.h"

namespace ad_utility::antlr_utility {
// _____________________________________________________________________________
ExceptionMetadata generateAntlrExceptionMetadataWithParser(
    antlr4::Parser* parser, const antlr4::Token* offendingToken, size_t line,
    size_t charPositionInLine) {
  return {
      parser->getTokenStream()->getTokenSource()->getInputStream()->toString(),
      offendingToken->getStartIndex(), offendingToken->getStopIndex(), line,
      charPositionInLine};
}

// _____________________________________________________________________________
ExceptionMetadata generateAntlrExceptionMetadata(
    const antlr4::ParserRuleContext* ctx) {
  auto start = ctx->getStart();
  return {start->getInputStream()->toString(), ctx->getStart()->getStartIndex(),
          ctx->getStop()->getStopIndex(), start->getLine(),
          start->getCharPositionInLine()};
}

// _____________________________________________________________________________
ExceptionMetadata generateAntlrExceptionMetadata(antlr4::Lexer* lexer,
                                                 size_t line,
                                                 size_t charPositionInLine) {
  return {lexer->getInputStream()->toString(), lexer->tokenStartCharIndex,
          lexer->tokenStartCharIndex, line, charPositionInLine};
}

// _____________________________________________________________________________
ExceptionMetadata generateAntlrExceptionMetadata(
    antlr4::Recognizer* recognizer, const antlr4::Token* offendingToken,
    size_t line, size_t charPositionInLine) {
  // The abstract `antlr4::Recognizer` interface does not provide enough
  // information to generate the Metadata. For this reason we manually cast to
  // the subclasses. As of ANTLRv4 `antlr4::Parser` and `antlr4::Lexer` are the
  // only subclasses of `antlr4::Recognizer`.
  if (auto* parser = dynamic_cast<antlr4::Parser*>(recognizer)) {
    AD_CONTRACT_CHECK(offendingToken != nullptr);
    return generateAntlrExceptionMetadataWithParser(parser, offendingToken,
                                                    line, charPositionInLine);
  } else if (auto* lexer = dynamic_cast<antlr4::Lexer*>(recognizer)) {
    // If the recognizer is a Lexer this means that the error was a Lexer error.
    // In that case `offendingToken` is `nullptr`.
    return generateAntlrExceptionMetadata(lexer, line, charPositionInLine);
  } else {
    AD_FAIL();  // Should be unreachable.
  }
}
}  // namespace ad_utility::antlr_utility
