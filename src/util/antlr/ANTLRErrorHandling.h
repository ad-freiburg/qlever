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
#include "RecognitionException.h"
#include "Recognizer.h"
#include "TokenSource.h"

struct ThrowingErrorStrategy : public antlr4::DefaultErrorStrategy {
  void reportError(antlr4::Parser*,
                   const antlr4::RecognitionException& e) override {
    throw antlr4::ParseCancellationException{
        e.what() + std::string{" at token \""} +
        e.getOffendingToken()->getText() + '"'};
  }
};

namespace {
// for string delimiter
std::vector<string> split(const string& s, const string& delimiter) {
  size_t pos_start = 0, pos_end, delim_len = delimiter.length();
  string token;
  std::vector<string> res;

  while ((pos_end = s.find(delimiter, pos_start)) != string::npos) {
    token = s.substr(pos_start, pos_end - pos_start);
    pos_start = pos_end + delim_len;
    res.push_back(token);
  }

  res.push_back(s.substr(pos_start));
  return res;
}

void underlineError(antlr4::Recognizer* recognizer,
                    antlr4::Token* offendingToken, size_t line,
                    size_t charPositionInLine) {
  auto tokens = (antlr4::CommonTokenStream*)recognizer->getInputStream();
  auto input = tokens->getTokenSource()->getInputStream()->toString();
  auto lines = split(input, "\n");
  auto errorLine = lines[line - 1];
  std::cout << errorLine << std::endl;
  for (size_t i = 0; i < charPositionInLine; i++) {
    std::cout << " ";
  }
  auto start = offendingToken->getStartIndex();
  auto stop = offendingToken->getStopIndex();
  if (start != INVALID_INDEX && stop != INVALID_INDEX) {
    for (size_t i = start; i <= stop; i++) {
      std::cout << "^";
    }
  }
  std::cout << std::endl;
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
    // Try out displaying the error with underlining.
    underlineError(recognizer, offendingSymbol, line, charPositionInLine);
    // Could do the highlighting with ANSI Escape Sequences.
    throw ParseException{
        absl::StrCat("line ", line, ":", charPositionInLine, " ", msg)};
  }
};
