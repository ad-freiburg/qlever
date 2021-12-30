// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

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
