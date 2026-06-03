// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "util/ConstructDeduplicationMode.h"

using namespace ad_utility;

namespace {
// Convenience aliases for the alternatives of `DeduplicationMode::value_`.
using None = DeduplicationMode::None;
using Global = DeduplicationMode::Global;
using BatchWise = DeduplicationMode::BatchWise;
}  // namespace

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FactoryFunctions) {
  EXPECT_TRUE(std::holds_alternative<None>(DeduplicationMode::none().value_));
  EXPECT_TRUE(
      std::holds_alternative<Global>(DeduplicationMode::global().value_));

  auto batchWise = DeduplicationMode::batchWise(42);
  ASSERT_TRUE(std::holds_alternative<BatchWise>(batchWise.value_));
  EXPECT_EQ(std::get<BatchWise>(batchWise.value_).batchSize_, 42u);
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringNone) {
  auto mode = DeduplicationModeFromString{}("false");
  EXPECT_TRUE(std::holds_alternative<None>(mode.value_));
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringGlobal) {
  auto mode = DeduplicationModeFromString{}("global");
  EXPECT_TRUE(std::holds_alternative<Global>(mode.value_));
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringBatchWise) {
  auto mode = DeduplicationModeFromString{}("100");
  ASSERT_TRUE(std::holds_alternative<BatchWise>(mode.value_));
  EXPECT_EQ(std::get<BatchWise>(mode.value_).batchSize_, 100u);
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringRejectsZeroBatchSize) {
  EXPECT_THROW(DeduplicationModeFromString{}("0"), std::runtime_error);
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringRejectsInvalidInput) {
  EXPECT_THROW(DeduplicationModeFromString{}(""), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("true"), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("-5"), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("global "), std::runtime_error);
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, ToString) {
  EXPECT_EQ(DeduplicationModeToString{}(DeduplicationMode::none()), "false");
  EXPECT_EQ(DeduplicationModeToString{}(DeduplicationMode::global()), "global");
  EXPECT_EQ(DeduplicationModeToString{}(DeduplicationMode::batchWise(7)), "7");
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, RoundTrip) {
  for (const std::string s : {"false", "global", "1", "1000"}) {
    auto roundTripped =
        DeduplicationModeToString{}(DeduplicationModeFromString{}(s));
    EXPECT_EQ(roundTripped, s);
  }
}
