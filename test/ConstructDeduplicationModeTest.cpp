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
using Full = DeduplicationMode::Full;
using Lru = DeduplicationMode::Lru;
}  // namespace

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FactoryFunctions) {
  EXPECT_TRUE(std::holds_alternative<None>(DeduplicationMode::none().value_));
  EXPECT_TRUE(std::holds_alternative<Full>(DeduplicationMode::full().value_));

  auto lru = DeduplicationMode::lru(42);
  ASSERT_TRUE(std::holds_alternative<Lru>(lru.value_));
  EXPECT_EQ(std::get<Lru>(lru.value_).capacity_, 42u);
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringNone) {
  auto mode = DeduplicationModeFromString{}("none");
  EXPECT_TRUE(std::holds_alternative<None>(mode.value_));
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringFull) {
  auto mode = DeduplicationModeFromString{}("full");
  EXPECT_TRUE(std::holds_alternative<Full>(mode.value_));
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringLru) {
  auto mode = DeduplicationModeFromString{}("lru:100");
  ASSERT_TRUE(std::holds_alternative<Lru>(mode.value_));
  EXPECT_EQ(std::get<Lru>(mode.value_).capacity_, 100u);
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringRejectsZeroBatchSize) {
  EXPECT_THROW(DeduplicationModeFromString{}("lru:0"), std::runtime_error);
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, FromStringRejectsInvalidInput) {
  EXPECT_THROW(DeduplicationModeFromString{}(""), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("false"), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("true"), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("100"), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("lru:"), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("lru:-5"), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("lru:abc"), std::runtime_error);
  EXPECT_THROW(DeduplicationModeFromString{}("full "), std::runtime_error);
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, ToString) {
  EXPECT_EQ(DeduplicationModeToString{}(DeduplicationMode::none()), "none");
  EXPECT_EQ(DeduplicationModeToString{}(DeduplicationMode::full()), "full");
  EXPECT_EQ(DeduplicationModeToString{}(DeduplicationMode::lru(7)), "lru:7");
}

// _____________________________________________________________________________
TEST(ConstructDeduplicationMode, RoundTrip) {
  for (const std::string s : {"none", "full", "lru:1", "lru:1000"}) {
    auto roundTripped =
        DeduplicationModeToString{}(DeduplicationModeFromString{}(s));
    EXPECT_EQ(roundTripped, s);
  }
}
