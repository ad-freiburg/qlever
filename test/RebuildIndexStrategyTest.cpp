//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>

#include "engine/RebuildIndexStrategy.h"
#include "util/GTestHelpers.h"

using qlever::RebuildIndexStrategy;
using ::testing::HasSubstr;
using ::testing::Optional;

// _____________________________________________________________________________
TEST(RebuildIndexStrategy, parseManual) {
  // "manual" means "no automatic rebuild".
  EXPECT_EQ(RebuildIndexStrategy::parse("manual"), std::nullopt);
}

// _____________________________________________________________________________
TEST(RebuildIndexStrategy, parseMinMaxFraction) {
  EXPECT_THAT(RebuildIndexStrategy::parse("10000:1000000:0.1"),
              Optional(RebuildIndexStrategy{10'000, 1'000'000, 0.1}));
  // `min` may be zero, and the fraction may exceed 1 (the delta can be larger
  // than the index, e.g. after many deletes).
  EXPECT_THAT(RebuildIndexStrategy::parse("0:100:2.5"),
              Optional(RebuildIndexStrategy{0, 100, 2.5}));
  // `min == max` makes the threshold a fixed number of delta triples.
  EXPECT_THAT(RebuildIndexStrategy::parse("500:500:1"),
              Optional(RebuildIndexStrategy{500, 500, 1.0}));
}

// _____________________________________________________________________________
TEST(RebuildIndexStrategy, parseErrors) {
  auto expectThrows = [](std::string_view strategy, std::string_view message) {
    AD_EXPECT_THROW_WITH_MESSAGE(RebuildIndexStrategy::parse(strategy),
                                 HasSubstr(message));
  };
  // Anything that is not "manual" and not three colon-separated parts.
  expectThrows("", "neither \"manual\" nor");
  expectThrows("automatic", "neither \"manual\" nor");
  expectThrows("500000", "neither \"manual\" nor");
  expectThrows("10:20", "neither \"manual\" nor");
  expectThrows("10:20:30:40", "neither \"manual\" nor");
  // `min` and `max` must be non-negative numbers.
  expectThrows("x:20:0.1", "for `min`");
  expectThrows("10:y:0.1", "for `max`");
  // The fraction must be a number greater than zero.
  expectThrows("10:20:0", "greater than 0");
  expectThrows("10:20:-1", "greater than 0");
  expectThrows("10:20:abc", "greater than 0");
  // `min` must not be larger than `max`.
  expectThrows("100:10:0.1", "must not be larger than");
}

// _____________________________________________________________________________
TEST(RebuildIndexStrategy, rebuildThresholdMinMaxFraction) {
  // Threshold is 10% of the index size, clamped to [10000, 1000000].
  RebuildIndexStrategy strategy{10'000, 1'000'000, 0.1};
  EXPECT_EQ(strategy.rebuildThreshold(0), 10'000);           // clamped to min
  EXPECT_EQ(strategy.rebuildThreshold(50'000), 10'000);      // 5000 -> min
  EXPECT_EQ(strategy.rebuildThreshold(1'000'000), 100'000);  // 10%
  EXPECT_EQ(strategy.rebuildThreshold(50'000'000), 1'000'000);  // clamped max

  // A fractional raw threshold is rounded up to a whole number of triples.
  RebuildIndexStrategy roundingStrategy{1, 1'000'000, 0.1};
  EXPECT_EQ(roundingStrategy.rebuildThreshold(105), 11);  // ceil(10.5)
  EXPECT_FALSE(roundingStrategy.shouldTriggerRebuild(10, 105));
  EXPECT_TRUE(roundingStrategy.shouldTriggerRebuild(11, 105));

  // A raw threshold beyond the range of `size_t` is clamped to `max` (and in
  // particular does not overflow in the conversion).
  RebuildIndexStrategy hugeFractionStrategy{1, 1'000'000, 1e30};
  EXPECT_EQ(hugeFractionStrategy.rebuildThreshold(1'000'000), 1'000'000);
}

// _____________________________________________________________________________
TEST(RebuildIndexStrategy, shouldTriggerRebuildMinMaxFraction) {
  // Rebuild when the delta reaches 10% of the index, but never below 10000
  // delta triples, and always at 1000000.
  RebuildIndexStrategy strategy{10'000, 1'000'000, 0.1};

  // Below `min`, never rebuild, even at a high fraction.
  EXPECT_FALSE(strategy.shouldTriggerRebuild(9'999, 0));
  EXPECT_FALSE(strategy.shouldTriggerRebuild(9'999, 1));
  // At `min`, rebuild (the threshold is `min` here).
  EXPECT_TRUE(strategy.shouldTriggerRebuild(10'000, 0));

  // Between `min` and `max`, rebuild exactly when the fraction is reached.
  EXPECT_FALSE(strategy.shouldTriggerRebuild(50'000, 1'000'000));  // 5%
  EXPECT_TRUE(strategy.shouldTriggerRebuild(100'000, 1'000'000));  // 10%
  EXPECT_TRUE(strategy.shouldTriggerRebuild(200'000, 1'000'000));  // 20%
  // At least `min`, but the fraction is not yet reached.
  EXPECT_FALSE(strategy.shouldTriggerRebuild(10'000, 100'000'000));

  // At `max`, always rebuild, even far below the fraction.
  EXPECT_TRUE(strategy.shouldTriggerRebuild(1'000'000, 100'000'000'000ULL));
}
