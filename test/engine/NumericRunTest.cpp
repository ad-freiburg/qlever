// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <cmath>
#include <limits>

#include "../util/IdTestHelpers.h"
#include "engine/NumericRun.h"

using namespace numericRun;
using ad_utility::testing::DoubleId;
using ad_utility::testing::IntId;
using ad_utility::testing::UndefId;
using enum NumericRun;

namespace {
constexpr double infValue = std::numeric_limits<double>::infinity();
constexpr double nanValue = std::numeric_limits<double>::quiet_NaN();
constexpr int64_t minInt = ValueId::IntegerType::min();
constexpr int64_t maxInt = ValueId::IntegerType::max();
}  // namespace

// Test that `getRunOfId` returns the run of each kind of numeric `Id`, and
// `std::nullopt` for other datatypes.
TEST(NumericRun, getRunOfId) {
  // The boundary values of each run.
  EXPECT_EQ(getRunOfId(IntId(0)), IntNonNegative);
  EXPECT_EQ(getRunOfId(IntId(maxInt)), IntNonNegative);
  EXPECT_EQ(getRunOfId(IntId(-1)), IntNegative);
  EXPECT_EQ(getRunOfId(IntId(minInt)), IntNegative);
  EXPECT_EQ(getRunOfId(DoubleId(0.0)), DoubleNonNegative);
  EXPECT_EQ(getRunOfId(DoubleId(infValue)), DoubleNonNegative);
  EXPECT_EQ(getRunOfId(DoubleId(nanValue)), DoublePositiveNan);
  EXPECT_EQ(getRunOfId(DoubleId(-0.0)), DoubleNegative);
  EXPECT_EQ(getRunOfId(DoubleId(-infValue)), DoubleNegative);
  EXPECT_EQ(getRunOfId(DoubleId(-nanValue)), DoubleNegativeNan);

  // Non-numeric `Id`s.
  EXPECT_EQ(getRunOfId(UndefId()), std::nullopt);
  EXPECT_EQ(getRunOfId(Id::makeFromBool(true)), std::nullopt);
}

// Test that `getCommonNumericDatatype` returns the datatype only if both `Id`s
// are `Int`s or both are `Double`s.
TEST(NumericRun, getCommonNumericDatatype) {
  // The same numeric datatype, in different runs.
  EXPECT_EQ(getCommonNumericDatatype(IntId(3), IntId(-3)), Datatype::Int);
  EXPECT_EQ(getCommonNumericDatatype(DoubleId(1.0), DoubleId(-nanValue)),
            Datatype::Double);

  // Different datatypes, or a non-numeric one.
  EXPECT_EQ(getCommonNumericDatatype(IntId(3), DoubleId(1.0)), std::nullopt);
  EXPECT_EQ(getCommonNumericDatatype(UndefId(), IntId(1)), std::nullopt);
  EXPECT_EQ(getCommonNumericDatatype(IntId(1), UndefId()), std::nullopt);
}

// Test that the enumerators of `NumericRun` and `getRunsInInternalOrder` are in
// the internal order of the `Id`s.
TEST(NumericRun, runsInInternalOrderMatchTheBits) {
  // One sample per run, in the order of the enumerators.
  std::vector<Id> samples{IntId(5),       IntId(-5),
                          DoubleId(5.0),  DoubleId(nanValue),
                          DoubleId(-5.0), DoubleId(-nanValue)};
  ASSERT_EQ(samples.size(), numNumericRuns);
  for (size_t i = 0; i < samples.size(); ++i) {
    EXPECT_EQ(toIndex(getRunOfId(samples[i]).value()), i);
  }

  // The samples, and thus the enumerators, are sorted by bits, and so is
  // `getRunsInInternalOrder`.
  EXPECT_TRUE(ql::ranges::is_sorted(samples, {}, &Id::getBits));
  EXPECT_THAT(getRunsInInternalOrder(Datatype::Int),
              ::testing::ElementsAre(IntNonNegative, IntNegative));
  EXPECT_THAT(getRunsInInternalOrder(Datatype::Double),
              ::testing::ElementsAre(DoubleNonNegative, DoublePositiveNan,
                                     DoubleNegative, DoubleNegativeNan));

  // The datatype of a run.
  EXPECT_EQ(getDatatype(IntNegative), Datatype::Int);
  EXPECT_EQ(getDatatype(DoubleNegativeNan), Datatype::Double);
}

// Test that `getRunsInSemanticOrder` returns the runs in the order of `ORDER
// BY ASC` and `ORDER BY DESC`, each with its direction.
TEST(NumericRun, getRunsInSemanticOrder) {
  // Compare as pairs, because `RunWithDirection` has no `operator==`.
  auto toPairs = [](const std::vector<RunWithDirection>& runs) {
    return ::ranges::to<std::vector<std::pair<NumericRun, bool>>>(
        runs | ql::views::transform([](const RunWithDirection& r) {
          auto [run, reversed] = r;
          return std::pair{run, reversed};
        }));
  };
  using P = std::pair<NumericRun, bool>;

  // Ints, ascending and descending.
  EXPECT_THAT(
      toPairs(getRunsInSemanticOrder(Datatype::Int, false)),
      ::testing::ElementsAre(P{IntNegative, false}, P{IntNonNegative, false}));
  EXPECT_THAT(
      toPairs(getRunsInSemanticOrder(Datatype::Int, true)),
      ::testing::ElementsAre(P{IntNonNegative, true}, P{IntNegative, true}));

  // Doubles, ascending and descending (the negative doubles are the only run
  // that is traversed in reverse for `ASC`).
  EXPECT_THAT(toPairs(getRunsInSemanticOrder(Datatype::Double, false)),
              ::testing::ElementsAre(
                  P{DoubleNegative, true}, P{DoubleNonNegative, false},
                  P{DoublePositiveNan, false}, P{DoubleNegativeNan, false}));
  EXPECT_THAT(toPairs(getRunsInSemanticOrder(Datatype::Double, true)),
              ::testing::ElementsAre(
                  P{DoubleNegativeNan, true}, P{DoublePositiveNan, true},
                  P{DoubleNonNegative, true}, P{DoubleNegative, false}));
}
