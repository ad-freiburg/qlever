// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "engine/sparqlExpressions/IntegerDateOperations.h"
#include "global/Id.h"

using namespace ql::engine::scalar;

// _____________________________________________________________________________
TEST(IntegerDateOperationsTest, ScalarDateExtraction) {
  auto dateId =
      Id::makeFromDate(DateYearOrDuration{Date{2026, 9, 3, 15, 30, 0.0}});

  EXPECT_EQ(IntegerDateOperations::extractYear(dateId), 2026);
  EXPECT_EQ(IntegerDateOperations::extractMonth(dateId), 9);
  EXPECT_EQ(IntegerDateOperations::extractDay(dateId), 3);
}

// _____________________________________________________________________________
TEST(IntegerDateOperationsTest, MakePackedDateRoundTrip) {
  auto dateId = IntegerDateOperations::makePackedDate(2026, 9, 3, 15, 30, 0);

  EXPECT_EQ(IntegerDateOperations::extractYear(dateId), 2026);
  EXPECT_EQ(IntegerDateOperations::extractMonth(dateId), 9);
  EXPECT_EQ(IntegerDateOperations::extractDay(dateId), 3);
}

// _____________________________________________________________________________
TEST(IntegerDateOperationsTest, NonDateReturnsNullopt) {
  auto intId = Id::makeFromInt(42);
  EXPECT_EQ(IntegerDateOperations::extractYear(intId), std::nullopt);
  EXPECT_EQ(IntegerDateOperations::extractMonth(intId), std::nullopt);
  EXPECT_EQ(IntegerDateOperations::extractDay(intId), std::nullopt);
  // Year 0 is a valid year and distinct from a non-date.
  EXPECT_EQ(IntegerDateOperations::extractYear(
                IntegerDateOperations::makePackedDate(0, 1, 1)),
            0);
  // An `xsd:gYear` has no month and no day.
  auto yearOnly = Id::makeFromDate(DateYearOrDuration::parseGYear("2026"));
  EXPECT_EQ(IntegerDateOperations::extractYear(yearOnly), 2026);
  EXPECT_EQ(IntegerDateOperations::extractMonth(yearOnly), std::nullopt);
  EXPECT_EQ(IntegerDateOperations::extractDay(yearOnly), std::nullopt);
}

// _____________________________________________________________________________
TEST(IntegerDateOperationsTest, BatchYearExtraction) {
  std::vector<Id> dates = {
      Id::makeFromDate(DateYearOrDuration{Date{1999, 12, 31, 0, 0, 0.0}}),
      Id::makeFromDate(DateYearOrDuration{Date{2000, 1, 1, 0, 0, 0.0}}),
      Id::makeFromInt(2026),
      Id::makeFromDate(DateYearOrDuration{Date{2026, 9, 3, 0, 0, 0.0}}),
  };

  std::vector<std::optional<int64_t>> years(dates.size());
  IntegerDateOperations::extractYearsBatch(dates, years);

  EXPECT_THAT(years, ::testing::ElementsAre(1999, 2000, std::nullopt, 2026));
}

// _____________________________________________________________________________
TEST(IntegerDateOperationsTest, InvalidInputsThrow) {
  // Month 13 and year 70000 are out of range for `Date`.
  EXPECT_ANY_THROW(IntegerDateOperations::makePackedDate(2026, 13, 1));
  EXPECT_ANY_THROW(IntegerDateOperations::makePackedDate(70000, 1, 1));
  // The output span must be at least as large as the input span.
  std::vector<Id> dates(2, IntegerDateOperations::makePackedDate(2026, 1, 1));
  std::vector<std::optional<int64_t>> years(1);
  EXPECT_ANY_THROW(IntegerDateOperations::extractYearsBatch(dates, years));
}
