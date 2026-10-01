// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

#include "util/RangeThatReleasesOnEnd.h"

using ad_utility::InputRangeFromGet;
using ad_utility::InputRangeTypeErased;
using ad_utility::RangeThatReleasesOnEnd;

namespace {
// An input range that yields the given `elements` and then (optionally) throws
// instead of signalling its end. It reports its destruction via `destroyed_`
// and counts how often `get()` was called via `numGetCalls_`.
class ReportingRange : public InputRangeFromGet<int> {
 private:
  std::vector<int> elements_;
  size_t nextIdx_ = 0;
  bool throwAtEnd_;
  bool* destroyed_;
  size_t* numGetCalls_;

 public:
  ReportingRange(std::vector<int> elements, bool throwAtEnd, bool* destroyed,
                 size_t* numGetCalls)
      : elements_{std::move(elements)},
        throwAtEnd_{throwAtEnd},
        destroyed_{destroyed},
        numGetCalls_{numGetCalls} {}
  ReportingRange(const ReportingRange&) = delete;
  ReportingRange& operator=(const ReportingRange&) = delete;
  ~ReportingRange() override { *destroyed_ = true; }

  std::optional<int> get() override {
    ++*numGetCalls_;
    if (nextIdx_ < elements_.size()) {
      return elements_[nextIdx_++];
    }
    if (throwAtEnd_) {
      throw std::runtime_error{"inner range failed"};
    }
    return std::nullopt;
  }
};

// Create a `RangeThatReleasesOnEnd` around a `ReportingRange` with the given
// arguments.
auto makeRange(std::vector<int> elements, bool throwAtEnd, bool* destroyed,
               size_t* numGetCalls) {
  return RangeThatReleasesOnEnd<int>{
      InputRangeTypeErased<int>{std::make_unique<ReportingRange>(
          std::move(elements), throwAtEnd, destroyed, numGetCalls)}};
}
}  // namespace

// _____________________________________________________________________________
TEST(RangeThatReleasesOnEnd, yieldsAllElementsAndReleasesOnEnd) {
  bool destroyed = false;
  size_t numGetCalls = 0;
  auto range = makeRange({3, 1, 4}, false, &destroyed, &numGetCalls);
  EXPECT_EQ(range.get(), 3);
  EXPECT_EQ(range.get(), 1);
  EXPECT_EQ(range.get(), 4);
  // The inner range is still alive, because its end has not been reached yet.
  EXPECT_FALSE(destroyed);
  EXPECT_EQ(range.get(), std::nullopt);
  // The inner range is released as soon as its end is reached, although the
  // outer range is still alive.
  EXPECT_TRUE(destroyed);
  EXPECT_EQ(numGetCalls, 4u);
  // Further calls simply yield nothing and don't touch the inner range.
  EXPECT_EQ(range.get(), std::nullopt);
  EXPECT_EQ(range.get(), std::nullopt);
  EXPECT_EQ(numGetCalls, 4u);
}

// _____________________________________________________________________________
TEST(RangeThatReleasesOnEnd, emptyInnerRange) {
  bool destroyed = false;
  size_t numGetCalls = 0;
  auto range = makeRange({}, false, &destroyed, &numGetCalls);
  EXPECT_FALSE(destroyed);
  EXPECT_EQ(range.get(), std::nullopt);
  EXPECT_TRUE(destroyed);
  EXPECT_EQ(range.get(), std::nullopt);
  EXPECT_EQ(numGetCalls, 1u);
}

// _____________________________________________________________________________
TEST(RangeThatReleasesOnEnd, releasesOnException) {
  bool destroyed = false;
  size_t numGetCalls = 0;
  auto range = makeRange({42}, true, &destroyed, &numGetCalls);
  EXPECT_EQ(range.get(), 42);
  EXPECT_THROW(range.get(), std::runtime_error);
  // The exception is propagated, and the inner range is released nevertheless.
  EXPECT_TRUE(destroyed);
  EXPECT_EQ(range.get(), std::nullopt);
  EXPECT_EQ(numGetCalls, 2u);
}

// _____________________________________________________________________________
TEST(RangeThatReleasesOnEnd, worksAsARangeInALoop) {
  bool destroyed = false;
  size_t numGetCalls = 0;
  auto range = makeRange({1, 2, 3, 4}, false, &destroyed, &numGetCalls);
  std::vector<int> result;
  for (int i : range) {
    result.push_back(i);
  }
  EXPECT_THAT(result, ::testing::ElementsAre(1, 2, 3, 4));
  EXPECT_TRUE(destroyed);
}
