//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <algorithm>

#define STXXL_DEFAULT_BLOCK_SIZE(type) 512
#include "../src/util/BackgroundStxxlSorter.h"
#include "../src/util/Random.h"

// The combination of 100MB for STXXL and 50M ints (which require 200MB of
// memory) unfortunately is the smallest configuration for STXXL that works and
// requires more than one block.
uint64_t memoryForTests = 1000 * 10 * 1;

struct IntSorter : public std::less<> {
  int max_value() const { return std::numeric_limits<int>::max(); }
  int min_value() const { return std::numeric_limits<int>::min(); }
};

TEST(BackgroundStxxlSorter, SortInts) {
  ad_utility::BackgroundStxxlSorter<int, IntSorter> sorter{memoryForTests};
  std::vector<int> ints;
  const uint64_t numInts = 50'000;
  ints.reserve(numInts);
  for (size_t i = 0; i < numInts; ++i) {
    ints.push_back(numInts - i);
  }

  for (auto i : ints) {
    sorter.push(i);
  }
  std::vector<int> result;
  for (const auto& element : sorter.sortedView()) {
    result.push_back(element);
  }
  std::sort(ints.begin(), ints.end());
  ASSERT_EQ(ints, result);
}

TEST(StxxlUniqueSorter, uniqueInts) {
  ad_utility::BackgroundStxxlSorter<int, IntSorter> sorter{memoryForTests};
  std::vector<int> originalInts;
  const uint64_t numInts = 50'000;
  originalInts.reserve(numInts);
  ad_utility::SlowRandomIntGenerator<int> r{-200'000, 200'000};
  for (size_t i = 0; i < numInts; ++i) {
    originalInts.push_back(r());
  }

  std::vector<int> duplicateInts;
  duplicateInts.reserve(3 * numInts);
  for (size_t i = 0; i < 3; ++i) {
    for (auto num : originalInts) {
      duplicateInts.push_back(num);
    }
  }

  for (auto i : duplicateInts) {
    sorter.push(i);
  }
  auto uniqueSorter = ad_utility::uniqueView(sorter.sortedView());
  std::vector<int> result;
  for (const auto& element : uniqueSorter) {
    result.push_back(element);
  }
  std::sort(originalInts.begin(), originalInts.end());
  // Erase "accidentally" unique duplicates from the random initialization.
  auto it = std::unique(originalInts.begin(), originalInts.end());
  originalInts.erase(it, originalInts.end());
  ASSERT_EQ(originalInts.size(), result.size());
  ASSERT_EQ(originalInts, result);
}
