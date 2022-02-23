//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <algorithm>

#include "../src/util/BackgroundStxxlSorter.h"
#include "../src/util/Random.h"

// The combination of 100MB for Stxxl and 50M ints (which require 200MB of
// Memory) unfortunately is the smallest configuration for stxxl that works and
// requires more than one block.
uint64_t memoryForTests = 1000 * 1000 * 100;

struct IntSorter : public std::less<> {
  int max_value() const { return std::numeric_limits<int>::max(); }
};

TEST(BackgroundStxxlSorter, sortInts) {
  ad_utility::BackgroundStxxlSorter<int, IntSorter> sorter{memoryForTests};
  std::vector<int> ints;
  const uint64_t numInts = 50'000'000;
  ints.reserve(numInts);
  SlowRandomIntGenerator<int> r{-200000, 200000};
  for (size_t i = 0; i < numInts; ++i) {
    ints.push_back(r());
  }

  for (auto i : ints) {
    sorter.push(i);
  }
  sorter.sort();
  std::vector<int> result;
  while (!sorter.empty()) {
    result.push_back(*sorter);
    ++sorter;
  }
  std::sort(ints.begin(), ints.end());
  ASSERT_EQ(ints, result);
}

TEST(StxxlUniqueSorter, uniqueInts) {
  ad_utility::BackgroundStxxlSorter<int, IntSorter> sorter{memoryForTests};
  std::vector<int> originalInts;
  const uint64_t numInts = 50'000;
  originalInts.reserve(numInts);
  SlowRandomIntGenerator<int> r{-200000, 200000};
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
  sorter.sort();
  ad_utility::StxxlUniqueSorter uniqueSorter{sorter};
  std::vector<int> result;
  while (!uniqueSorter.empty()) {
    result.push_back(*uniqueSorter);
    ++uniqueSorter;
  }
  std::sort(originalInts.begin(), originalInts.end());
  // Erase "accidentally" unique duplicates from the random initialization.
  auto it = std::unique(originalInts.begin(), originalInts.end());
  originalInts.erase(it, originalInts.end());
  ASSERT_EQ(originalInts.size(), result.size());
  ASSERT_EQ(originalInts, result);
}
