// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>
#include <cstdio>
#include <unordered_set>
#include "../src/engine/Sort.h"

namespace std {
template <>
struct hash<std::array<Id, 3>> {
  size_t operator()(const std::array<Id, 3>& row) const {
    return row[0] + row[1] + row[2];
  }
};
}  // namespace std

TEST(SortTest, radix_sort) {
  IdTableStatic<3> init;
  unsigned int seed = time(NULL);
  for (size_t i = 0; i < 1000; i++) {
    init.push_back({static_cast<Id>(rand_r(&seed)),
                    static_cast<Id>(rand_r(&seed)),
                    static_cast<Id>(rand_r(&seed))});
  }

  IdTableStatic<3> stdSorted = init;
  IdTableStatic<3> radixSorted = init;
  std::sort(stdSorted.begin(), stdSorted.end(),
            [](const auto& a, const auto& b) { return a[1] < b[1]; });
  IdTable tmp = radixSorted.moveToDynamic();
  Sort::sort<3>(&tmp, 1, ResultTable::ResultType::KB);
  radixSorted = tmp.moveToStatic<3>();

  ASSERT_EQ(stdSorted.size(), radixSorted.size());
  size_t pos_radix = 0;
  // Use an unordered set to allow for comparisons of the two sorted arrays
  // without the sorting being stable.
  std::unordered_set<std::array<Id, 3>> current_block;
  for (size_t i = 0; i < stdSorted.size();) {
    size_t v = stdSorted[i][1];
    size_t num = 0;
    current_block.clear();
    while (i < stdSorted.size() && stdSorted[i][1] == v) {
      current_block.insert(stdSorted[i]);
      i++;
      num++;
    }
    for (size_t j = 0; j < num; j++) {
      ASSERT_EQ(v, radixSorted[pos_radix][1]);
      ASSERT_TRUE(current_block.find(radixSorted[pos_radix]) !=
                  current_block.end());
      pos_radix++;
    }
  }
}

TEST(SortTest, radix_sort_short) {
  IdTableStatic<3> init;
  unsigned int seed = time(NULL);
  for (size_t i = 0; i < 32; i++) {
    init.push_back({static_cast<Id>(rand_r(&seed)),
                    static_cast<Id>(rand_r(&seed)),
                    static_cast<Id>(rand_r(&seed))});
  }

  IdTableStatic<3> stdSorted = init;
  IdTableStatic<3> radixSorted = init;
  std::sort(stdSorted.begin(), stdSorted.end(),
            [](const auto& a, const auto& b) { return a[1] < b[1]; });
  IdTable tmp = radixSorted.moveToDynamic();
  Sort::sort<3>(&tmp, 1, ResultTable::ResultType::KB);
  radixSorted = tmp.moveToStatic<3>();

  ASSERT_EQ(stdSorted.size(), radixSorted.size());
  size_t pos_radix = 0;
  // Use an unordered set to allow for comparisons of the two sorted arrays
  // without the sorting being stable.
  std::unordered_set<std::array<Id, 3>> current_block;
  for (size_t i = 0; i < stdSorted.size();) {
    size_t v = stdSorted[i][1];
    size_t num = 0;
    current_block.clear();
    while (i < stdSorted.size() && stdSorted[i][1] == v) {
      current_block.insert(stdSorted[i]);
      i++;
      num++;
    }
    for (size_t j = 0; j < num; j++) {
      ASSERT_EQ(v, radixSorted[pos_radix][1]);
      ASSERT_TRUE(current_block.find(radixSorted[pos_radix]) !=
                  current_block.end());
      pos_radix++;
    }
  }
}
