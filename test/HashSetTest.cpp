// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Niklas Schnelle (schnelle@informatik.uni-freiburg.de)

#include <gtest/gtest.h>
#include <utility>
#include <vector>

#include "../src/util/HashSet.h"

// Note: Since the HashSet class is a wrapper for a well tested hash set
// implementation the following tests only check the API for functionality and
// sanity

TEST(HashSetTest, sizeAndInsert) {
  ad_utility::HashSet<int> set;
  ASSERT_EQ(set.size(), 0u);
  set.insert(42);
  set.insert(41);
  set.insert(12);
  ASSERT_EQ(set.size(), 3u);
};

TEST(HashSetTest, insertRange) {
  ad_utility::HashSet<int> set;
  std::vector<int> values = {2, 3, 5, 7, 11};
  set.insert(values.begin() + 1, values.end());
  ASSERT_EQ(set.count(2), 0u);
  ASSERT_EQ(set.count(4), 0u);
  ASSERT_EQ(set.count(8), 0u);

  ASSERT_EQ(set.count(3), 1u);
  ASSERT_EQ(set.count(5), 1u);
  ASSERT_EQ(set.count(7), 1u);
  ASSERT_EQ(set.count(11), 1u);
};

TEST(HashSetTest, iterator) {
  ad_utility::HashSet<int> set;
  set.insert(41);
  set.insert(12);
  ASSERT_TRUE(++(++set.begin()) == set.end());
  ad_utility::HashSet<int> settwo;
  settwo.insert(set.begin(), set.end());
  ASSERT_EQ(settwo.size(), 2u);
  ASSERT_EQ(settwo.count(41), 1u);
  ASSERT_EQ(settwo.count(12), 1u);
};

TEST(HashSetTest, erase) {
  ad_utility::HashSet<int> set;
  set.insert(41);
  set.insert(12);
  ASSERT_EQ(set.size(), 2u);
  set.erase(41);
  ASSERT_EQ(set.size(), 1u);
};

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
