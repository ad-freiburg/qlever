// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Niklas Schnelle (schnelle@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "../src/util/HashMap.h"

// Note: Since the HashMap class is a wrapper for a well tested hash map
// implementation the following tests only check the API for functionality and
// sanity

TEST(HashMapTest, operatorBrackets) {
  ad_utility::HashMap<std::string, int> map;
  map["foo"] = 42;
  ASSERT_EQ(map["foo"], 42);
};

TEST(HashMapTest, size) {
  ad_utility::HashMap<std::string, int> map;
  map["foo"] = 42;
  map["bar"] = 41;
  ASSERT_EQ(map.size(), 2u);
};

TEST(HashMapTest, insertSingle) {
  ad_utility::HashMap<std::string, int> map;
  map.insert({"foo", 42});
  map.insert({"bar", 3});
  ASSERT_EQ(map["foo"], 42);
  ASSERT_EQ(map["bar"], 3);
};

TEST(HashMapTest, insertRange) {
  ad_utility::HashMap<std::string, int> map;
  std::vector<std::pair<std::string, int>> values = {
      {"one", 1}, {"two", 2}, {"three", 3}};
  map.insert(values.begin() + 1, values.end());
  ASSERT_EQ(map["two"], 2);
  ASSERT_EQ(map["three"], 3);
  ASSERT_EQ(map.count("one"), 0u);
};

TEST(HashMapTest, iterator) {
  ad_utility::HashMap<std::string, int> map;
  map["foo"] = 42;
  map["bar"] = 41;
  ASSERT_TRUE(++(++map.begin()) == map.end());
  ad_utility::HashMap<std::string, int> maptwo;
  maptwo.insert(map.begin(), map.end());
  ASSERT_EQ(maptwo.size(), 2u);
  ASSERT_EQ(maptwo["foo"], 42);
  ASSERT_EQ(maptwo["bar"], 41);
};
