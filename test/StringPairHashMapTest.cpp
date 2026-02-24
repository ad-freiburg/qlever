// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gtest/gtest.h>

#include "util/StringPairHashMap.h"

// _____________________________________________________________________________
TEST(StringPairHashMapTest, InsertAndLookup) {
  ad_utility::StringPairHashMap<int> map;

  using ad_utility::detail::StringPair;
  using ad_utility::detail::StringViewPair;

  // Insert using `std::string` pairs.
  map[StringPair{"hello", "world"}] = 7;
  map[StringPair{"foo", "bar"}] = 42;
  StringPair x{StringViewPair{"a", "b"}};

  ASSERT_EQ(map.size(), 2u);

  // Lookup using `std::string_view` pairs.
  auto it = map.find(StringViewPair{"hello", "world"});
  ASSERT_NE(it, map.end());
  ASSERT_EQ(it->second, 7);

  EXPECT_EQ(map.count(StringViewPair{"foo", "bar"}), 1u);
  EXPECT_EQ(map.count(StringViewPair{"does not", "exist"}), 0u);
}

// _____________________________________________________________________________
TEST(StringPairHashMapTest, StringPairEq) {
  using ad_utility::detail::StringPair;
  using ad_utility::detail::StringViewPair;
  ad_utility::detail::StringPairEq eq;

  StringPair a{"a", "b"};
  StringPair b{"x", "y"};
  StringPair c{"x", "g"};

  EXPECT_TRUE(eq(a, a));
  EXPECT_FALSE(eq(a, b));
  EXPECT_FALSE(eq(a, c));

  StringViewPair aEq{"a", "b"};
  StringViewPair aNe{"a", "c"};
  StringViewPair bNe{"f", "g"};

  EXPECT_TRUE(eq(a, aEq));
  EXPECT_FALSE(eq(a, aNe));
  EXPECT_FALSE(eq(b, bNe));

  EXPECT_TRUE(eq(aEq, a));
  EXPECT_FALSE(eq(aNe, a));
  EXPECT_FALSE(eq(bNe, b));

  StringViewPair aSv{"a", "b"};

  EXPECT_TRUE(eq(a, aSv));
}
