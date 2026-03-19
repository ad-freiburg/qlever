// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "index/LocatedTriples.h"
#include "util/IdTestHelpers.h"

namespace {
auto V = ad_utility::testing::VocabId;

auto IT = [](const auto& c1, const auto& c2, const auto& c3) {
  return IdTriple{std::array<Id, 4>{V(c1), V(c2), V(c3), V(0)}};
};

auto LT = [](size_t blockIndex, const IdTriple<>& triple, bool insertOrDelete) {
  return LocatedTriple{blockIndex, triple, insertOrDelete};
};

// TODO: actually finish
TEST(SortedVectorTest, test) {
  auto lt1 = LT(0, IT(1, 2, 3), true);
  auto lt1I = LT(0, IT(1, 2, 3), false);
  auto lt2 = LT(0, IT(2, 2, 3), true);

  SortedLocatedTriplesVector sv;
  EXPECT_TRUE(sv.empty());
  EXPECT_EQ(sv.size(), 0);

  sv.insert(lt1);

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 1);
  EXPECT_EQ(*sv.begin(), lt1);

  sv.insert(lt1);

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 1);

  sv.insert(lt1I);
  sv.insert(lt1);

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 1);
}

// Basic insert and iteration test.
TEST(SortedVectorTest, insertAndIterate) {
  SortedLocatedTriplesVector sv;
  EXPECT_TRUE(sv.empty());

  sv.insert(LT(0, IT(3, 2, 1), true));
  sv.insert(LT(0, IT(1, 2, 3), true));
  sv.insert(LT(0, IT(2, 2, 2), true));

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 3);

  // After iteration, elements should be sorted by triple.
  std::vector<LocatedTriple> result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].triple_, IT(1, 2, 3));
  EXPECT_EQ(result[1].triple_, IT(2, 2, 2));
  EXPECT_EQ(result[2].triple_, IT(3, 2, 1));
}

// Test that duplicates are removed (last one wins).
TEST(SortedVectorTest, duplicatesRemoved) {
  SortedLocatedTriplesVector sv;

  // Insert same triple twice with different insertOrDelete values.
  sv.insert(LT(0, IT(1, 2, 3), true));
  sv.insert(LT(0, IT(1, 2, 3), false));

  EXPECT_EQ(sv.size(), 1);

  // The last inserted value should win.
  auto it = sv.begin();
  EXPECT_EQ(it->triple_, IT(1, 2, 3));
  EXPECT_FALSE(it->insertOrDelete_);
}

// Test erase functionality.
TEST(SortedVectorTest, erase) {
  SortedLocatedTriplesVector sv;

  auto lt1 = LT(0, IT(1, 2, 3), true);
  auto lt2 = LT(0, IT(2, 3, 4), true);
  auto lt3 = LT(0, IT(3, 4, 5), true);

  sv.insert(lt1);
  sv.insert(lt2);
  sv.insert(lt3);

  EXPECT_EQ(sv.size(), 3);

  sv.erase(lt2);

  EXPECT_EQ(sv.size(), 2);

  std::vector result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].triple_, IT(1, 2, 3));
  EXPECT_EQ(result[1].triple_, IT(3, 4, 5));
}

// Test that sorting is deferred until access.
TEST(SortedVectorTest, lazySorting) {
  SortedLocatedTriplesVector sv;

  sv.insert(LT(0, IT(3, 0, 0), true));
  sv.insert(LT(0, IT(1, 0, 0), true));

  // Internal state should be dirty, but we can't directly test that.
  // Access triggers sorting.
  EXPECT_EQ(sv.size(), 2);

  // Insert more after sorting.
  sv.insert(LT(0, IT(2, 0, 0), true));

  // Should re-sort on next access.
  std::vector result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].triple_, IT(1, 0, 0));
  EXPECT_EQ(result[1].triple_, IT(2, 0, 0));
  EXPECT_EQ(result[2].triple_, IT(3, 0, 0));
}

// Test equality operator.
TEST(SortedVectorTest, equality) {
  SortedLocatedTriplesVector sv1;
  SortedLocatedTriplesVector sv2;

  sv1.insert(LT(0, IT(1, 2, 3), true));
  sv1.insert(LT(0, IT(2, 3, 4), true));

  sv2.insert(LT(0, IT(2, 3, 4), true));
  sv2.insert(LT(0, IT(1, 2, 3), true));

  // Force sorting.
  sv1.ensureItemsAreSorted();
  sv2.ensureItemsAreSorted();

  EXPECT_EQ(sv1, sv2);
}

// Test with empty vector.
TEST(SortedVectorTest, emptyVector) {
  SortedLocatedTriplesVector sv;

  EXPECT_TRUE(sv.empty());
  EXPECT_EQ(sv.size(), 0);
  EXPECT_EQ(sv.begin(), sv.end());
  EXPECT_EQ(sv.rbegin(), sv.rend());
}

}  // namespace
