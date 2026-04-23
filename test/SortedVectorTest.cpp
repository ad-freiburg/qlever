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
#include "util/GTestHelpers.h"
#include "util/IdTestHelpers.h"

namespace {
auto V = ad_utility::testing::VocabId;

auto IT = [](const auto& c1, const auto& c2, const auto& c3) {
  return IdTriple{std::array<Id, 4>{V(c1), V(c2), V(c3), V(0)}};
};

auto LT = [](size_t blockIndex, const IdTriple<>& triple, bool insertOrDelete) {
  return LocatedTriple{blockIndex, triple, insertOrDelete};
};
}  // namespace

TEST(SortedVectorTest, test) {
  auto lt1 = LT(0, IT(1, 2, 3), true);
  auto lt1I = LT(0, IT(1, 2, 3), false);

  SortedLocatedTriplesVector sv;
  EXPECT_TRUE(sv.empty());
  EXPECT_EQ(sv.size(), 0);

  sv.insert(lt1);
  sv.consolidate();

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 1);
  EXPECT_EQ(*sv.begin(), lt1);

  sv.insert(lt1);
  sv.consolidate();

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 1);

  sv.insert(lt1I);
  sv.insert(lt1);
  sv.consolidate();

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

  AD_EXPECT_THROW_WITH_MESSAGE(sv.begin(), testing::_);

  sv.consolidate();

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 3);

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
  sv.consolidate();

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
  sv.consolidate();

  EXPECT_EQ(sv.size(), 3);

  // erase requires sorted data (AD_CONTRACT_CHECK inside); after erase the
  // vector stays sorted since sortedUntil_ is updated.
  sv.erase(lt2);

  EXPECT_EQ(sv.size(), 2);

  std::vector result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].triple_, IT(1, 2, 3));
  EXPECT_EQ(result[1].triple_, IT(3, 4, 5));
}

// Test that sorting must be done explicitly.
TEST(SortedVectorTest, explicitSorting) {
  SortedLocatedTriplesVector sv;

  sv.insert(LT(0, IT(3, 0, 0), true));
  sv.insert(LT(0, IT(1, 0, 0), true));

  // Must call ensureItemsAreSorted before accessing size or iterators.
  sv.consolidate();
  EXPECT_EQ(sv.size(), 2);

  // Insert more; must sort again before access.
  sv.insert(LT(0, IT(2, 0, 0), true));
  sv.consolidate();

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

  // Must sort before comparing.
  sv1.consolidate();
  sv2.consolidate();

  EXPECT_EQ(sv1, sv2);
}

// Test with empty vector.
TEST(SortedVectorTest, emptyVector) {
  SortedLocatedTriplesVector sv;

  // A freshly constructed vector satisfies sortedUntil_ == size() == 0,
  // so all contract checks pass without an explicit ensureItemsAreSorted call.
  EXPECT_TRUE(sv.empty());
  EXPECT_EQ(sv.size(), 0);
  EXPECT_EQ(sv.begin(), sv.end());
}

TEST(SortedVectorTest, sortedVector) {
  using SV = SortedLocatedTriplesVector;
  using LT = LocatedTriple;
  using LTs = std::vector<LT>;

  // Helpers: three distinct triples, each with insert/delete variants.
  auto LT1d = LT{0, IT(10, 1, 0), false};
  auto LT1i = LT{0, IT(10, 1, 0), true};
  auto LT2d = LT{0, IT(10, 2, 1), false};
  auto LT2i = LT{0, IT(10, 2, 1), true};
  auto LT3d = LT{0, IT(11, 3, 0), false};
  auto LT3i = LT{0, IT(11, 3, 0), true};

  auto expect = [](LTs input, const testing::Matcher<LTs>& expect,
                   std::optional<size_t> subrangeBegin = std::nullopt,
                   std::optional<size_t> subrangeEnd = std::nullopt,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto subrange = ql::ranges::subrange(
        input.begin() + subrangeBegin.value_or(0),
        input.begin() + subrangeEnd.value_or(input.size()));
    SV::sortAndRemoveDuplicates(input, subrange);
    EXPECT_THAT(input, expect);
  };

  expect({}, testing::IsEmpty());
  expect({LT1i}, testing::Eq(LTs{LT1i}));
  expect({LT1d, LT2i, LT3d}, testing::Eq(LTs{LT1d, LT2i, LT3d}));
  expect({LT3d, LT1i, LT2d}, testing::Eq(LTs{LT1i, LT2d, LT3d}));
  expect({LT1i, LT2d, LT3d, LT1d, LT1i, LT2i},
         testing::Eq(LTs{LT1i, LT2i, LT3d}));
  expect({LT1d, LT1i, LT1d, LT1i}, testing::Eq(LTs{LT1i}));
  expect({LT1d, LT2i, LT3i, LT2d, LT3d},
         testing::Eq(LTs{LT1d, LT2i, LT2d, LT3d}), 2);
  expect({LT1d, LT2i, LT1i, LT2d, LT1d},
         testing::Eq(LTs{LT1d, LT2i, LT1d, LT2d}), 2);
}

TEST(SortedVectorTest, eraseSortedSubRange) {
  using SV = SortedLocatedTriplesVector;
  using LTs = std::vector<LocatedTriple>;

  auto lt0 = LT(0, IT(0, 0, 0), true);  // smaller than all others
  auto lt1 = LT(0, IT(1, 2, 3), true);
  auto lt2 = LT(0, IT(2, 3, 4), true);
  auto lt3 = LT(0, IT(3, 4, 5), true);
  auto lt4 = LT(0, IT(9, 9, 9), true);  // larger than all others

  auto test = [](LTs triples, LTs toDelete, const LTs& expected,
                 ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    SV::eraseSortedSubRange(triples, toDelete);
    EXPECT_EQ(triples, expected);
  };

  test({}, {}, {});
  test({}, {lt1}, {});
  test({lt1, lt2, lt3}, {}, {lt1, lt2, lt3});
  test({lt1}, {lt1}, {});
  test({lt1}, {lt2}, {lt1});
  test({lt1, lt2, lt3}, {lt1}, {lt2, lt3});
  test({lt1, lt2, lt3}, {lt2}, {lt1, lt3});
  test({lt1, lt2, lt3}, {lt3}, {lt1, lt2});
  test({lt1, lt2, lt3}, {lt1, lt2, lt3}, {});
  test({lt1, lt2, lt3}, {lt1, lt3}, {lt2});
  test({lt1, lt2}, {lt0, lt2}, {lt1});
  test({lt1, lt2}, {lt1, lt4}, {lt2});
  test({lt1, lt2, lt3}, {lt0, lt2, lt4}, {lt1, lt3});
  test({lt1, lt2, lt3}, {lt0, lt4}, {lt1, lt2, lt3});
}

// ============================================================================
// BlockSortedLocatedTriplesVector tests
// ============================================================================

TEST(BlockSortedVectorTest, test) {
  auto lt1 = LT(0, IT(1, 2, 3), true);
  auto lt1I = LT(0, IT(1, 2, 3), false);

  BlockSortedLocatedTriplesVector sv;
  EXPECT_TRUE(sv.empty());
  EXPECT_EQ(sv.size(), 0);

  sv.insert(lt1);
  sv.consolidate();

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 1);
  EXPECT_EQ(*sv.begin(), lt1);

  sv.insert(lt1);
  sv.consolidate();

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 1);

  sv.insert(lt1I);
  sv.insert(lt1);
  sv.consolidate();

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 1);
}

TEST(BlockSortedVectorTest, insertAndIterate) {
  BlockSortedLocatedTriplesVector sv;
  EXPECT_TRUE(sv.empty());

  sv.insert(LT(0, IT(3, 2, 1), true));
  sv.insert(LT(0, IT(1, 2, 3), true));
  sv.insert(LT(0, IT(2, 2, 2), true));

  AD_EXPECT_THROW_WITH_MESSAGE(sv.begin(), testing::_);

  sv.consolidate();

  EXPECT_FALSE(sv.empty());
  EXPECT_EQ(sv.size(), 3);

  std::vector<LocatedTriple> result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].triple_, IT(1, 2, 3));
  EXPECT_EQ(result[1].triple_, IT(2, 2, 2));
  EXPECT_EQ(result[2].triple_, IT(3, 2, 1));
}

TEST(BlockSortedVectorTest, duplicatesRemoved) {
  BlockSortedLocatedTriplesVector sv;

  sv.insert(LT(0, IT(1, 2, 3), true));
  sv.insert(LT(0, IT(1, 2, 3), false));
  sv.consolidate();

  EXPECT_EQ(sv.size(), 1);

  auto it = sv.begin();
  EXPECT_EQ(it->triple_, IT(1, 2, 3));
  EXPECT_FALSE(it->insertOrDelete_);
}

TEST(BlockSortedVectorTest, erase) {
  BlockSortedLocatedTriplesVector sv;

  auto lt1 = LT(0, IT(1, 2, 3), true);
  auto lt2 = LT(0, IT(2, 3, 4), true);
  auto lt3 = LT(0, IT(3, 4, 5), true);

  sv.insert(lt1);
  sv.insert(lt2);
  sv.insert(lt3);
  sv.consolidate();

  EXPECT_EQ(sv.size(), 3);

  sv.erase(lt2);

  EXPECT_EQ(sv.size(), 2);

  std::vector result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].triple_, IT(1, 2, 3));
  EXPECT_EQ(result[1].triple_, IT(3, 4, 5));
}

TEST(BlockSortedVectorTest, explicitSorting) {
  BlockSortedLocatedTriplesVector sv;

  sv.insert(LT(0, IT(3, 0, 0), true));
  sv.insert(LT(0, IT(1, 0, 0), true));

  sv.consolidate();
  EXPECT_EQ(sv.size(), 2);

  sv.insert(LT(0, IT(2, 0, 0), true));
  sv.consolidate();

  std::vector result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].triple_, IT(1, 0, 0));
  EXPECT_EQ(result[1].triple_, IT(2, 0, 0));
  EXPECT_EQ(result[2].triple_, IT(3, 0, 0));
}

TEST(BlockSortedVectorTest, equality) {
  BlockSortedLocatedTriplesVector sv1;
  BlockSortedLocatedTriplesVector sv2;

  sv1.insert(LT(0, IT(1, 2, 3), true));
  sv1.insert(LT(0, IT(2, 3, 4), true));

  sv2.insert(LT(0, IT(2, 3, 4), true));
  sv2.insert(LT(0, IT(1, 2, 3), true));

  sv1.consolidate();
  sv2.consolidate();

  EXPECT_EQ(sv1, sv2);
}

TEST(BlockSortedVectorTest, emptyVector) {
  BlockSortedLocatedTriplesVector sv;

  EXPECT_TRUE(sv.empty());
  EXPECT_EQ(sv.size(), 0);
  EXPECT_EQ(sv.begin(), sv.end());
}

TEST(BlockSortedVectorTest, back) {
  BlockSortedLocatedTriplesVector sv;

  sv.insert(LT(0, IT(1, 0, 0), true));
  sv.insert(LT(0, IT(5, 0, 0), true));
  sv.insert(LT(0, IT(3, 0, 0), true));
  sv.consolidate();

  EXPECT_EQ(sv.back().triple_, IT(5, 0, 0));
}

TEST(BlockSortedVectorTest, batchErase) {
  BlockSortedLocatedTriplesVector sv;

  auto lt1 = LT(0, IT(1, 0, 0), true);
  auto lt2 = LT(0, IT(2, 0, 0), true);
  auto lt3 = LT(0, IT(3, 0, 0), true);
  auto lt4 = LT(0, IT(4, 0, 0), true);

  sv.insert(lt1);
  sv.insert(lt2);
  sv.insert(lt3);
  sv.insert(lt4);
  sv.consolidate();

  sv.erase(std::vector{lt2, lt4});

  EXPECT_EQ(sv.size(), 2);
  std::vector result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].triple_, IT(1, 0, 0));
  EXPECT_EQ(result[1].triple_, IT(3, 0, 0));
}

TEST(BlockSortedVectorTest, manyElementsSplitsBlocks) {
  BlockSortedLocatedTriplesVector sv;

  // Insert more than kTargetBlockSize elements to trigger splitting.
  constexpr size_t n = 2048;
  for (size_t i = 0; i < n; ++i) {
    sv.insert(LT(0, IT(i, 0, 0), true));
  }
  sv.consolidate();

  EXPECT_EQ(sv.size(), n);

  // Verify iteration yields all elements in sorted order.
  std::vector result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), n);
  for (size_t i = 0; i < n; ++i) {
    EXPECT_EQ(result[i].triple_, IT(i, 0, 0));
  }
}

TEST(BlockSortedVectorTest, eraseEntireBlock) {
  BlockSortedLocatedTriplesVector sv;

  // Insert a single element, erase it.
  auto lt1 = LT(0, IT(1, 0, 0), true);
  sv.insert(lt1);
  sv.consolidate();

  sv.erase(lt1);
  EXPECT_TRUE(sv.empty());
  EXPECT_EQ(sv.size(), 0);
  EXPECT_EQ(sv.begin(), sv.end());
}

TEST(BlockSortedVectorTest, incrementalConsolidate) {
  BlockSortedLocatedTriplesVector sv;

  // First batch.
  sv.insert(LT(0, IT(1, 0, 0), true));
  sv.insert(LT(0, IT(5, 0, 0), true));
  sv.consolidate();

  // Second batch inserts elements between existing ones.
  sv.insert(LT(0, IT(3, 0, 0), true));
  sv.insert(LT(0, IT(7, 0, 0), true));
  sv.consolidate();

  EXPECT_EQ(sv.size(), 4);
  std::vector result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 4);
  EXPECT_EQ(result[0].triple_, IT(1, 0, 0));
  EXPECT_EQ(result[1].triple_, IT(3, 0, 0));
  EXPECT_EQ(result[2].triple_, IT(5, 0, 0));
  EXPECT_EQ(result[3].triple_, IT(7, 0, 0));
}

TEST(BlockSortedVectorTest, duplicateAcrossConsolidations) {
  BlockSortedLocatedTriplesVector sv;

  // Insert a triple, consolidate, then insert the same triple with different
  // insertOrDelete.
  sv.insert(LT(0, IT(1, 0, 0), true));
  sv.consolidate();

  sv.insert(LT(0, IT(1, 0, 0), false));
  sv.consolidate();

  EXPECT_EQ(sv.size(), 1);
  EXPECT_FALSE(sv.begin()->insertOrDelete_);
}

TEST(BlockSortedVectorTest, fromSorted) {
  std::vector<LocatedTriple> sorted = {
      LT(0, IT(1, 0, 0), true),
      LT(0, IT(2, 0, 0), true),
      LT(0, IT(3, 0, 0), true),
  };
  auto sv = BlockSortedLocatedTriplesVector::fromSorted(std::move(sorted));

  EXPECT_EQ(sv.size(), 3);
  std::vector result(sv.begin(), sv.end());
  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].triple_, IT(1, 0, 0));
  EXPECT_EQ(result[1].triple_, IT(2, 0, 0));
  EXPECT_EQ(result[2].triple_, IT(3, 0, 0));
}
