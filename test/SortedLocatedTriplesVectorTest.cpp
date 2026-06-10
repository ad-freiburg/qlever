// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
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

auto lt1 = LT(0, IT(1, 2, 3), true);
auto lt1I = LT(0, IT(1, 2, 3), false);
auto lt2 = LT(0, IT(1, 2, 4), true);
auto lt3 = LT(0, IT(1, 2, 5), true);
auto lt3I = LT(0, IT(1, 2, 5), false);
auto lt4 = LT(0, IT(2, 1, 1), true);
auto lt5 = LT(0, IT(2, 1, 3), true);
auto lt5I = LT(0, IT(2, 1, 3), false);
auto lt6 = LT(0, IT(3, 3, 3), true);

using SV = SortedLocatedTriplesVector;
auto SizesAre = [](size_t size, size_t sizeForTesting) {
  return testing::AllOf(
      AD_PROPERTY(SV, size, testing::Eq(size)),
      AD_PROPERTY(SV, sizeForTesting, testing::Eq(sizeForTesting)));
};
auto AssertionFailed = [](const std::string& assertion) {
  return testing::HasSubstr("Assertion `" + assertion + "` failed");
};
auto NotClean = AssertionFailed("isClean()");
auto Empty = AssertionFailed("!empty()");
}  // namespace

using namespace ad_utility::testing;

// `SortedLocatedTriplesVector` satisfies the range concept.
static_assert(ql::ranges::range<SV>);

TEST(SortedLocatedTriplesVectorTest, constructor) {
  {
    SV s{};
    EXPECT_THAT(s, testing::IsEmpty());
    EXPECT_THAT(s, testing::SizeIs(0));
    EXPECT_THAT(s.sizeForTesting(), testing::Eq(0));
    EXPECT_THAT(s, testing::ElementsAre());
    EXPECT_EQ(s.begin(), s.end());
  }
  {
    SV s = SV::fromSorted({});
    EXPECT_THAT(s, testing::IsEmpty());
    EXPECT_THAT(s, testing::SizeIs(0));
    EXPECT_THAT(s.sizeForTesting(), testing::Eq(0));
    EXPECT_THAT(s, testing::ElementsAre());
    EXPECT_EQ(s.begin(), s.end());
  }
  {
    if (ad_utility::areExpensiveChecksEnabled) {
      AD_EXPECT_THROW_WITH_MESSAGE(
          SV::fromSorted({lt2, lt1}),
          AssertionFailed(
              "ql::ranges::is_sorted(sortedTriples, LocatedTripleCompare{})"));
      AD_EXPECT_THROW_WITH_MESSAGE(
          SV::fromSorted({lt1, lt1I}),
          AssertionFailed("ql::ranges::adjacent_find(sortedTriples, {}, "
                          "&LocatedTriple::triple_) == sortedTriples.end()"));
    }
  }
  {
    SV s = SV::fromSorted({lt1, lt2});
    EXPECT_THAT(s, testing::Not(testing::IsEmpty()));
    EXPECT_THAT(s, testing::SizeIs(2));
    EXPECT_THAT(s.sizeForTesting(), testing::Eq(2));
    EXPECT_THAT(s, testing::ElementsAre(lt1, lt2));
    EXPECT_NE(s.begin(), s.end());
  }
}

TEST(SortedLocatedTriplesVectorTest, empty) {
  auto insertInto = [](SV& sv, LocatedTriple lt,
                       ad_utility::source_location loc =
                           AD_CURRENT_SOURCE_LOC()) {
    auto t = generateLocationTrace(loc);
    sv.insert(std::move(lt));
    sv.consolidate(0);
    EXPECT_FALSE(sv.empty());
  };
  {
    SV s{};
    EXPECT_TRUE(s.empty());
  }
  {
    SV s = SV::fromSorted({});
    EXPECT_TRUE(s.empty());
  }
  {
    SV s = SV::fromSorted({lt1});
    EXPECT_FALSE(s.empty());
  }
  {
    SV s{};
    insertInto(s, lt1);
    insertInto(s, lt1);
    insertInto(s, lt1I);
    s.erase(lt1I);
    EXPECT_TRUE(s.empty());
  }
  {
    SV s{};
    insertInto(s, lt1);
    s.clear();
    EXPECT_TRUE(s.empty());
  }
  {
    SV s{};
    ad_utility::FastRandomIntGenerator<uint64_t> rng{};
    auto rndVocabId = [&rng]() { return VocabId(rng() % Id::maxIndex); };
    for (size_t i = 0; i < 1000; ++i) {
      insertInto(s, LT(0,
                       IdTriple{std::array{rndVocabId(), rndVocabId(),
                                           rndVocabId(), rndVocabId()}},
                       rng() % 2 == 0));
    }
    s.clear();
    EXPECT_TRUE(s.empty());
  }
}

TEST(SortedLocatedTriplesVectorTest, size) {
  {
    SV s{};
    s.insert(lt1);
    AD_EXPECT_THROW_WITH_MESSAGE(s.size(), NotClean);
    AD_EXPECT_THROW_WITH_MESSAGE(s.sizeForTesting(), NotClean);
  }
  {
    SV s{};
    EXPECT_THAT(s, SizesAre(0, 0));
    s.insert(lt1);
    s.consolidate();
    EXPECT_THAT(s, SizesAre(1, 1));
    s.insert(lt1);  // Inserted twice, does not contribute.
    s.insert(lt2);
    s.insert(lt3);
    s.insert(lt4);
    s.insert(lt5);
    s.insert(lt6);
    s.consolidate();
    EXPECT_THAT(s, SizesAre(6, 6));
    // Due to the relative sizes of the two parts both `lt1` and `lt1I` exist in
    // the internal storage. When iterating only the `lt1I` will be returned.
    s.insert(lt1I);
    s.consolidate();
    EXPECT_THAT(s, SizesAre(7, 6));
    s.erase(lt2);
    EXPECT_THAT(s, SizesAre(6, 5));
    // `lt1` is shadowed by `lt1I` but kept because the two parts are not
    // merged. Removing it resolve the discrepancy between `size` and
    // `sizeForTesting`.
    s.erase(lt1);
    EXPECT_THAT(s, SizesAre(5, 5));
    s.clear();
    EXPECT_THAT(s, SizesAre(0, 0));
  }
}

TEST(SortedLocatedTriplesVectorTest, clear) {
  SV s{};
  EXPECT_THAT(s, testing::IsEmpty());
  s.clear();
  EXPECT_THAT(s, testing::IsEmpty());
  s.insert(lt1);
  s.insert(lt2);
  s.insert(lt3);
  s.consolidate();
  EXPECT_THAT(s, testing::Not(testing::IsEmpty()));
  s.clear();
  EXPECT_TRUE(s.empty());
}

TEST(SortedLocatedTriplesVectorTest, insert) {
  SV s{};
  EXPECT_TRUE(s.smallPartIsSorted_);
  s.insert(lt1);
  EXPECT_FALSE(s.smallPartIsSorted_);
  EXPECT_THAT(s, AD_FIELD(SV, triples_, testing::ElementsAre(lt1)));
  s.insert(lt1I);
  s.insert(lt4);
  s.insert(lt3);
  s.insert(lt2);
  EXPECT_THAT(s, AD_FIELD(SV, triples_,
                          testing::ElementsAre(lt1, lt1I, lt4, lt3, lt2)));
  EXPECT_FALSE(s.smallPartIsSorted_);
  s.consolidate();
  EXPECT_THAT(
      s, AD_FIELD(SV, triples_, testing::ElementsAre(lt1I, lt2, lt3, lt4)));
  EXPECT_TRUE(s.smallPartIsSorted_);
  s.insert(lt1);
  EXPECT_THAT(s, AD_FIELD(SV, triples_,
                          testing::ElementsAre(lt1I, lt2, lt3, lt4, lt1)));
  EXPECT_FALSE(s.smallPartIsSorted_);
  s.consolidate();
  EXPECT_THAT(s, SizesAre(5, 4));
  EXPECT_TRUE(s.smallPartIsSorted_);
}

TEST(SortedLocatedTriplesVectorTest, back) {
  // Runs `testLamda` on with `const SV&` and `SV&` to test the const overloads.
  auto testOverloads = [](SV& initial, auto testLambda,
                          ad_utility::source_location l =
                              AD_CURRENT_SOURCE_LOC()) {
    {
      auto trace =
          generateLocationTrace(l, "During execution of const overload");
      testLambda(std::as_const(initial));
    }
    {
      auto trace =
          generateLocationTrace(l, "During execution of non-const overload");
      testLambda(initial);
    }
  };
  {
    SV s{};
    testOverloads(
        s, [](auto& s) { AD_EXPECT_THROW_WITH_MESSAGE(s.back(), Empty); });
  }
  {
    SV s = SV::fromSorted({lt1, lt2, lt3, lt4, lt5});
    testOverloads(s, [](auto& s) { EXPECT_EQ(s.back(), lt5); });
    s.insert(lt5I);  // `lt5I` will be in the small part
    testOverloads(
        s, [](auto& s) { AD_EXPECT_THROW_WITH_MESSAGE(s.back(), NotClean); });
    s.consolidate();
    testOverloads(s, [](auto& s) { EXPECT_EQ(s.back(), lt5I); });
    s.insert(lt6);  // `lt6` will be in the small part
    s.consolidate();
    testOverloads(s, [](auto& s) { EXPECT_EQ(s.back(), lt6); });
  }
}

TEST(SortedLocatedTriplesVectorTest, iteration) {
  auto expectElements =
      [](SV& sv, const std::vector<LocatedTriple>& expectedTriples,
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        {
          auto trace =
              generateLocationTrace(l, "During execution of const overload");
          EXPECT_THAT(std::as_const(sv),
                      testing::ElementsAreArray(expectedTriples));
        }
        {
          auto trace = generateLocationTrace(
              l, "During execution of non-const overload");
          EXPECT_THAT(sv, testing::ElementsAreArray(expectedTriples));
        }
      };
  {
    SV s{};
    expectElements(s, {});
  }
  {
    SV s = SV::fromSorted({lt1, lt2});
    expectElements(s, {lt1, lt2});
    s.insert(lt1I);
    s.consolidate();
    expectElements(s, {lt1I, lt2});
    s.insert(lt3);
    s.insert(lt5);
    s.consolidate();
    expectElements(s, {lt1I, lt2, lt3, lt5});
    s.clear();
    expectElements(s, {});
  }
}

// ====== Old part. ======

// Test erase functionality.
TEST(SortedLocatedTriplesVectorTest, erase) {
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

// Test equality operator.
TEST(SortedLocatedTriplesVectorTest, equality) {
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

TEST(SortedLocatedTriplesVectorTest, sortedVector) {
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

TEST(SortedLocatedTriplesVectorTest, eraseSortedSubRange) {
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

TEST(BlockSortedLocatedTriplesVectorTest, test) {
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

TEST(BlockSortedLocatedTriplesVectorTest, insertAndIterate) {
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

TEST(BlockSortedLocatedTriplesVectorTest, duplicatesRemoved) {
  BlockSortedLocatedTriplesVector sv;

  sv.insert(LT(0, IT(1, 2, 3), true));
  sv.insert(LT(0, IT(1, 2, 3), false));
  sv.consolidate();

  EXPECT_EQ(sv.size(), 1);

  auto it = sv.begin();
  EXPECT_EQ(it->triple_, IT(1, 2, 3));
  EXPECT_FALSE(it->insertOrDelete_);
}

TEST(BlockSortedLocatedTriplesVectorTest, erase) {
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

TEST(BlockSortedLocatedTriplesVectorTest, explicitSorting) {
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

TEST(BlockSortedLocatedTriplesVectorTest, equality) {
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

TEST(BlockSortedLocatedTriplesVectorTest, emptyVector) {
  BlockSortedLocatedTriplesVector sv;

  EXPECT_TRUE(sv.empty());
  EXPECT_EQ(sv.size(), 0);
  EXPECT_EQ(sv.begin(), sv.end());
}

TEST(BlockSortedLocatedTriplesVectorTest, back) {
  BlockSortedLocatedTriplesVector sv;

  sv.insert(LT(0, IT(1, 0, 0), true));
  sv.insert(LT(0, IT(5, 0, 0), true));
  sv.insert(LT(0, IT(3, 0, 0), true));
  sv.consolidate();

  EXPECT_EQ(sv.back().triple_, IT(5, 0, 0));
}

TEST(BlockSortedLocatedTriplesVectorTest, batchErase) {
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

TEST(BlockSortedLocatedTriplesVectorTest, manyElementsSplitsBlocks) {
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

TEST(BlockSortedLocatedTriplesVectorTest, eraseEntireBlock) {
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

TEST(BlockSortedLocatedTriplesVectorTest, incrementalConsolidate) {
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

TEST(BlockSortedLocatedTriplesVectorTest, duplicateAcrossConsolidations) {
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

TEST(BlockSortedLocatedTriplesVectorTest, fromSorted) {
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
