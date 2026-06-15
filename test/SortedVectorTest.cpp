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

namespace ad_utility {
namespace {

using namespace ::testing;
using Pair = std::pair<int, int>;
using SV = SortedVector<Pair, std::less<>, MemberProj<&Pair::first>>;

auto p10 = Pair{1, 0};
auto p11 = Pair{1, 1};
auto p20 = Pair{2, 0};
auto p21 = Pair{2, 1};
auto p30 = Pair{3, 0};
auto p31 = Pair{3, 1};
auto p40 = Pair{4, 0};
auto p50 = Pair{5, 0};
auto p51 = Pair{5, 1};
auto p60 = Pair{6, 0};

auto SizesAre = [](size_t sizeUpperBound, size_t sizeForTesting) {
  return AllOf(AD_PROPERTY(SV, sizeUpperBound, Eq(sizeUpperBound)),
               AD_PROPERTY(SV, sizeForTesting, Eq(sizeForTesting)));
};
auto AssertionFailed = [](const std::string& assertion) {
  return HasSubstr("Assertion `" + assertion + "` failed");
};
auto NotClean = AssertionFailed("isClean()");
auto IsEmpty = AD_PROPERTY(SV, empty, Eq(true));
auto IsNotEmpty = AD_PROPERTY(SV, empty, Not(Eq(true)));

auto testConstOverloads = [](SV& initial, auto testLambda,
                             source_location l = AD_CURRENT_SOURCE_LOC()) {
  {
    auto trace = generateLocationTrace(l, "During execution of const overload");
    testLambda(std::as_const(initial));
  }
  {
    auto trace =
        generateLocationTrace(l, "During execution of non-const overload");
    testLambda(initial);
  }
};
}  // namespace

struct SortedVectorPairsTestHelper {
  static auto stateIs(const std::vector<Pair>& elements,
                      size_t numElementsLargePart, bool smallPartIsSorted) {
    return AllOf(AD_FIELD(SV, elements_, ElementsAreArray(elements)),
                 AD_FIELD(SV, numItemsLargePart_, Eq(numElementsLargePart)),
                 AD_FIELD(SV, smallPartIsSorted_, Eq(smallPartIsSorted)));
  }
};

namespace {
auto StateIs = SortedVectorPairsTestHelper::stateIs;
}  // namespace

static_assert(ql::ranges::range<SV>);

TEST(SortedVectorTest, constructor) {
  {
    SV s{};
    EXPECT_THAT(s, SizesAre(0, 0));
    EXPECT_TRUE(s.empty());
    EXPECT_THAT(s, ElementsAre());
    EXPECT_EQ(s.begin(), s.end());
    EXPECT_THAT(s, StateIs({}, 0, true));
  }
  {
    SV s = SV::fromSorted({});
    EXPECT_THAT(s, SizesAre(0, 0));
    EXPECT_TRUE(s.empty());
    EXPECT_THAT(s, ElementsAre());
    EXPECT_EQ(s.begin(), s.end());
    EXPECT_THAT(s, StateIs({}, 0, true));
  }
  {
    if (areExpensiveChecksEnabled) {
      AD_EXPECT_THROW_WITH_MESSAGE(
          SV::fromSorted({p20, p10}),
          AssertionFailed("ql::ranges::is_sorted(sortedElements, comp, proj)"));
      AD_EXPECT_THROW_WITH_MESSAGE(
          SV::fromSorted({p10, p11}),
          AssertionFailed(
              "ql::ranges::adjacent_find(sortedElements, {}, proj) == "
              "sortedElements.end()"));
    }
  }
  {
    SV s = SV::fromSorted({p10, p20});
    EXPECT_THAT(s, SizesAre(2, 2));
    EXPECT_FALSE(s.empty());
    EXPECT_THAT(s, ElementsAre(p10, p20));
    EXPECT_NE(s.begin(), s.end());
    EXPECT_THAT(s, StateIs({p10, p20}, 2, true));
  }
}

TEST(SortedVectorTest, empty) {
  auto insertInto = [](SV& sv, Pair p,
                       source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto t = generateLocationTrace(loc);
    sv.insert(std::move(p));
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
    SV s = SV::fromSorted({p10});
    EXPECT_FALSE(s.empty());
  }
  {
    SV s{};
    insertInto(s, p10);
    insertInto(s, p10);
    insertInto(s, p11);
    s.erase(p11);
    EXPECT_TRUE(s.empty());
  }
  {
    SV s{};
    insertInto(s, p10);
    s.clear();
    EXPECT_TRUE(s.empty());
  }
  {
    SV s{};
    FastRandomIntGenerator<uint64_t> rng{};
    for (size_t i = 0; i < 100; ++i) {
      insertInto(
          s, {static_cast<int>(rng() % 1000), static_cast<int>(rng() % 1000)});
    }
    s.clear();
    EXPECT_TRUE(s.empty());
  }
}

TEST(SortedVectorTest, size) {
  {
    SV s{};
    s.insert(p10);
    AD_EXPECT_THROW_WITH_MESSAGE(s.sizeUpperBound(), NotClean);
    AD_EXPECT_THROW_WITH_MESSAGE(s.sizeForTesting(), NotClean);
  }
  {
    SV s{};
    EXPECT_THAT(s, SizesAre(0, 0));
    s.insert(p10);
    s.consolidate();
    EXPECT_THAT(s, SizesAre(1, 1));
    s.insert(p10);  // Inserted twice, does not contribute.
    s.insert(p20);
    s.insert(p30);
    s.insert(p40);
    s.insert(p50);
    s.insert(p60);
    s.consolidate();
    EXPECT_THAT(s, SizesAre(6, 6));
    // Due to the relative sizes of the two parts both `p1` and `p1I` exist in
    // the internal storage. When iterating only `p1I` will be returned.
    s.insert(p11);
    s.consolidate();
    EXPECT_THAT(s, SizesAre(7, 6));
    s.erase(p20);
    EXPECT_THAT(s, SizesAre(6, 5));
    // `p10` is shadowed by `p11` but kept because the two parts are not merged.
    // Removing it resolves the discrepancy between `size` and `sizeForTesting`.
    s.erase(p10);
    EXPECT_THAT(s, SizesAre(5, 5));
    s.clear();
    EXPECT_THAT(s, SizesAre(0, 0));
  }
}

TEST(SortedVectorTest, clear) {
  SV s{};
  EXPECT_THAT(s, IsEmpty);
  s.clear();
  EXPECT_THAT(s, IsEmpty);
  s.insert(p10);
  s.insert(p20);
  s.insert(p30);
  s.consolidate();
  EXPECT_THAT(s, IsNotEmpty);
  s.clear();
  EXPECT_TRUE(s.empty());
}

TEST(SortedVectorTest, insert) {
  SV s{};
  EXPECT_TRUE(s.smallPartIsSorted_);
  s.insert(p10);
  EXPECT_FALSE(s.smallPartIsSorted_);
  EXPECT_THAT(s, AD_FIELD(SV, elements_, ElementsAre(p10)));
  s.insert(p11);
  s.insert(p40);
  s.insert(p30);
  s.insert(p20);
  EXPECT_THAT(s, AD_FIELD(SV, elements_, ElementsAre(p10, p11, p40, p30, p20)));
  EXPECT_FALSE(s.smallPartIsSorted_);
  s.consolidate();
  // p1I is the last-inserted element for key=1, so it wins deduplication.
  EXPECT_THAT(s, AD_FIELD(SV, elements_, ElementsAre(p11, p20, p30, p40)));
  EXPECT_TRUE(s.smallPartIsSorted_);
  s.insert(p10);
  EXPECT_THAT(s, AD_FIELD(SV, elements_, ElementsAre(p11, p20, p30, p40, p10)));
  EXPECT_FALSE(s.smallPartIsSorted_);
  s.consolidate();
  EXPECT_THAT(s, SizesAre(5, 4));
  EXPECT_TRUE(s.smallPartIsSorted_);
}

TEST(SortedVectorTest, back) {
  {
    SV s{};
    testConstOverloads(s, [](auto& s) {
      AD_EXPECT_THROW_WITH_MESSAGE(s.back(), AssertionFailed("!empty()"));
    });
  }
  {
    SV s = SV::fromSorted({p10, p20, p30, p40, p50});
    // small part is empty
    testConstOverloads(s, [](auto& s) { EXPECT_EQ(s.back(), p50); });
    s.insert(p21);
    s.consolidate();
    // The small part is non-empty but the large part has the largest element.
    testConstOverloads(s, [](auto& s) { EXPECT_EQ(s.back(), p50); });
    s.insert(p51);  // `p5I` will be in the small part
    testConstOverloads(
        s, [](auto& s) { AD_EXPECT_THROW_WITH_MESSAGE(s.back(), NotClean); });
    s.consolidate();
    testConstOverloads(s, [](auto& s) { EXPECT_EQ(s.back(), p51); });
    s.insert(p60);  // `p6` will be in the small part
    s.consolidate();
    testConstOverloads(s, [](auto& s) { EXPECT_EQ(s.back(), p60); });
  }
}

TEST(SortedVectorTest, iteration) {
  auto expectElements = [&](SV& sv,
                            const std::vector<std::pair<int, int>>& expected,
                            source_location l = AD_CURRENT_SOURCE_LOC()) {
    testConstOverloads(
        sv,
        [&expected](auto& sv) { EXPECT_THAT(sv, ElementsAreArray(expected)); },
        l);
  };
  {
    SV s{};
    expectElements(s, {});
  }
  {
    SV s = SV::fromSorted({p10, p20});
    expectElements(s, {p10, p20});
    s.insert(p11);
    s.consolidate();
    expectElements(s, {p11, p20});
    s.insert(p30);
    s.insert(p50);
    s.consolidate();
    expectElements(s, {p11, p20, p30, p50});
    s.clear();
    expectElements(s, {});
  }
  {
    SV s{};
    s.insert(p10);
    testConstOverloads(
        s, [](auto& s) { AD_EXPECT_THROW_WITH_MESSAGE(s.begin(), NotClean); });
    testConstOverloads(
        s, [](auto& s) { AD_EXPECT_THROW_WITH_MESSAGE(s.end(), NotClean); });
  }
}

TEST(SortedVectorTest, erase) {
  {
    SV s = SV::fromSorted({p10, p20, p30, p40});
    s.erase(p20);
    EXPECT_THAT(s, ElementsAre(p10, p30, p40));
    s.erase(p40);
    EXPECT_THAT(s, ElementsAre(p10, p30));
    s.erase(p10);
    EXPECT_THAT(s, ElementsAre(p30));
    s.insert(p20);
    AD_EXPECT_THROW_WITH_MESSAGE(s.erase(p30), NotClean);
    s.consolidate();
    s.erase(p30);
    EXPECT_THAT(s, ElementsAre(p20));
  }
  {
    SV s = SV::fromSorted({p10, p20, p30, p40, p50, p60});
    s.eraseUnsorted(std::vector<std::pair<int, int>>{});
    EXPECT_THAT(s, ElementsAre(p10, p20, p30, p40, p50, p60));
    s.eraseUnsorted(std::vector{p20, p40});
    EXPECT_THAT(s, ElementsAre(p10, p30, p50, p60));
    s.eraseUnsorted(std::vector{p10, p20});  // `p2` is no longer contained.
    EXPECT_THAT(s, ElementsAre(p30, p50, p60));
    s.insert(p10);
    // this `erase` overload sorts the elements
    AD_EXPECT_THROW_WITH_MESSAGE(s.eraseUnsorted({p60, p30, p50, p10}),
                                 NotClean);
    s.consolidate();
    s.eraseUnsorted(std::vector{p60, p30, p50, p10});
    EXPECT_THAT(s, ElementsAre());
  }
  {
    SV s = SV::fromSorted({p10, p20, p30, p40, p50, p60});
    auto erase = [&s](std::vector<std::pair<int, int>> pairs) {
      s.eraseSorted(pairs);
    };
    erase({});
    EXPECT_THAT(s, ElementsAre(p10, p20, p30, p40, p50, p60));
    erase({p20, p40});
    EXPECT_THAT(s, ElementsAre(p10, p30, p50, p60));
    s.insert(p40);
    AD_EXPECT_THROW_WITH_MESSAGE(erase({p10, p20, p40}), NotClean);
    s.consolidate();
    erase({p10, p20, p40});  // `p2` is no longer contained.
    EXPECT_THAT(s, ElementsAre(p30, p50, p60));
    if (areExpensiveChecksEnabled) {
      AD_EXPECT_THROW_WITH_MESSAGE(
          erase({p60, p30, p50}),
          AssertionFailed("ql::ranges::is_sorted(sortedElems, comp_, proj_)"));
    }
    erase({p30, p50, p60});
    EXPECT_THAT(s, ElementsAre());
  }
}

TEST(SortedVectorTest, sortAndRemoveDuplicates) {
  using Pairs = std::vector<std::pair<int, int>>;
  auto expect = [](Pairs input, const auto& resultMatcher,
                   std::optional<size_t> subrangeBegin = std::nullopt,
                   std::optional<size_t> subrangeEnd = std::nullopt,
                   source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto subrange = ql::ranges::subrange(
        input.begin() + subrangeBegin.value_or(0),
        input.begin() + subrangeEnd.value_or(input.size()));
    SV::sortAndRemoveDuplicates(input, subrange);
    EXPECT_THAT(input, resultMatcher);
  };

  expect({}, ElementsAre());
  expect({p10}, ElementsAre(p10));
  expect({p11, p20, p31}, ElementsAre(p11, p20, p31));
  expect({p31, p10, p21}, ElementsAre(p10, p21, p31));
  expect({p10, p21, p31, p11, p10, p20}, ElementsAre(p10, p20, p31));
  expect({p11, p10, p11, p10}, ElementsAre(p10));
  expect({p11, p20, p30, p21, p31}, ElementsAre(p11, p20, p21, p31), 2);
  expect({p11, p20, p10, p21, p11}, ElementsAre(p11, p20, p11, p21), 2);
  expect({p11, p20, p10, p21, p11}, ElementsAre(p10, p20, p21, p11), 0, 3);
}

TEST(SortedVectorTest, eraseSortedSubRange) {
  using Pairs = std::vector<std::pair<int, int>>;

  auto test = [](Pairs triples, Pairs toDelete, const Pairs& expected,
                 size_t numDeleted,
                 source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_EQ(SV::eraseSortedSubRange(triples, triples, toDelete), numDeleted);
    EXPECT_EQ(triples, expected);
  };

  test({}, {}, {}, 0);
  test({}, {p10}, {}, 0);
  test({p10, p20, p30}, {}, {p10, p20, p30}, 0);
  test({p10}, {p10}, {}, 1);
  test({p10}, {p20}, {p10}, 0);
  test({p10, p20, p30}, {p10}, {p20, p30}, 1);
  test({p10, p20, p30}, {p20}, {p10, p30}, 1);
  test({p10, p20, p30}, {p30}, {p10, p20}, 1);
  test({p10, p20, p30}, {p10, p20, p30}, {}, 3);
  test({p10, p20, p30}, {p10, p30}, {p20}, 2);
  test({p20, p30}, {p10, p30}, {p20}, 1);
  test({p10, p20}, {p10, p40}, {p20}, 1);
  test({p20, p30, p40}, {p10, p30, p50}, {p20, p40}, 1);
  test({p20, p30, p40}, {p10, p50}, {p20, p30, p40}, 0);
}

}  // namespace ad_utility
