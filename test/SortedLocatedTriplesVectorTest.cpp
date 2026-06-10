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
auto lt2I = LT(0, IT(1, 2, 4), false);
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

TEST(SortedLocatedTriplesVectorTest, erase) {
  {
    SV s = SV::fromSorted({lt1, lt2, lt3, lt4});
    s.erase(lt2);
    EXPECT_THAT(s, testing::ElementsAre(lt1, lt3, lt4));
    s.erase(lt4);
    EXPECT_THAT(s, testing::ElementsAre(lt1, lt3));
    s.erase(lt1);
    EXPECT_THAT(s, testing::ElementsAre(lt3));
    s.erase(lt3);
    EXPECT_THAT(s, testing::ElementsAre());
  }
  {
    SV s = SV::fromSorted({lt1, lt2, lt3, lt4, lt5, lt6});
    s.erase(std::vector<LocatedTriple>{});
    EXPECT_THAT(s, testing::ElementsAre(lt1, lt2, lt3, lt4, lt5, lt6));
    s.erase(std::vector{lt2, lt4});
    EXPECT_THAT(s, testing::ElementsAre(lt1, lt3, lt5, lt6));
    s.erase(std::vector{lt1, lt2});  // `lt2` is no longer contained.
    EXPECT_THAT(s, testing::ElementsAre(lt3, lt5, lt6));
    s.erase(std::vector{lt6, lt3,
                        lt5});  // this `erase` overload sorts the elements
    EXPECT_THAT(s, testing::ElementsAre());
  }
  {
    SV s = SV::fromSorted({lt1, lt2, lt3, lt4, lt5, lt6});
    auto erase = [&s](std::vector<LocatedTriple> triples) {
      s.eraseSorted(triples);
    };
    erase({});
    EXPECT_THAT(s, testing::ElementsAre(lt1, lt2, lt3, lt4, lt5, lt6));
    erase({lt2, lt4});
    EXPECT_THAT(s, testing::ElementsAre(lt1, lt3, lt5, lt6));
    erase({lt1, lt2});  // `lt2` is no longer contained.
    EXPECT_THAT(s, testing::ElementsAre(lt3, lt5, lt6));
    if (ad_utility::areExpensiveChecksEnabled) {
      AD_EXPECT_THROW_WITH_MESSAGE(
          erase({lt6, lt3, lt5}),
          AssertionFailed("ql::ranges::is_sorted(sortedTriples)"));
    }
    erase({lt3, lt5, lt6});  // this `erase` overload sorts the elements
    EXPECT_THAT(s, testing::ElementsAre());
  }
}

TEST(SortedLocatedTriplesVectorTest, sortAndRemoveDuplicates) {
  auto expect = [](std::vector<LocatedTriple> input, const auto& resultMatcher,
                   std::optional<size_t> subrangeBegin = std::nullopt,
                   std::optional<size_t> subrangeEnd = std::nullopt,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto subrange = ql::ranges::subrange(
        input.begin() + subrangeBegin.value_or(0),
        input.begin() + subrangeEnd.value_or(input.size()));
    SV::sortAndRemoveDuplicates(input, subrange);
    EXPECT_THAT(input, resultMatcher);
  };

  expect({}, testing::ElementsAre());
  expect({lt1}, testing::ElementsAre(lt1));
  expect({lt1I, lt2, lt3I}, testing::ElementsAre(lt1I, lt2, lt3I));
  expect({lt3I, lt1, lt2I}, testing::ElementsAre(lt1, lt2I, lt3I));
  expect({lt1, lt2I, lt3I, lt1I, lt1, lt2},
         testing::ElementsAre(lt1, lt2, lt3I));
  expect({lt1I, lt1, lt1I, lt1}, testing::ElementsAre(lt1));
  expect({lt1I, lt2, lt3, lt2I, lt3I},
         testing::ElementsAre(lt1I, lt2, lt2I, lt3I), 2);
  expect({lt1I, lt2, lt1, lt2I, lt1I},
         testing::ElementsAre(lt1I, lt2, lt1I, lt2I), 2);
  expect({lt1I, lt2, lt1, lt2I, lt1I},
         testing::ElementsAre(lt1, lt2, lt2I, lt1I), 0, 3);
}

TEST(SortedLocatedTriplesVectorTest, eraseSortedSubRange) {
  using LTs = std::vector<LocatedTriple>;

  auto test = [](LTs triples, LTs toDelete, const LTs& expected,
                 size_t numDeleted,
                 ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_EQ(SV::eraseSortedSubRange(triples, triples, toDelete), numDeleted);
    EXPECT_EQ(triples, expected);
  };

  test({}, {}, {}, 0);
  test({}, {lt1}, {}, 0);
  test({lt1, lt2, lt3}, {}, {lt1, lt2, lt3}, 0);
  test({lt1}, {lt1}, {}, 1);
  test({lt1}, {lt2}, {lt1}, 0);
  test({lt1, lt2, lt3}, {lt1}, {lt2, lt3}, 1);
  test({lt1, lt2, lt3}, {lt2}, {lt1, lt3}, 1);
  test({lt1, lt2, lt3}, {lt3}, {lt1, lt2}, 1);
  test({lt1, lt2, lt3}, {lt1, lt2, lt3}, {}, 3);
  test({lt1, lt2, lt3}, {lt1, lt3}, {lt2}, 2);
  test({lt2, lt3}, {lt1, lt3}, {lt2}, 1);
  test({lt1, lt2}, {lt1, lt4}, {lt2}, 1);
  test({lt2, lt3, lt4}, {lt1, lt3, lt5}, {lt2, lt4}, 1);
  test({lt2, lt3, lt4}, {lt1, lt5}, {lt2, lt3, lt4}, 0);
}

TEST(SortedLocatedTriplesVectorTest, equality) {
  EXPECT_EQ(SV::fromSorted({lt1, lt2}), SV::fromSorted({lt1, lt2}));
  EXPECT_NE(SV::fromSorted({lt1, lt2}), SV::fromSorted({lt3}));
  {
    SV s1;
    SV s2;

    s1.insert(lt1);
    s1.insert(lt2);

    s2.insert(lt2);
    s2.insert(lt1);

    AD_EXPECT_THROW_WITH_MESSAGE(s1 == s2, NotClean);
    s1.consolidate();
    s2.consolidate();

    EXPECT_EQ(s1, s2);
  }
  {
    SV s1;
    SV s2;

    s1.insert(lt1);
    s1.insert(lt2);
    s1.insert(lt3);
    s1.insert(lt4);
    s1.insert(lt5);
    s1.consolidate();
    s1.insert(lt6);
    s1.consolidate();

    s2.insert(lt6);
    s2.insert(lt5);
    s2.insert(lt4);
    s2.insert(lt3);
    s2.insert(lt2);
    s2.consolidate();
    s2.insert(lt1);
    s2.consolidate();

    EXPECT_NE(s1, s2);
  }
}
