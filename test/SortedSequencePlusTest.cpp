// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <map>
#include <random>
#include <utility>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/SortedSequencePlus.h"
#include "util/TransparentFunctors.h"

namespace ad_utility {
namespace {

using namespace ::testing;
using Pair = std::pair<int, int>;
// A small block size, so that the tests exercise block splitting.
constexpr size_t testBlockSize = 4;
using SV = SortedSequencePlus<Pair, std::less<>, MemberProjection<&Pair::first>,
                              testBlockSize>;

// Insert `elems` and consolidate.
void insertAll(SV& s, const std::vector<Pair>& elems) {
  for (const auto& elem : elems) {
    s.insert(elem);
  }
  s.consolidate();
}

// The elements of `s` as a vector (requires a consolidated state).
std::vector<Pair> elementsOf(const SV& s) {
  std::vector<Pair> result;
  ql::ranges::copy(s.getSortedView(), std::back_inserter(result));
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(SortedSequencePlusTest, emptyAndClear) {
  SV s;
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.sizeUpperBound(), 0);
  EXPECT_EQ(s.sizeForTesting(), 0);
  EXPECT_THAT(elementsOf(s), ElementsAre());

  s.insert({1, 0});
  EXPECT_FALSE(s.empty());
  s.consolidate();
  EXPECT_THAT(elementsOf(s), ElementsAre(Pair{1, 0}));
  s.clear();
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.sizeForTesting(), 0);
}

// _____________________________________________________________________________
TEST(SortedSequencePlusTest, insertConsolidateAndDeduplicate) {
  SV s;
  // Unsorted input with duplicate keys; the last insert for a key wins.
  insertAll(s, {{3, 0}, {1, 0}, {2, 0}, {1, 1}, {3, 1}});
  EXPECT_THAT(elementsOf(s), ElementsAre(Pair{1, 1}, Pair{2, 0}, Pair{3, 1}));
  EXPECT_EQ(s.sizeForTesting(), 3);

  // A later consolidate overrides keys from earlier rounds and mixes in new
  // ones.
  insertAll(s, {{2, 5}, {0, 5}, {4, 5}});
  EXPECT_THAT(elementsOf(s), ElementsAre(Pair{0, 5}, Pair{1, 1}, Pair{2, 5},
                                         Pair{3, 1}, Pair{4, 5}));
}

// _____________________________________________________________________________
TEST(SortedSequencePlusTest, blockSplitting) {
  // Insert far more elements than fit in one block (block size 4), in
  // descending order, across several consolidation rounds.
  SV s;
  for (int round = 0; round < 5; ++round) {
    for (int i = 19 - round; i >= 0; i -= 5) {
      s.insert({i, round});
    }
    s.consolidate();
  }
  // All 20 keys are present, each with the value of the round it was inserted
  // in (each key is inserted exactly once).
  auto elems = elementsOf(s);
  ASSERT_EQ(elems.size(), 20);
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ(elems[i].first, i);
    EXPECT_EQ(elems[i].second, (19 - i) % 5);
  }
  EXPECT_EQ(s.sizeForTesting(), 20);
}

// _____________________________________________________________________________
TEST(SortedSequencePlusTest, fromSorted) {
  std::vector<Pair> sorted;
  for (int i = 0; i < 11; ++i) {
    sorted.push_back({i, i});
  }
  auto s = SV::fromSorted(sorted);
  EXPECT_EQ(s.sizeForTesting(), 11);
  EXPECT_THAT(elementsOf(s), ElementsAreArray(sorted));
  EXPECT_EQ(s.front(), (Pair{0, 0}));
  EXPECT_EQ(s.back(), (Pair{10, 10}));

  // Inserting into a `fromSorted` container works (and overrides).
  insertAll(s, {{5, 100}, {11, 100}});
  EXPECT_EQ(s.sizeForTesting(), 12);
  EXPECT_EQ(elementsOf(s)[5], (Pair{5, 100}));
  EXPECT_EQ(s.back(), (Pair{11, 100}));
}

// _____________________________________________________________________________
TEST(SortedSequencePlusTest, frontAndBack) {
  SV s;
  insertAll(s, {{5, 0}, {1, 0}, {9, 0}});
  EXPECT_EQ(s.front(), (Pair{1, 0}));
  EXPECT_EQ(s.back(), (Pair{9, 0}));
  // The non-const overloads allow modifying the values (not the keys).
  s.back().second = 42;
  s.front().second = 43;
  EXPECT_THAT(elementsOf(s), ElementsAre(Pair{1, 43}, Pair{5, 0}, Pair{9, 42}));
}

// _____________________________________________________________________________
TEST(SortedSequencePlusTest, eraseSingle) {
  SV s;
  insertAll(s, {{1, 0}, {2, 0}, {3, 0}, {4, 0}, {5, 0}, {6, 0}});
  // Erase is by projected key; the value does not matter.
  s.erase({3, 999});
  EXPECT_THAT(elementsOf(s), ElementsAre(Pair{1, 0}, Pair{2, 0}, Pair{4, 0},
                                         Pair{5, 0}, Pair{6, 0}));
  // Erasing an absent key is a no-op.
  s.erase({3, 0});
  s.erase({100, 0});
  EXPECT_EQ(s.sizeForTesting(), 5);
  // Erase everything, one by one.
  for (int i = 1; i <= 6; ++i) {
    s.erase({i, 0});
  }
  EXPECT_TRUE(s.empty());
}

// _____________________________________________________________________________
TEST(SortedSequencePlusTest, eraseSortedAndUnsorted) {
  SV s;
  std::vector<Pair> elems;
  for (int i = 0; i < 20; ++i) {
    elems.push_back({i, 0});
  }
  insertAll(s, elems);

  std::vector<Pair> toDelete{{3, 0}, {4, 0}, {11, 0}, {17, 0}, {17, 1}};
  s.eraseSorted(ql::span(toDelete));
  EXPECT_EQ(s.sizeForTesting(), 16);
  auto after = elementsOf(s);
  for (const auto& p : after) {
    EXPECT_TRUE(p.first != 3 && p.first != 4 && p.first != 11 && p.first != 17);
  }

  std::vector<Pair> toDeleteUnsorted{{19, 0}, {0, 0}, {10, 0}};
  s.eraseUnsorted(std::move(toDeleteUnsorted));
  EXPECT_EQ(s.sizeForTesting(), 13);
  EXPECT_EQ(s.front(), (Pair{1, 0}));
  EXPECT_EQ(s.back(), (Pair{18, 0}));
}

// _____________________________________________________________________________
TEST(SortedSequencePlusTest, contractChecks) {
  SV s;
  s.insert({1, 0});
  // Reading before `consolidate` fails the contract.
  AD_EXPECT_THROW_WITH_MESSAGE(s.getSortedView(),
                               HasSubstr("isConsolidated()"));
  AD_EXPECT_THROW_WITH_MESSAGE(s.sizeForTesting(),
                               HasSubstr("isConsolidated()"));
  AD_EXPECT_THROW_WITH_MESSAGE(s.back(), HasSubstr("isConsolidated()"));
  s.consolidate();
  EXPECT_NO_THROW(s.getSortedView());

  SV empty;
  AD_EXPECT_THROW_WITH_MESSAGE(empty.front(), HasSubstr("!empty()"));
  AD_EXPECT_THROW_WITH_MESSAGE(empty.back(), HasSubstr("!empty()"));
}

// _____________________________________________________________________________
// Randomized comparison against a simple reference implementation, across
// many rounds of interleaved batch inserts (with key overrides), single
// erases, and batch erases.
TEST(SortedSequencePlusTest, randomizedAgainstReference) {
  std::mt19937 gen{42};
  std::uniform_int_distribution<int> keyDist{0, 200};
  SV s;
  std::map<int, int> reference;
  int value = 0;
  for (int round = 0; round < 50; ++round) {
    // Insert a batch of random keys.
    int batchSize = keyDist(gen) % 30 + 1;
    for (int i = 0; i < batchSize; ++i) {
      int key = keyDist(gen);
      s.insert({key, ++value});
      reference[key] = value;
    }
    s.consolidate();
    // Erase a few random keys.
    for (int i = 0; i < 5; ++i) {
      int key = keyDist(gen);
      s.erase({key, 0});
      reference.erase(key);
    }
    // Batch-erase a random range of keys.
    if (round % 7 == 0) {
      std::vector<Pair> toDelete;
      int lo = keyDist(gen);
      for (int key = lo; key < lo + 20; ++key) {
        toDelete.push_back({key, 0});
        reference.erase(key);
      }
      s.eraseSorted(ql::span(toDelete));
    }
    // Compare.
    std::vector<Pair> expected(reference.begin(), reference.end());
    ASSERT_THAT(elementsOf(s), ElementsAreArray(expected)) << "round " << round;
    ASSERT_EQ(s.sizeForTesting(), expected.size()) << "round " << round;
  }
}

}  // namespace ad_utility
