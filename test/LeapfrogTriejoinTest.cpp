// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <tuple>
#include <utility>
#include <vector>

#include "engine/LeapfrogTriejoin.h"
#include "global/Id.h"

using namespace ql::engine::wcoj;

TEST(LeapfrogTriejoinTest, ThreeWayIntersection) {
  std::vector<Id> listA = {Id::makeFromInt(1), Id::makeFromInt(3),
                           Id::makeFromInt(5), Id::makeFromInt(7),
                           Id::makeFromInt(9)};
  std::vector<Id> listB = {Id::makeFromInt(2), Id::makeFromInt(3),
                           Id::makeFromInt(6), Id::makeFromInt(7),
                           Id::makeFromInt(10)};
  std::vector<Id> listC = {Id::makeFromInt(3), Id::makeFromInt(4),
                           Id::makeFromInt(7), Id::makeFromInt(8),
                           Id::makeFromInt(11)};

  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);
  iterators.emplace_back(listC);

  auto matches = LeapfrogJoin::intersect(iterators);

  // Common elements across all three lists: 3 and 7
  ASSERT_EQ(matches.size(), 2u);
  EXPECT_EQ(matches[0], Id::makeFromInt(3));
  EXPECT_EQ(matches[1], Id::makeFromInt(7));
}

TEST(LeapfrogTriejoinTest, FirstIteratorStartsLargestNoFalsePositive) {
  // Iterator 0 starts with the largest key and shares nothing with the
  // others; the old round-robin `p` handling wrongly emitted 10 here.
  std::vector<Id> listA = {Id::makeFromInt(10), Id::makeFromInt(11)};
  std::vector<Id> listB = {Id::makeFromInt(1), Id::makeFromInt(2)};
  std::vector<Id> listC = {Id::makeFromInt(3), Id::makeFromInt(4)};

  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);
  iterators.emplace_back(listC);

  EXPECT_TRUE(LeapfrogJoin::intersect(iterators).empty());
}

TEST(LeapfrogTriejoinTest, FirstIteratorStartsLargestWithCommonKey) {
  // Iterator 0 starts largest, but 20 is common to all three lists.
  std::vector<Id> listA = {Id::makeFromInt(10), Id::makeFromInt(20),
                           Id::makeFromInt(30)};
  std::vector<Id> listB = {Id::makeFromInt(5), Id::makeFromInt(20),
                           Id::makeFromInt(25)};
  std::vector<Id> listC = {Id::makeFromInt(7), Id::makeFromInt(20),
                           Id::makeFromInt(40)};

  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);
  iterators.emplace_back(listC);

  auto matches = LeapfrogJoin::intersect(iterators);
  ASSERT_EQ(matches.size(), 1u);
  EXPECT_EQ(matches[0], Id::makeFromInt(20));
}

TEST(LeapfrogTriejoinTest, DisjointListsEmptyResult) {
  std::vector<Id> listA = {Id::makeFromInt(1), Id::makeFromInt(2)};
  std::vector<Id> listB = {Id::makeFromInt(3), Id::makeFromInt(4)};

  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);

  auto matches = LeapfrogJoin::intersect(iterators);
  EXPECT_TRUE(matches.empty());
}

namespace {
// Collect the result of `LeapfrogJoin::forEachCommonKey` as
// `(key, run of the first iterator, run of the second iterator)`.
using Match =
    std::tuple<int64_t, std::pair<size_t, size_t>, std::pair<size_t, size_t>>;
std::vector<Match> commonKeys(const std::vector<Id>& a,
                              const std::vector<Id>& b) {
  std::array iterators{LeapfrogIterator{a}, LeapfrogIterator{b}};
  std::vector<Match> result;
  LeapfrogJoin::forEachCommonKey(
      iterators, [&result](Id key, const std::array<KeyRun, 2>& runs) {
        result.emplace_back(key.getInt(),
                            std::pair{runs[0].begin_, runs[0].end_},
                            std::pair{runs[1].begin_, runs[1].end_});
      });
  return result;
}

std::vector<Id> ints(const std::vector<int64_t>& values) {
  std::vector<Id> result;
  for (auto value : values) {
    result.push_back(Id::makeFromInt(value));
  }
  return result;
}
}  // namespace

// Duplicate keys are reported once, together with their runs in each range.
TEST(LeapfrogTriejoinTest, ForEachCommonKeyReportsRunsOfDuplicates) {
  auto a = ints({1, 3, 3, 5, 7, 7, 7, 9});
  auto b = ints({3, 4, 7, 7, 9, 9});
  EXPECT_THAT(
      commonKeys(a, b),
      ::testing::ElementsAre(Match{3, {1, 3}, {0, 1}}, Match{7, {4, 7}, {2, 4}},
                             Match{9, {7, 8}, {4, 6}}));
}

// Empty and disjoint ranges have no common keys.
TEST(LeapfrogTriejoinTest, ForEachCommonKeyEmptyAndDisjoint) {
  EXPECT_TRUE(commonKeys({}, ints({1, 2})).empty());
  EXPECT_TRUE(commonKeys(ints({1, 2}), {}).empty());
  EXPECT_TRUE(commonKeys(ints({1, 2}), ints({3, 4})).empty());
  EXPECT_TRUE(commonKeys(ints({5, 6}), ints({1, 2})).empty());
}

// Three ranges, the last common key is the last entry of every range.
TEST(LeapfrogTriejoinTest, ForEachCommonKeyThreeRanges) {
  auto a = ints({1, 2, 4, 8});
  auto b = ints({2, 3, 4, 8});
  auto c = ints({0, 4, 4, 8});
  std::array iterators{LeapfrogIterator{a}, LeapfrogIterator{b},
                       LeapfrogIterator{c}};
  std::vector<std::pair<int64_t, size_t>> keysAndCountsInC;
  LeapfrogJoin::forEachCommonKey(
      iterators, [&](Id key, const std::array<KeyRun, 3>& runs) {
        keysAndCountsInC.emplace_back(key.getInt(), runs[2].size());
      });
  using P = std::pair<int64_t, size_t>;
  EXPECT_THAT(keysAndCountsInC, ::testing::ElementsAre(P{4, 2}, P{8, 1}));
}
