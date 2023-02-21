//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./util/GTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "global/Id.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/JoinAlgorithms.h"

using namespace ad_utility;
using ad_utility::source_location;

namespace {
const Id U = Id::makeUndefined();
auto V = ad_utility::testing::VocabId;
template <size_t I>
using Arr = std::array<Id, I>;
}  // namespace

template <size_t I>
void testSmallerUndefRangesForRowsWithoutUndef(
    std::array<Id, I> row, const std::vector<std::array<Id, I>>& range,
    const std::vector<size_t>& positions,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  ASSERT_TRUE(std::ranges::is_sorted(range));
  std::vector<int64_t> foundPositions;
  for (auto it : findSmallerUndefRangesForRowsWithoutUndef<std::array<Id, I>>(
           row, range.begin(), range.end())) {
    foundPositions.push_back(it - range.begin());
  }
  EXPECT_THAT(foundPositions, ::testing::ElementsAreArray(positions));
}

TEST(JoinAlgorithms, findSmallerUndefRangesForRowsWithoutUndef) {
  std::vector<Arr<1>> oneCol{{U}, {U}, {V(3)}, {V(7)}, {V(8)}};
  testSmallerUndefRangesForRowsWithoutUndef<1>({V(3)}, oneCol, {0, 1});

  // (3, 19) is compatible to (U, U), (U, 19), and (3, U);
  std::vector<Arr<2>> twoCols{{U, U},       {U, V(1)},     {U, V(2)}, {U, V(3)},
                              {U, V(3)},    {U, V(19)},    {V(1), U}, {V(3), U},
                              {V(3), V(3)}, {V(7), V(12)}, {V(8), U}};
  testSmallerUndefRangesForRowsWithoutUndef<2>({V(3), V(19)}, twoCols,
                                               {0, 5, 7});

  // (3, 19, 2) is compatible to (U, U, U)[0], (U, 19, U)[4], (U, 19, 2)[5], and
  // (3, 19, U)[9] Note: it is NOT compatible to (3, 19, 2)[10] because we only
  // look for elements that are smaller than (3, 19, 2) AND contain at least one
  // undefined value.
  std::vector<Arr<3>> threeCols{
      {U, U, U},        {U, U, V(0)},     {U, U, V(1)},        {U, V(1), U},
      {U, V(19), U},    {U, V(19), V(2)}, {U, V(19), V(18)},   {V(0), U, U},
      {V(3), V(18), U}, {V(3), V(19), U}, {V(3), V(19), V(2)}, {V(4), U, U},
      {V(5), V(3), U},  {V(7), V(12), U}, {V(8), U, U}};
  testSmallerUndefRangesForRowsWithoutUndef<3>({V(3), V(19), V(2)}, threeCols,
                                               {0, 4, 5, 9});
}

template <size_t I>
void testSmallerUndefRangesForRowsWithUndefInLastColumns(
    Arr<I> row, const std::vector<Arr<I>>& range, size_t numLastUndef,
    const std::vector<size_t>& positions,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  ASSERT_TRUE(std::ranges::is_sorted(range));
  std::vector<int64_t> foundPositions;
  for (auto it : findSmallerUndefRangesForRowsWithUndefInLastColumns(
           row, numLastUndef, range.begin(), range.end())) {
    foundPositions.push_back(it - range.begin());
  }
  EXPECT_THAT(foundPositions, ::testing::ElementsAreArray(positions));
}

TEST(JoinAlgorithms, findSmallerUndefRangesForRowsWithUndefInLastColumns) {
  /*
  std::vector<Arr<1>> oneCol{{U}, {U}, {V(3)}, {V(7)}, {V(8)}};
  // There can be no smaller row than one that is completely undefined, so the
  // result is empty.
  testSmallerUndefRangesForRowsWithUndefInLastColumns<1>({U}, oneCol, 1, {});

  // (x, U) with x != U is compatible to (U, U), all other compatible entries
  are greater or equal to (x, U) ; std::vector<Arr<2>> twoCols{{U, U},       {U,
  V(1)},     {U, V(2)}, {U, V(3)}, {U, V(3)},    {U, V(19)},    {V(1), U},
  {V(3), U}, {V(3), V(3)}, {V(7), V(12)}, {V(8), U}};
  testSmallerUndefRangesForRowsWithUndefInLastColumns<2>({V(3), U}, twoCols, 1,
                                               {0});
  testSmallerUndefRangesForRowsWithUndefInLastColumns<2>({V(128), U}, twoCols,
  1, {0}); testSmallerUndefRangesForRowsWithUndefInLastColumns<2>({U, U},
  twoCols, 2,
                                                         {});
                                                         */

  // (3, 19, U) is compatible to (U, U, X)[0-2],  (U, 19, X)[4-6],  and, (3, U,
  // X)[8-9] Note: it is NOT compatible to (3, 19, U) because we only look for
  // elements that are smaller than (3, 19, U) AND contain at least one
  // undefined value.
  std::vector<Arr<3>> threeCols{
      {U, U, U},           {U, U, V(0)},     {U, U, V(1)},
      {U, V(1), U},        {U, V(19), U},    {U, V(19), V(2)},
      {U, V(19), V(18)},   {V(0), U, U},     {V(3), U, U},
      {V(3), U, V(123)},   {V(3), V(18), U}, {V(3), V(19), U},
      {V(3), V(19), V(2)}, {V(4), U, U},     {V(5), V(3), U},
      {V(7), V(12), U},    {V(8), U, U}};
  testSmallerUndefRangesForRowsWithUndefInLastColumns<3>({U, U, U}, threeCols,
                                                         3, {});
  testSmallerUndefRangesForRowsWithUndefInLastColumns<3>(
      {V(3), V(19), U}, threeCols, 1, {0, 1, 2, 4, 5, 6, 8, 9});

  // TODO<joka921> Further testing and also comment this function.
}
