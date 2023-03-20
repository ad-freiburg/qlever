//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./util/AllocatorTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
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

std::vector<int64_t> toPositions(auto generator, const auto& range) {
  std::vector<int64_t> foundPositions;
  for (auto it : generator) {
    foundPositions.push_back(it - range.begin());
  }
  return foundPositions;
}

template <size_t I>
void testSmallerUndefRangesForArbitraryRows(
    std::array<Id, I> row, const std::vector<std::array<Id, I>>& range,
    const std::vector<size_t>& positions,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  EXPECT_THAT(toPositions(findSmallerUndefRangesArbitrary<std::array<Id, I>>(
                              row, range.begin(), range.end()),
                          range),
              ::testing::ElementsAreArray(positions));
  EXPECT_THAT(toPositions(findSmallerUndefRanges<std::array<Id, I>>(
                              row, range.begin(), range.end()),
                          range),
              ::testing::ElementsAreArray(positions));
}

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

  // Every input can also be tested against the function for arbitrary inputs
  testSmallerUndefRangesForArbitraryRows(row, range, positions);
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

  // Every input can also be tested against the function for arbitrary inputs
  testSmallerUndefRangesForArbitraryRows(row, range, positions);
}

TEST(JoinAlgorithms, findSmallerUndefRangesForRowsWithUndefInLastColumns) {
  std::vector<Arr<1>> oneCol{{U}, {U}, {V(3)}, {V(7)}, {V(8)}};
  // There can be no smaller row than one that is completely undefined, so the
  // result is empty.
  testSmallerUndefRangesForRowsWithUndefInLastColumns<1>({U}, oneCol, 1, {});

  // (U, x) is compatible to (3, U), all other compatible entries
  // are greater or equal to (3, U)
  std::vector<Arr<2>> twoCols{{U, U},       {U, V(1)},     {U, V(2)}, {U, V(3)},
                              {U, V(3)},    {U, V(19)},    {V(1), U}, {V(3), U},
                              {V(3), V(3)}, {V(7), V(12)}, {V(8), U}};
  testSmallerUndefRangesForRowsWithUndefInLastColumns<2>({V(3), U}, twoCols, 1,
                                                         {0, 1, 2, 3, 4, 5});
  // The behavior of (128, U) is exactly the same as of (3, U)
  testSmallerUndefRangesForRowsWithUndefInLastColumns<2>({V(128), U}, twoCols,
                                                         1, {0, 1, 2, 3, 4, 5});
  // Again, no row can be smaller than (U, U)
  testSmallerUndefRangesForRowsWithUndefInLastColumns<2>({U, U}, twoCols, 2,
                                                         {});

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

  // (8, U, U) is compatible to (U, X, X) (entries 0-6)
  testSmallerUndefRangesForRowsWithUndefInLastColumns<3>(
      {V(8), U, U}, threeCols, 2, {0, 1, 2, 3, 4, 5, 6});
}

// This test only tests input rows that don't match the two above cases and have
// UNDEF values not only in the last columns. All other inputs have already been
// tested against the general `findSmallerUndefRangesArbitrary` function as part
// of the above unit tests.
TEST(JoinAlgorithms, findSmallerUndefRangesArbitrary) {
  // No test for width 1, as all rows always fall into one of the above cases.

  // Only `(U, U)` is compatible to and smaller than `(U, 3)`
  std::vector<Arr<2>> twoCols{{U, U},       {U, V(1)},     {U, V(2)}, {U, V(3)},
                              {U, V(3)},    {U, V(19)},    {V(1), U}, {V(3), U},
                              {V(3), V(3)}, {V(7), V(12)}, {V(8), U}};
  testSmallerUndefRangesForArbitraryRows<2>({U, V(3)}, twoCols, {0});
  // The behavior of (U, 128) is exactly the same as of (U, 3)
  testSmallerUndefRangesForArbitraryRows<2>({U, V(128)}, twoCols, {0});
  // Again, no row can be smaller than (U, U)
  testSmallerUndefRangesForArbitraryRows<2>({U, U}, twoCols, {});

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
  testSmallerUndefRangesForArbitraryRows<3>({U, U, U}, threeCols, {});
  // (3, U, 2) is compatible to (U, U, U), (U, 1, U), (U, 19, U), (U, 19, 2),
  // (3, U, U)
  testSmallerUndefRangesForArbitraryRows<3>({V(3), U, V(2)}, threeCols,
                                            {0, 3, 4, 5, 8});

  // (U, 1, 1) is compatible to (U, U, U), (U, U, 1), (U, 1, U)
  testSmallerUndefRangesForArbitraryRows<3>({U, V(1), V(1)}, threeCols,
                                            {0, 2, 3});

  // TODO<joka921> Can we implement an optimized algorithm when the last
  // column(s) are undefined and there still are other undefined values? (U, 19,
  // U) is compatible to and greater than (U, U, X)
  testSmallerUndefRangesForArbitraryRows<3>({U, V(19), U}, threeCols,
                                            {
                                                0,
                                                1,
                                                2,
                                            });
}

/*
TEST(JoinAlgorithms, AddCombinedRowToIdTable) {
  AddCombinedRowToIdTable adder{5};
  IdTable table(ad_utility::testing::makeAllocator());
  table.setNumColumns(5);
  adder(std::array{V(1), U, U}, std::array{V(1), V(3), V(28), U}, 2, &table);
  adder(std::array{U, V(3), V(8)}, std::array{V(7), U, V(28), V(3)}, 2, &table);
  adder(std::array{U, V(3), V(8)}, std::array{U, V(3), V(28), U}, 2, &table);
  adder(std::array{V(12), V(3), V(8)}, std::array{V(12), V(3), U, U}, 2,
        &table);

  EXPECT_EQ(table, makeIdTableFromIdVector({{V(1), V(3), U, V(28), U},
                                            {V(7), V(3), V(8), V(28), V(3)},
                                            {U, V(3), V(8), V(28), U},
                                            {V(12), V(3), V(8), U, U}}));
  EXPECT_THAT(adder.numUndefinedPerColumn(),
              ::testing::ElementsAre(1, 0, 1, 1, 3));
}
 */
