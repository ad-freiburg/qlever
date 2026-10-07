// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <vector>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "libqlever/CanonicalRowOrder.h"

using namespace qlever;
using testing::ElementsAre;
using testing::IsEmpty;

namespace {
using Ints = std::vector<size_t>;

// Create a table whose entries are the given integers (as `Id`s).
IdTable intTable(const VectorTable& content, size_t numColumns = 2) {
  auto table = makeIdTableFromVector(content, ad_utility::testing::IntId);
  if (content.empty()) {
    return IdTable{numColumns, ad_utility::testing::makeAllocator()};
  }
  return table;
}

auto view(const IdTable& table) { return table.asStaticView<0>(); }
}  // namespace

// _____________________________________________________________________________
TEST(CanonicalRowOrder, sortWithoutResultSortedOn) {
  auto table = intTable({{2, 1}, {1, 5}, {1, 2}, {2, 0}});
  EXPECT_THAT(canonicalSortingPermutation(view(table), {}),
              ElementsAre(2, 1, 3, 0));
  EXPECT_FALSE(isInCanonicalOrder(view(table), {}));
  auto sorted =
      permuteRows(view(table), canonicalSortingPermutation(view(table), {}),
                  ad_utility::testing::makeAllocator());
  EXPECT_EQ(sorted, intTable({{1, 2}, {1, 5}, {2, 0}, {2, 1}}));
  EXPECT_TRUE(isInCanonicalOrder(view(sorted), {}));
}

// _____________________________________________________________________________
TEST(CanonicalRowOrder, sortWithResultSortedOn) {
  // First by column 1, then by the remaining column 0.
  auto table = intTable({{2, 1}, {1, 5}, {1, 2}, {3, 1}});
  std::vector<ColumnIndex> sortedOn{1};
  EXPECT_THAT(canonicalSortingPermutation(view(table), sortedOn),
              ElementsAre(0, 3, 2, 1));
  auto sorted = permuteRows(view(table),
                            canonicalSortingPermutation(view(table), sortedOn),
                            ad_utility::testing::makeAllocator());
  EXPECT_EQ(sorted, intTable({{2, 1}, {3, 1}, {1, 2}, {1, 5}}));
  EXPECT_TRUE(isInCanonicalOrder(view(sorted), sortedOn));
  // This table is canonical for column 1 first, but not for the default order.
  EXPECT_FALSE(isInCanonicalOrder(view(sorted), {}));

  // Several sorted columns, in the given order.
  auto table3 = intTable({{1, 2, 3}, {1, 1, 4}, {0, 2, 3}, {5, 1, 1}});
  std::vector<ColumnIndex> sortedOn2{2, 0};
  auto sorted3 = permuteRows(
      view(table3), canonicalSortingPermutation(view(table3), sortedOn2),
      ad_utility::testing::makeAllocator());
  EXPECT_EQ(sorted3, intTable({{5, 1, 1}, {0, 2, 3}, {1, 2, 3}, {1, 1, 4}}));
}

// _____________________________________________________________________________
TEST(CanonicalRowOrder, stabilityAndIdentity) {
  // Equal rows are not reordered, so the permutation is stable.
  auto table = intTable({{1, 1}, {0, 0}, {1, 1}, {0, 0}});
  EXPECT_THAT(canonicalSortingPermutation(view(table), {}),
              ElementsAre(1, 3, 0, 2));
  auto sortedTable = intTable({{0, 0}, {0, 0}, {1, 1}, {2, 2}});
  EXPECT_THAT(canonicalSortingPermutation(view(sortedTable), {}),
              ElementsAre(0, 1, 2, 3));
  EXPECT_TRUE(isInCanonicalOrder(view(sortedTable), {}));

  auto empty = intTable({});
  EXPECT_THAT(canonicalSortingPermutation(view(empty), {}), IsEmpty());
  EXPECT_TRUE(isInCanonicalOrder(view(empty), {}));
}

// _____________________________________________________________________________
TEST(CanonicalRowOrder, invertPermutation) {
  EXPECT_THAT(invertPermutation(Ints{2, 0, 1}), ElementsAre(1, 2, 0));
  EXPECT_THAT(invertPermutation(Ints{}), IsEmpty());
  EXPECT_ANY_THROW(invertPermutation(Ints{0, 0}));
  EXPECT_ANY_THROW(invertPermutation(Ints{0, 2}));
}

// _____________________________________________________________________________
TEST(CanonicalRowOrder, permuteRowsSelectsRows) {
  auto table = intTable({{1, 1}, {2, 2}, {3, 3}});
  // The result may have fewer rows than the input.
  EXPECT_EQ(permuteRows(view(table), Ints{2, 0},
                        ad_utility::testing::makeAllocator()),
            intTable({{3, 3}, {1, 1}}));
}

// _____________________________________________________________________________
TEST(CanonicalRowOrder, alignRows) {
  constexpr size_t no = noMatchingRow;
  auto base = intTable({{1, 1}, {2, 2}, {2, 2}, {4, 4}, {6, 6}});
  // Row `{1, 1}` is unchanged, `{2, 2}` has one duplicate fewer, `{3, 3}` and
  // `{5, 5}` are inserted, `{4, 4}` is kept, `{6, 6}` is deleted, `{7, 7}` is
  // inserted.
  auto target = intTable({{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {7, 7}});
  EXPECT_THAT(alignRows(view(base), view(target)),
              ElementsAre(0, 1, no, 3, no, no));

  // More duplicates in the target than in the base: matched pairwise.
  auto targetDuplicates = intTable({{2, 2}, {2, 2}, {2, 2}});
  EXPECT_THAT(alignRows(view(base), view(targetDuplicates)),
              ElementsAre(1, 2, no));

  // Identical tables and all-new rows.
  EXPECT_THAT(alignRows(view(base), view(base)), ElementsAre(0, 1, 2, 3, 4));
  auto allNew = intTable({{0, 0}, {3, 3}, {9, 9}});
  EXPECT_THAT(alignRows(view(base), view(allNew)), ElementsAre(no, no, no));

  // Empty tables.
  auto empty = intTable({});
  EXPECT_THAT(alignRows(view(base), view(empty)), IsEmpty());
  EXPECT_THAT(alignRows(view(empty), view(allNew)), ElementsAre(no, no, no));

  // Different numbers of columns are rejected.
  auto oneColumn = intTable({{1}});
  EXPECT_ANY_THROW(alignRows(view(base), view(oneColumn)));
}

// _____________________________________________________________________________
TEST(CanonicalRowOrder, alignRowsWithResultSortedOn) {
  // Canonical for `resultSortedOn == {1}`: sorted by column 1, then column 0.
  std::vector<ColumnIndex> sortedOn{1};
  auto base = intTable({{5, 1}, {2, 2}, {7, 2}, {1, 3}});
  auto target = intTable({{5, 1}, {3, 2}, {7, 2}, {1, 3}});
  ASSERT_TRUE(isInCanonicalOrder(view(base), sortedOn));
  ASSERT_TRUE(isInCanonicalOrder(view(target), sortedOn));
  EXPECT_THAT(alignRows(view(base), view(target), sortedOn),
              ElementsAre(0, noMatchingRow, 2, 3));
}
