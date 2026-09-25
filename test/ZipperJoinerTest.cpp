// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <utility>
#include <vector>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/idTable/IdColumnZipperJoin.h"
#include "global/Id.h"

using namespace ad_utility::testing;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

namespace {

IdTable makeEmptyTable() {
  return IdTable{1, ad_utility::testing::makeAllocator()};
}

// Sorts `table` by `col` using the generic `Id::operator<`, which is the
// same total order (datatype-major, `LocalVocabIndex` handled semantically)
// that `zipperJoinIdColumns` requires of its inputs.
IdTable makeSortedTable(const VectorTable& rows, size_t col = 0) {
  IdTable table = makeIdTableFromVector(rows);
  ql::ranges::sort(table, [col](const auto& r1, const auto& r2) {
    return r1[col] < r2[col];
  });
  return table;
}

size_t indexOf(const IdTable& table, size_t col, Id value) {
  for (size_t i = 0; i < table.numRows(); ++i) {
    if (table(i, col) == value) {
      return i;
    }
  }
  ADD_FAILURE() << "Value not found in table";
  return static_cast<size_t>(-1);
}

// Collects the callback invocations of `zipperJoinIdColumns` for inspection.
struct JoinRecorder {
  std::vector<std::pair<size_t, size_t>> matches;
  std::vector<size_t> notFound;
  size_t cancelChecks = 0;

  void run(const IdTable& left, const IdTable& right, size_t col) {
    auto leftCol = std::as_const(left).getColumn(col);
    auto rightCol = std::as_const(right).getColumn(col);
    columnBasedIdTable::zipperJoinIdColumns(
        leftCol, rightCol,
        [this](size_t l, size_t r) { matches.emplace_back(l, r); },
        [this](size_t lBegin, size_t lEnd, size_t rBegin, size_t rEnd) {
          for (size_t l = lBegin; l < lEnd; ++l) {
            for (size_t r = rBegin; r < rEnd; ++r) {
              matches.emplace_back(l, r);
            }
          }
        },
        [this](size_t l) { notFound.push_back(l); },
        [this]() { ++cancelChecks; });
  }
};

}  // namespace

// _____________________________________________________________________________
TEST(ZipperJoiner, emptyBothSides) {
  JoinRecorder rec;
  rec.run(makeEmptyTable(), makeEmptyTable(), 0);
  EXPECT_THAT(rec.matches, IsEmpty());
  EXPECT_THAT(rec.notFound, IsEmpty());
}

TEST(ZipperJoiner, emptyLeftNonEmptyRight) {
  JoinRecorder rec;
  rec.run(makeEmptyTable(), makeSortedTable({{IntId(1)}, {IntId(2)}}), 0);
  EXPECT_THAT(rec.matches, IsEmpty());
  EXPECT_THAT(rec.notFound, IsEmpty());
}

TEST(ZipperJoiner, nonEmptyLeftEmptyRight) {
  JoinRecorder rec;
  rec.run(makeSortedTable({{IntId(1)}, {IntId(2)}, {IntId(3)}}),
         makeEmptyTable(), 0);
  EXPECT_THAT(rec.matches, IsEmpty());
  EXPECT_THAT(rec.notFound, ElementsAre(0u, 1u, 2u));
}

TEST(ZipperJoiner, noOverlapSameDatatype) {
  IdTable left = makeSortedTable({{IntId(1)}, {IntId(3)}, {IntId(5)}});
  IdTable right = makeSortedTable({{IntId(2)}, {IntId(4)}, {IntId(6)}});
  JoinRecorder rec;
  rec.run(left, right, 0);
  EXPECT_THAT(rec.matches, IsEmpty());
  EXPECT_THAT(rec.notFound, ElementsAre(0u, 1u, 2u));
}

TEST(ZipperJoiner, simpleOneToOneMatches) {
  IdTable left = makeSortedTable({{IntId(1)}, {IntId(3)}, {IntId(5)}});
  IdTable right = makeSortedTable({{IntId(1)}, {IntId(4)}, {IntId(5)}});
  JoinRecorder rec;
  rec.run(left, right, 0);
  EXPECT_THAT(rec.matches, UnorderedElementsAre(Pair(0u, 0u), Pair(2u, 2u)));
  EXPECT_THAT(rec.notFound, ElementsAre(1u));
}

TEST(ZipperJoiner, groupCartesianProduct) {
  IdTable left = makeSortedTable({{IntId(2)}, {IntId(2)}, {IntId(3)}});
  IdTable right = makeSortedTable({{IntId(2)}, {IntId(2)}, {IntId(2)}});
  JoinRecorder rec;
  rec.run(left, right, 0);
  EXPECT_THAT(rec.matches,
              UnorderedElementsAre(Pair(0u, 0u), Pair(0u, 1u), Pair(0u, 2u),
                                   Pair(1u, 0u), Pair(1u, 1u), Pair(1u, 2u)));
  EXPECT_THAT(rec.notFound, ElementsAre(2u));
}

// Multiple runs of different datatypes on each side, with a value present
// only on the left, one only on the right, and a datatype run (right's
// `VocabId(2)`) with no counterpart at all.
TEST(ZipperJoiner, multipleDatatypeRuns) {
  IdTable left = makeSortedTable(
      {{IntId(1)}, {IntId(2)}, {VocabId(1)}, {VocabId(3)}});
  IdTable right =
      makeSortedTable({{IntId(2)}, {VocabId(1)}, {VocabId(2)}});
  JoinRecorder rec;
  rec.run(left, right, 0);

  size_t lInt2 = indexOf(left, 0, IntId(2));
  size_t rInt2 = indexOf(right, 0, IntId(2));
  size_t lVocab1 = indexOf(left, 0, VocabId(1));
  size_t rVocab1 = indexOf(right, 0, VocabId(1));
  size_t lInt1 = indexOf(left, 0, IntId(1));
  size_t lVocab3 = indexOf(left, 0, VocabId(3));

  EXPECT_THAT(rec.matches, UnorderedElementsAre(Pair(lInt2, rInt2),
                                                Pair(lVocab1, rVocab1)));
  EXPECT_THAT(rec.notFound, UnorderedElementsAre(lInt1, lVocab3));
}

// `LocalVocabId` interns its argument in a shared `LocalVocab` (see
// `IdTestHelpers.h`), so two calls with the same value refer to the same
// entry and compare equal, even though `LocalVocabIndex` doesn't compare
// bitwise and is handled by the semantic phase.
TEST(ZipperJoiner, localVocabIndexMatches) {
  IdTable left = makeSortedTable({{LocalVocabId(1)}, {LocalVocabId(2)}});
  IdTable right = makeSortedTable({{LocalVocabId(2)}, {LocalVocabId(3)}});
  JoinRecorder rec;
  rec.run(left, right, 0);

  size_t lV2 = indexOf(left, 0, LocalVocabId(2));
  size_t rV2 = indexOf(right, 0, LocalVocabId(2));
  size_t lV1 = indexOf(left, 0, LocalVocabId(1));

  EXPECT_THAT(rec.matches, ElementsAre(Pair(lV2, rV2)));
  EXPECT_THAT(rec.notFound, ElementsAre(lV1));
}

// A `LocalVocabIndex` value sits between `IntId` values in sort order, so
// this exercises both the bitwise phase (for the `IntId`s) and the
// transition into and out of the semantic phase (for the `LocalVocabIndex`).
TEST(ZipperJoiner, localVocabMixedWithOtherDatatypes) {
  IdTable left =
      makeSortedTable({{IntId(1)}, {LocalVocabId(5)}, {IntId(100)}});
  IdTable right =
      makeSortedTable({{IntId(1)}, {LocalVocabId(5)}, {IntId(200)}});
  JoinRecorder rec;
  rec.run(left, right, 0);

  size_t lInt1 = indexOf(left, 0, IntId(1));
  size_t rInt1 = indexOf(right, 0, IntId(1));
  size_t lLV5 = indexOf(left, 0, LocalVocabId(5));
  size_t rLV5 = indexOf(right, 0, LocalVocabId(5));
  size_t lInt100 = indexOf(left, 0, IntId(100));

  EXPECT_THAT(rec.matches,
              UnorderedElementsAre(Pair(lInt1, rInt1), Pair(lLV5, rLV5)));
  EXPECT_THAT(rec.notFound, ElementsAre(lInt100));
}

// The semantic phase (only reached when a `LocalVocabIndex` is involved)
// checks for cancellation on every iteration; a purely bitwise join is too
// small in a unit test to ever hit the batched check in the bitwise phase.
TEST(ZipperJoiner, cancelCallbackInvokedForLocalVocab) {
  IdTable left = makeSortedTable({{LocalVocabId(1)}});
  IdTable right = makeSortedTable({{LocalVocabId(1)}});
  JoinRecorder rec;
  rec.run(left, right, 0);
  EXPECT_GT(rec.cancelChecks, 0u);
}
