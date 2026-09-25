// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "global/Id.h"
#include "index/IdColumnSort.h"
#include "util/Exception.h"

using namespace ad_utility::testing;
using columnBasedIdTable::SingleKeySorter;

namespace {

std::vector<Id> getRowAsVector(const IdTable& table, size_t row) {
  std::vector<Id> result;
  for (size_t c = 0; c < table.numColumns(); ++c) {
    result.push_back(table(row, c));
  }
  return result;
}

// Checks that `sorted` is `original` with its rows permuted such that
// `keyCol` is non-decreasing, and that no row was corrupted in the process
// (every column, not just the key column, must survive the permutation).
void expectSortedAndPermutationCorrect(const IdTable& original,
                                       const IdTable& sorted, size_t keyCol) {
  ASSERT_EQ(original.numRows(), sorted.numRows());
  ASSERT_EQ(original.numColumns(), sorted.numColumns());
  for (size_t i = 1; i < sorted.numRows(); ++i) {
    EXPECT_FALSE(sorted(i, keyCol) < sorted(i - 1, keyCol))
        << "Row " << i << " is out of order";
  }
  auto toSortedRows = [](const IdTable& t) {
    std::vector<std::vector<Id>> rows;
    for (size_t r = 0; r < t.numRows(); ++r) {
      rows.push_back(getRowAsVector(t, r));
    }
    ql::ranges::sort(rows);
    return rows;
  };
  EXPECT_EQ(toSortedRows(original), toSortedRows(sorted));
}

}  // namespace

// _____________________________________________________________________________
TEST(IdColumnSort, isSortableTrueForSingleDatatype) {
  IdTable table = makeIdTableFromVector({{IntId(1)}, {IntId(3)}, {IntId(2)}});
  EXPECT_TRUE(SingleKeySorter::isSortable(table, 0));
}

TEST(IdColumnSort, isSortableTrueForMixedDatatypesWithoutLocalVocab) {
  IdTable table = makeIdTableFromVector(
      {{IntId(3)}, {VocabId(1)}, {UndefId()}, {BoolId(true)}});
  EXPECT_TRUE(SingleKeySorter::isSortable(table, 0));
}

TEST(IdColumnSort, isSortableFalseForLocalVocab) {
  IdTable table = makeIdTableFromVector({{IntId(1)}, {LocalVocabId(1)}});
  EXPECT_FALSE(SingleKeySorter::isSortable(table, 0));
}

TEST(IdColumnSort, isSortableTrueForEmptyTable) {
  IdTable table{1, ad_utility::testing::makeAllocator()};
  EXPECT_TRUE(SingleKeySorter::isSortable(table, 0));
}

TEST(IdColumnSort, sortThrowsIfKeyColumnIsNotSortable) {
  IdTable table = makeIdTableFromVector({{IntId(1)}, {LocalVocabId(1)}});
  SingleKeySorter sorter{table, 1};
  EXPECT_THROW(sorter.sort(0), ad_utility::Exception);
}

TEST(IdColumnSort, emptyTableSortIsNoop) {
  IdTable table{2, ad_utility::testing::makeAllocator()};
  SingleKeySorter{table, 1}.sort(0);
  EXPECT_EQ(table.numRows(), 0u);
}

TEST(IdColumnSort, singleRowTable) {
  IdTable table = makeIdTableFromVector({{IntId(42), IntId(7)}});
  SingleKeySorter{table, 1}.sort(0);
  EXPECT_EQ(table(0, 0), IntId(42));
  EXPECT_EQ(table(0, 1), IntId(7));
}

// Every row shares one datatype: exercises `partitionByDatatype`'s
// `singleDatatype_` fast path (and `sortPartitions`'s corresponding
// single-partition sort), including a duplicate key.
TEST(IdColumnSort, singleDatatypeFastPath) {
  IdTable table = makeIdTableFromVector({{IntId(5), IntId(50)},
                                         {IntId(3), IntId(30)},
                                         {IntId(4), IntId(40)},
                                         {IntId(1), IntId(10)},
                                         {IntId(2), IntId(20)},
                                         {IntId(3), IntId(31)}});
  IdTable original = table.clone();
  SingleKeySorter{table, 1}.sort(0);
  expectSortedAndPermutationCorrect(original, table, 0);
}

// Multiple datatypes in the key column: exercises the general
// bucket-offset path in `partitionByDatatype`/`sortPartitions`.
TEST(IdColumnSort, multiDatatypeGeneralPath) {
  IdTable table = makeIdTableFromVector({{UndefId(), IntId(1)},
                                         {IntId(3), IntId(2)},
                                         {VocabId(2), IntId(3)},
                                         {IntId(1), IntId(4)},
                                         {BoolId(true), IntId(5)},
                                         {VocabId(1), IntId(6)},
                                         {BoolId(false), IntId(7)}});
  IdTable original = table.clone();
  SingleKeySorter{table, 1}.sort(0);
  expectSortedAndPermutationCorrect(original, table, 0);
}

// Degenerate case: every row has the same key, so the whole permutation
// lands in one datatype/payload bucket.
TEST(IdColumnSort, allValuesIdentical) {
  IdTable table = makeIdTableFromVector(
      {{IntId(7), IntId(1)}, {IntId(7), IntId(2)}, {IntId(7), IntId(3)}});
  IdTable original = table.clone();
  SingleKeySorter{table, 2}.sort(0);
  expectSortedAndPermutationCorrect(original, table, 0);
}

// Many columns and `numThreads > 1`, so `applyPermutation` splits the
// columns across several worker threads.
TEST(IdColumnSort, wideTableMultiThreaded) {
  VectorTable rows;
  for (int i = 20; i >= 0; --i) {
    rows.push_back({IntId(i), IntId(i * 2), IntId(i * 3), IntId(i * 4),
                    IntId(i * 5), IntId(i * 6)});
  }
  IdTable table = makeIdTableFromVector(rows);
  IdTable original = table.clone();
  SingleKeySorter{table, 4}.sort(0);
  expectSortedAndPermutationCorrect(original, table, 0);
}

// `numColumns == 1` forces `numWorkers` down to 1 in `applyPermutation`
// even though several threads were requested.
TEST(IdColumnSort, singleColumnTableWithThreadsRequested) {
  IdTable table = makeIdTableFromVector({{IntId(3)}, {IntId(1)}, {IntId(2)}});
  IdTable original = table.clone();
  SingleKeySorter{table, 8}.sort(0);
  expectSortedAndPermutationCorrect(original, table, 0);
}

// With unique keys the correct output is uniquely determined, so the
// sequential (`numThreads == 1`) and threaded (`numThreads > 1`) paths
// through `applyPermutation` must produce byte-identical tables.
TEST(IdColumnSort, singleThreadedMatchesMultiThreaded) {
  VectorTable rows;
  for (int i = 50; i >= 0; --i) {
    rows.push_back({IntId(i), IntId(i * 2), IntId(i * 3)});
  }
  IdTable tableSeq = makeIdTableFromVector(rows);
  IdTable tablePar = tableSeq.clone();

  SingleKeySorter{tableSeq, 1}.sort(0);
  SingleKeySorter{tablePar, 8}.sort(0);

  for (size_t r = 0; r < tableSeq.numRows(); ++r) {
    for (size_t c = 0; c < tableSeq.numColumns(); ++c) {
      EXPECT_EQ(tableSeq(r, c), tablePar(r, c))
          << "row " << r << " col " << c;
    }
  }
}
