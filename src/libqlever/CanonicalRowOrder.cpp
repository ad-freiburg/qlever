// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/CanonicalRowOrder.h"

#include <numeric>

#include "backports/algorithm.h"
#include "util/Exception.h"

namespace qlever {

namespace {
// Return the columns of a table with `numColumns` columns in the order in which
// they are compared in the canonical order: first the `resultSortedOn`
// columns, then all other columns in increasing order.
std::vector<ColumnIndex> comparisonColumns(
    size_t numColumns, ql::span<const ColumnIndex> resultSortedOn) {
  std::vector<ColumnIndex> result{resultSortedOn.begin(), resultSortedOn.end()};
  std::vector<bool> isSortedOn(numColumns, false);
  for (ColumnIndex column : resultSortedOn) {
    AD_CONTRACT_CHECK(column < numColumns);
    isSortedOn[column] = true;
  }
  for (ColumnIndex column = 0; column < numColumns; ++column) {
    if (!isSortedOn[column]) {
      result.push_back(column);
    }
  }
  return result;
}

// Compare the row `rowA` of `tableA` with the row `rowB` of `tableB` by the
// given `columns` and return a negative number, zero, or a positive number if
// the first row is less than, equal to, or greater than the second one.
int compareRows(const IdTableView<0>& tableA, size_t rowA,
                const IdTableView<0>& tableB, size_t rowB,
                const std::vector<ColumnIndex>& columns) {
  for (ColumnIndex column : columns) {
    auto comparison =
        tableA(rowA, column).compareThreeWay(tableB(rowB, column));
    if (comparison < 0) {
      return -1;
    }
    if (comparison > 0) {
      return 1;
    }
  }
  return 0;
}
}  // namespace

// _____________________________________________________________________________
std::vector<size_t> canonicalSortingPermutation(
    const IdTableView<0>& table, ql::span<const ColumnIndex> resultSortedOn) {
  auto columns = comparisonColumns(table.numColumns(), resultSortedOn);
  auto less = [&table, &columns](size_t a, size_t b) {
    return compareRows(table, a, table, b, columns) < 0;
  };
  std::vector<size_t> permutation(table.numRows());
  std::iota(permutation.begin(), permutation.end(), size_t{0});
  // The check is cheaper than sorting, and the tables that we write are
  // usually in canonical order already.
  if (!ql::ranges::is_sorted(permutation, less)) {
    ql::ranges::stable_sort(permutation, less);
  }
  return permutation;
}

// _____________________________________________________________________________
bool isInCanonicalOrder(const IdTableView<0>& table,
                        ql::span<const ColumnIndex> resultSortedOn) {
  auto columns = comparisonColumns(table.numColumns(), resultSortedOn);
  for (size_t row = 1; row < table.numRows(); ++row) {
    if (compareRows(table, row - 1, table, row, columns) > 0) {
      return false;
    }
  }
  return true;
}

// _____________________________________________________________________________
IdTable permuteRows(const IdTableView<0>& table,
                    ql::span<const size_t> oldRowOfNewRow,
                    const ad_utility::AllocatorWithLimit<Id>& allocator) {
  IdTable result{table.numColumns(), allocator};
  result.resize(oldRowOfNewRow.size());
  for (size_t column = 0; column < table.numColumns(); ++column) {
    auto source = table.getColumn(column);
    auto target = result.getColumn(column);
    for (size_t row = 0; row < oldRowOfNewRow.size(); ++row) {
      target[row] = source[oldRowOfNewRow[row]];
    }
  }
  return result;
}

// _____________________________________________________________________________
std::vector<size_t> invertPermutation(ql::span<const size_t> permutation) {
  std::vector<size_t> result(permutation.size(), noMatchingRow);
  for (size_t i = 0; i < permutation.size(); ++i) {
    AD_CONTRACT_CHECK(permutation[i] < permutation.size() &&
                      result[permutation[i]] == noMatchingRow);
    result[permutation[i]] = i;
  }
  return result;
}

// _____________________________________________________________________________
std::vector<size_t> alignRows(const IdTableView<0>& base,
                              const IdTableView<0>& target,
                              ql::span<const ColumnIndex> resultSortedOn) {
  AD_CONTRACT_CHECK(base.numColumns() == target.numColumns());
  auto columns = comparisonColumns(base.numColumns(), resultSortedOn);
  std::vector<size_t> result(target.numRows(), noMatchingRow);
  size_t baseRow = 0;
  size_t targetRow = 0;
  while (baseRow < base.numRows() && targetRow < target.numRows()) {
    int comparison = compareRows(base, baseRow, target, targetRow, columns);
    if (comparison == 0) {
      result[targetRow++] = baseRow++;
    } else if (comparison < 0) {
      ++baseRow;
    } else {
      ++targetRow;
    }
  }
  return result;
}

}  // namespace qlever
