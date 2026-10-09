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
#include <optional>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/Views.h"

namespace qlever::canonicalRowOrder {

namespace {
// Return the columns of a table with `numColumns` columns in the order in which
// they are compared in the canonical order: first the `resultSortedOn`
// columns, then all other columns in increasing order. Return `std::nullopt` if
// `resultSortedOn` contains a column that does not exist. Duplicates in
// `resultSortedOn` are ignored (they do not change the order).
std::optional<std::vector<ColumnIndex>> tryComparisonColumns(
    size_t numColumns, ql::span<const ColumnIndex> resultSortedOn) {
  std::vector<ColumnIndex> result;
  ad_utility::HashSet<ColumnIndex> sortedOn;
  for (ColumnIndex column : resultSortedOn) {
    if (column >= numColumns) {
      return std::nullopt;
    }
    if (sortedOn.insert(column).second) {
      result.push_back(column);
    }
  }
  for (ColumnIndex column : ad_utility::integerRange(numColumns)) {
    if (!sortedOn.contains(column)) {
      result.push_back(column);
    }
  }
  return result;
}

// Like `tryComparisonColumns`, but fail via `AD_CONTRACT_CHECK` for an invalid
// column.
std::vector<ColumnIndex> comparisonColumns(
    size_t numColumns, ql::span<const ColumnIndex> resultSortedOn) {
  auto result = tryComparisonColumns(numColumns, resultSortedOn);
  AD_CONTRACT_CHECK(result.has_value());
  return std::move(result).value();
}

// Return the number of rows of a table that is given by its `columns`, which
// have to have the same number of rows each.
size_t numRowsOf(IdColumns columns) {
  if (columns.empty()) {
    return 0;
  }
  size_t numRows = columns[0].size();
  AD_CONTRACT_CHECK(ql::ranges::all_of(columns, [numRows](const auto& column) {
    return column.size() == numRows;
  }));
  return numRows;
}

// Return the columns of `table`, which are valid as long as `table`.
std::vector<ConstIdColumnRef> columnsOf(const IdTableView<0>& table) {
  return ::ranges::to<std::vector<ConstIdColumnRef>>(table.getColumns());
}

// Compare the two `Id`s via `ValueId::compareThreeWay`, and break ties by
// their raw bits, so that only bitwise identical `Id`s compare equal (the
// canonical order has to be a total order, see `CanonicalRowOrder.h`).
//
// NOTE: `compareThreeWay` compares the raw bits of all `Id`s except those of
// type `LocalVocabIndex`, which it compares by their words. It thus considers
// two `Id`s equal that differ bitwise if both are of type `LocalVocabIndex` and
// refer to different entries with the same word (or if one of them is of type
// `LocalVocabIndex` and its word is the one at the position in the vocabulary
// that the other one refers to).
int compareIds(Id a, Id b) {
  auto comparison = a.compareThreeWay(b);
  if (comparison < 0) {
    return -1;
  }
  if (comparison > 0) {
    return 1;
  }
  if (a.getBits() < b.getBits()) {
    return -1;
  }
  return a.getBits() > b.getBits() ? 1 : 0;
}

// Compare the row `rowA` of the table `a` with the row `rowB` of the table `b`
// by the given `columns` and return a negative number, zero, or a positive
// number if the first row is less than, equal to, or greater than the second
// one.
int compareRows(IdColumns a, size_t rowA, IdColumns b, size_t rowB,
                const std::vector<ColumnIndex>& columns) {
  for (ColumnIndex column : columns) {
    int comparison = compareIds(a[column][rowA], b[column][rowB]);
    if (comparison != 0) {
      return comparison;
    }
  }
  return 0;
}
}  // namespace

// _____________________________________________________________________________
std::vector<size_t> canonicalSortingPermutation(
    const IdTableView<0>& table, ql::span<const ColumnIndex> resultSortedOn) {
  auto columns = comparisonColumns(table.numColumns(), resultSortedOn);
  auto tableColumns = columnsOf(table);
  auto less = [&tableColumns, &columns](size_t a, size_t b) {
    return compareRows(tableColumns, a, tableColumns, b, columns) < 0;
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
bool isInCanonicalOrder(IdColumns columns,
                        ql::span<const ColumnIndex> resultSortedOn) {
  auto order = tryComparisonColumns(columns.size(), resultSortedOn);
  if (!order.has_value()) {
    return false;
  }
  size_t numRows = numRowsOf(columns);
  for (size_t row = 1; row < numRows; ++row) {
    if (compareRows(columns, row - 1, columns, row, order.value()) > 0) {
      return false;
    }
  }
  return true;
}

// _____________________________________________________________________________
bool isInCanonicalOrder(const IdTableView<0>& table,
                        ql::span<const ColumnIndex> resultSortedOn) {
  return isInCanonicalOrder(columnsOf(table), resultSortedOn);
}

// _____________________________________________________________________________
IdTable permuteRows(const IdTableView<0>& table,
                    ql::span<const size_t> oldRowOfNewRow,
                    const ad_utility::AllocatorWithLimit<Id>& allocator) {
  IdTable result{table.numColumns(), allocator};
  result.resize(oldRowOfNewRow.size());
  for (auto&& [source, target] :
       ::ranges::views::zip(table.getColumns(), result.getColumns())) {
    for (size_t row : ad_utility::integerRange(oldRowOfNewRow.size())) {
      target[row] = source[oldRowOfNewRow[row]];
    }
  }
  return result;
}

// _____________________________________________________________________________
std::vector<size_t> invertPermutation(ql::span<const size_t> permutation) {
  std::vector<size_t> result(permutation.size(), noMatchingRow);
  for (size_t i : ad_utility::integerRange(permutation.size())) {
    AD_CONTRACT_CHECK(permutation[i] < permutation.size() &&
                      result[permutation[i]] == noMatchingRow);
    result[permutation[i]] = i;
  }
  return result;
}

// _____________________________________________________________________________
std::vector<size_t> alignRows(IdColumns base, IdColumns target,
                              ql::span<const ColumnIndex> resultSortedOn) {
  AD_CONTRACT_CHECK(base.size() == target.size());
  auto columns = comparisonColumns(base.size(), resultSortedOn);
  size_t numBase = numRowsOf(base);
  size_t numTarget = numRowsOf(target);
  std::vector<size_t> result(numTarget, noMatchingRow);
  size_t baseRow = 0;
  size_t targetRow = 0;
  while (baseRow < numBase && targetRow < numTarget) {
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

// _____________________________________________________________________________
std::vector<size_t> alignRows(const IdTableView<0>& base,
                              const IdTableView<0>& target,
                              ql::span<const ColumnIndex> resultSortedOn) {
  return alignRows(columnsOf(base), columnsOf(target), resultSortedOn);
}

}  // namespace qlever::canonicalRowOrder
