// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_ROWMAJORIDTABLE_H
#define QLEVER_SRC_ENGINE_IDTABLE_ROWMAJORIDTABLE_H

#include <algorithm>
#include <array>
#include <cstddef>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/UninitializedAllocator.h"

// A row-major counterpart of the (column-major) `IdTable`, together with the
// cache-friendly transpositions between the two layouts.
//
// The `IdTable` stores its data column-major, which is the right layout for
// everything that processes whole columns, but the wrong one for the row-wise
// operations of an external sort: sorting a block permutes whole rows (so every
// swap touches one cache line per column), and merging presorted runs both
// compares and copies whole rows (so every single row that is merged touches
// one cache line per column of the input block *and* one per column of the
// output block). A row-major layout turns all of those scattered accesses into
// a single contiguous one, at the price of transposing the data once on the way
// in and once on the way out.
//
// See `CompressedExternalIdTable.h`, which uses these types when the runtime
// parameter `external-sorter-row-major` is set, and which also documents where
// exactly the transpositions happen.
namespace ad_utility {

namespace rowMajorIdTable {

// The number of bytes that a single tile of the transpositions below works on.
// The tile is the working set that has to stay in the cache while the
// transposition walks over all the columns of the tile once, so this is sized
// for the L1 data cache (which is 32 KiB or more on every CPU that QLever
// targets).
constexpr inline size_t TRANSPOSITION_TILE_SIZE_BYTES = 32 * 1024;

// The number of rows of a single tile of the transpositions below, given the
// number of columns. It is at least one, no matter how wide the rows are.
template <size_t NumCols>
constexpr size_t transpositionTileNumRows() {
  static_assert(NumCols > 0);
  constexpr size_t numRows =
      TRANSPOSITION_TILE_SIZE_BYTES / (NumCols * sizeof(Id));
  return numRows > 0 ? numRows : 1;
}

// A single row of a `RowMajorIdTable` with `NumCols` columns. It is a
// `std::array` and hence dense, trivially copyable, and (in contrast to the
// proxy row references of the column-based `IdTable`) an ordinary value type,
// which is what makes sorting a range of such rows as cheap as sorting a range
// of scalars.
template <size_t NumCols>
using Row = std::array<Id, NumCols>;

// Return the `row` as an owning row of a dynamic `IdTable`, which is the type
// in which the metadata of the presorted runs stores the first and the last row
// of a block (see `CompressedExternalIdTableWriter`).
template <size_t NumCols>
IdTable::row_type toDynamicRow(const Row<NumCols>& row) {
  IdTable::row_type result{NumCols};
  for (size_t col = 0; col < NumCols; ++col) {
    result[col] = row[col];
  }
  return result;
}

// The pointers to the first element of each column of a column-major block, as
// they are consumed by the transpositions below.
template <size_t NumCols>
using ConstColumnPointers = std::array<const Id*, NumCols>;
template <size_t NumCols>
using ColumnPointers = std::array<Id*, NumCols>;

// Return the pointers to the columns of the `table` (which has to be some kind
// of `IdTable`), each of them offset by `beginRow`.
template <size_t NumCols, typename Table>
ConstColumnPointers<NumCols> columnPointers(const Table& table,
                                            size_t beginRow) {
  AD_CORRECTNESS_CHECK(table.numColumns() == NumCols);
  ConstColumnPointers<NumCols> pointers{};
  for (size_t col = 0; col < NumCols; ++col) {
    pointers[col] = table.getColumn(col).data() + beginRow;
  }
  return pointers;
}

// Return the pointers to the columns of the `table` (which has to be some kind
// of `IdTable` and must not be a view), each of them offset by `beginRow`.
template <size_t NumCols, typename Table>
ColumnPointers<NumCols> mutableColumnPointers(Table& table, size_t beginRow) {
  AD_CORRECTNESS_CHECK(table.numColumns() == NumCols);
  ColumnPointers<NumCols> pointers{};
  for (size_t col = 0; col < NumCols; ++col) {
    pointers[col] = table.getColumn(col).data() + beginRow;
  }
  return pointers;
}

// Copy `numRows` rows from the column-major `columns` into the row-major
// `rows`, which has to have room for that many rows.
//
// The loop is tiled over the rows: for a single tile, the destination rows are
// touched once per column, and a tile is small enough that all of them stay in
// the L1 cache in between (see `TRANSPOSITION_TILE_SIZE_BYTES`). Each of the
// inner loops therefore reads one column contiguously and writes strided into a
// range that is already hot, instead of walking the whole (arbitrarily large)
// destination once per column.
template <size_t NumCols>
void transposeToRowMajor(const ConstColumnPointers<NumCols>& columns,
                         size_t numRows, Row<NumCols>* rows) {
  constexpr size_t tileNumRows = transpositionTileNumRows<NumCols>();
  for (size_t tileBegin = 0; tileBegin < numRows; tileBegin += tileNumRows) {
    const size_t tileSize = std::min(tileNumRows, numRows - tileBegin);
    for (size_t col = 0; col < NumCols; ++col) {
      const Id* source = columns[col] + tileBegin;
      Row<NumCols>* target = rows + tileBegin;
      for (size_t row = 0; row < tileSize; ++row) {
        target[row][col] = source[row];
      }
    }
  }
}

// Copy `numRows` rows from the row-major `rows` into the column-major
// `columns`, each of which has to have room for that many `Id`s. This is the
// exact inverse of `transposeToRowMajor` above, and it is tiled for the same
// reason, see there.
template <size_t NumCols>
void transposeToColumnMajor(const Row<NumCols>* rows, size_t numRows,
                            const ColumnPointers<NumCols>& columns) {
  constexpr size_t tileNumRows = transpositionTileNumRows<NumCols>();
  for (size_t tileBegin = 0; tileBegin < numRows; tileBegin += tileNumRows) {
    const size_t tileSize = std::min(tileNumRows, numRows - tileBegin);
    for (size_t col = 0; col < NumCols; ++col) {
      const Row<NumCols>* source = rows + tileBegin;
      Id* target = columns[col] + tileBegin;
      for (size_t row = 0; row < tileSize; ++row) {
        target[row] = source[row][col];
      }
    }
  }
}

}  // namespace rowMajorIdTable

// A dense, row-major table of `Id`s whose number of columns is known at compile
// time. The rows are `std::array<Id, NumCols>`, so the table is exactly as
// large as its contents, a row is copied and swapped as a single contiguous
// block of `NumCols * sizeof(Id)` bytes, and the compiler knows all of those
// sizes.
//
// A number of columns that is only known at runtime is handled by dispatching
// to the matching instantiation of this class via
// `ad_utility::callFixedSize`, see `rowMajorTableVariant` in
// `CompressedExternalIdTable.h`.
//
// NOTE: The interface is deliberately only as large as the external sorter
// needs it to be. In particular there is no column-based access at all: the
// only way from and to the column-major layout are the transpositions above,
// which this class exposes as `appendTransposed` and `appendToColumnMajor`.
template <size_t NumCols>
class RowMajorIdTable {
  static_assert(NumCols > 0,
                "The number of columns of a `RowMajorIdTable` has to be known "
                "at compile time");

 public:
  static constexpr size_t numStaticColumns = NumCols;
  using Row = rowMajorIdTable::Row<NumCols>;
  using value_type = Row;
  // NOTE: The `default_init_allocator` is what makes `resize` (which
  // `appendTransposed` uses, and which is immediately followed by writing every
  // single `Id` of the new rows) skip the zeroing of those rows.
  using Allocator = default_init_allocator<Row, AllocatorWithLimit<Row>>;
  using Storage = std::vector<Row, Allocator>;
  using iterator = typename Storage::iterator;
  using const_iterator = typename Storage::const_iterator;

 private:
  Storage rows_;

 public:
  // Construct an empty table. The `allocator` is the very allocator that the
  // corresponding `IdTable`s use, so that the memory of a table counts towards
  // the same limit no matter which of the two layouts it currently has.
  explicit RowMajorIdTable(const AllocatorWithLimit<Id>& allocator)
      : rows_{Allocator{allocator.template as<Row>()}} {}

  // The simple getters. The number of columns is a constant, but is still
  // exposed as a member function, such that generic code can treat this class
  // like an `IdTable`.
  size_t numRows() const { return rows_.size(); }
  size_t size() const { return rows_.size(); }
  bool empty() const { return rows_.empty(); }
  static constexpr size_t numColumns() { return NumCols; }

  // Iteration over the rows, which is what the sorting and the merging use.
  iterator begin() { return rows_.begin(); }
  iterator end() { return rows_.end(); }
  const_iterator begin() const { return rows_.begin(); }
  const_iterator end() const { return rows_.end(); }
  Row& operator[](size_t row) { return rows_[row]; }
  const Row& operator[](size_t row) const { return rows_[row]; }
  Row* data() { return rows_.data(); }
  const Row* data() const { return rows_.data(); }

  // Modifiers that only affect the size. `clear` keeps the capacity, which is
  // what makes the buffer of the sorter reusable.
  void clear() { rows_.clear(); }
  void reserve(size_t numRows) { rows_.reserve(numRows); }
  void resize(size_t numRows) { rows_.resize(numRows); }

  // Append a single row, which may be anything that can be indexed with
  // `[0, NumCols)`, in particular a `Row` or a (proxy) row reference of an
  // `IdTable`.
  template <typename R>
  void push_back(const R& row) {
    AD_EXPENSIVE_CHECK(row.size() == NumCols);
    Row newRow;
    for (size_t col = 0; col < NumCols; ++col) {
      newRow[col] = row[col];
    }
    rows_.push_back(newRow);
  }

  // Append the rows `[beginRow, endRow)` of the column-major `table`,
  // transposing them on the way, see `rowMajorIdTable::transposeToRowMajor`.
  // This is the row-major counterpart of `IdTable::insertAtEnd`.
  CPP_template(typename Table)(
      requires IdTableLike<Table>) void appendTransposed(const Table& table,
                                                         size_t beginRow,
                                                         size_t endRow) {
    AD_CONTRACT_CHECK(beginRow <= endRow && endRow <= table.numRows());
    const size_t numNewRows = endRow - beginRow;
    const size_t oldSize = rows_.size();
    rows_.resize(oldSize + numNewRows);
    rowMajorIdTable::transposeToRowMajor<NumCols>(
        rowMajorIdTable::columnPointers<NumCols>(table, beginRow), numNewRows,
        rows_.data() + oldSize);
  }

  // Write the rows `[beginRow, endRow)` of the column-major `table` into the
  // already existing rows `[targetRow, targetRow + (endRow - beginRow))` of
  // this table, transposing them on the way. In contrast to `appendTransposed`
  // above, this doesn't change the size of this table, so several such writes
  // into disjoint target ranges may run concurrently, see
  // `CompressedExternalIdTableBase::pushBlockConcurrently`.
  CPP_template(typename Table)(
      requires IdTableLike<Table>) void writeTransposedAt(const Table& table,
                                                          size_t beginRow,
                                                          size_t endRow,
                                                          size_t targetRow) {
    AD_CONTRACT_CHECK(beginRow <= endRow && endRow <= table.numRows());
    const size_t numNewRows = endRow - beginRow;
    AD_CONTRACT_CHECK(targetRow + numNewRows <= rows_.size());
    rowMajorIdTable::transposeToRowMajor<NumCols>(
        rowMajorIdTable::columnPointers<NumCols>(table, beginRow), numNewRows,
        rows_.data() + targetRow);
  }

  // Append the rows `[beginRow, endRow)` of this table to the column-major
  // `table`, transposing them on the way, see
  // `rowMajorIdTable::transposeToColumnMajor`.
  CPP_template(typename Table)(
      requires IdTableLike<Table>) void appendToColumnMajor(Table& table,
                                                            size_t beginRow,
                                                            size_t endRow)
      const {
    AD_CONTRACT_CHECK(beginRow <= endRow && endRow <= numRows());
    const size_t numNewRows = endRow - beginRow;
    const size_t oldSize = table.numRows();
    table.resize(oldSize + numNewRows);
    rowMajorIdTable::transposeToColumnMajor<NumCols>(
        rows_.data() + beginRow, numNewRows,
        rowMajorIdTable::mutableColumnPointers<NumCols>(table, oldSize));
  }

  // Return the contents of this table as a column-major `IdTableStatic`.
  IdTableStatic<NumCols> toColumnMajor(
      const AllocatorWithLimit<Id>& allocator) const {
    IdTableStatic<NumCols> table{NumCols, allocator};
    appendToColumnMajor(table, 0, numRows());
    return table;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_IDTABLE_ROWMAJORIDTABLE_H
