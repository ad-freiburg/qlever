// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_ROWMAJORMERGEBLOCK_H
#define QLEVER_SRC_ENGINE_IDTABLE_ROWMAJORMERGEBLOCK_H

#include <cstddef>
#include <utility>
#include <variant>

#include "engine/idTable/CompressedIdTableBlocks.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/RowMajorIdTable.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"

namespace ad_utility {

// The block type of the merge phase of a `CompressedExternalIdTableSorter` in
// row-major mode (see `CompressedExternalIdTable.h`).
//
// It is row-major (a `RowMajorIdTable`) while the merge builds and reads it,
// which is what makes the comparisons and the copies of the merge touch a
// single cache line per row instead of one per column. It is column-major (an
// `IdTableStatic`) once it has left the merge, because that is the layout that
// both the consumer of the sorted result and the compressed spill file of the
// merge phase want, see `compressedIdTable::BlockCodec`.
//
// Every block is therefore transposed exactly once: either when it is spilled
// (in which case it is read back column-major and needs no further
// transposition), or by `BlockCodec::finalize` while it waits in the storage
// of the merge phase. Both of those run on the worker threads of the merge, so
// the consumer never transposes anything itself.
//
// NOTE: Only the row-major state is a range. That suffices, because the merge
// only ever iterates over the blocks that it has read via
// `RowMajorCompressedIdTableRunsInput::getBlock` and over the output blocks
// that it builds itself, and both of those are row-major.
template <size_t NumCols>
class RowMajorMergeBlock {
 public:
  using RowMajor = RowMajorIdTable<NumCols>;
  using ColumnMajor = IdTableStatic<NumCols>;
  using Row = rowMajorIdTable::Row<NumCols>;
  using value_type = Row;
  using iterator = Row*;
  using const_iterator = const Row*;

 private:
  std::variant<RowMajor, ColumnMajor> data_;

 public:
  // Construct an empty block in the row-major state.
  explicit RowMajorMergeBlock(const AllocatorWithLimit<Id>& allocator)
      : data_{RowMajor{allocator}} {}

  // Construct from a table in either of the two states.
  explicit RowMajorMergeBlock(RowMajor rows) : data_{std::move(rows)} {}
  explicit RowMajorMergeBlock(ColumnMajor table) : data_{std::move(table)} {}

  // Whether this block currently is row-major, see the class comment above.
  bool isRowMajor() const { return std::holds_alternative<RowMajor>(data_); }

  // Access the row-major table. May only be called in the row-major state.
  RowMajor& rows() {
    AD_CORRECTNESS_CHECK(isRowMajor());
    return std::get<RowMajor>(data_);
  }
  const RowMajor& rows() const {
    AD_CORRECTNESS_CHECK(isRowMajor());
    return std::get<RowMajor>(data_);
  }

  // Access the column-major table. May only be called in the column-major
  // state.
  const ColumnMajor& columnMajorTable() const {
    AD_CORRECTNESS_CHECK(!isRowMajor());
    return std::get<ColumnMajor>(data_);
  }

  // The simple getters, which work in both states.
  size_t numRows() const {
    return isRowMajor() ? std::get<RowMajor>(data_).numRows()
                        : std::get<ColumnMajor>(data_).numRows();
  }
  size_t size() const { return numRows(); }
  bool empty() const { return numRows() == 0; }
  static constexpr size_t numColumns() { return NumCols; }

  // Iteration over the rows. Only valid in the row-major state, see the NOTE
  // at the class comment above.
  iterator begin() { return rows().data(); }
  iterator end() { return rows().data() + numRows(); }
  const_iterator begin() const { return rows().data(); }
  const_iterator end() const { return rows().data() + numRows(); }
  Row& operator[](size_t row) { return rows()[row]; }
  const Row& operator[](size_t row) const { return rows()[row]; }

  // Append a single row. Only valid in the row-major state.
  template <typename R>
  void push_back(const R& row) {
    rows().push_back(row);
  }

  // Return the contents of this block as a column-major `IdTable`, transposing
  // them if this block still is row-major.
  ColumnMajor toColumnMajor(const AllocatorWithLimit<Id>& allocator) && {
    if (!isRowMajor()) {
      return std::get<ColumnMajor>(std::move(data_));
    }
    return std::get<RowMajor>(data_).toColumnMajor(allocator);
  }
};

namespace compressedIdTable {

// The codec of a `RowMajorMergeBlock`: a block is transposed before it is
// compressed, so that the file is column-major just like for an ordinary
// `IdTable` (see the comment of the primary template), and a block that is read
// back is column-major and therefore needs no transposition at all.
template <size_t NumCols>
struct BlockCodec<RowMajorMergeBlock<NumCols>> {
  using Block = RowMajorMergeBlock<NumCols>;

  // A block that stays in memory is transposed by `finalize` below, so that
  // the consumer of the merge gets a column-major block no matter whether that
  // block went through the file or not.
  static constexpr bool needsFinalization = true;

  // ___________________________________________________________________________
  static Block finalize(Block block, const AllocatorWithLimit<Id>& allocator) {
    if (!block.isRowMajor()) {
      return block;
    }
    return Block{std::move(block).toColumnMajor(allocator)};
  }

  // ___________________________________________________________________________
  static BlockMetadata write(CompressedBlockFile& file, const Block& block,
                             const AllocatorWithLimit<Id>& allocator) {
    if (!block.isRowMajor()) {
      // A block that is already column-major is simply written as it is. This
      // cannot happen in the merge phase (a block is spilled long before it
      // reaches the consumer), but the codec is well-defined for it anyway.
      const auto& columnMajor = block.columnMajorTable();
      return writeBlock(file, columnMajor, 0, columnMajor.numRows());
    }
    auto columnMajor = block.rows().toColumnMajor(allocator);
    return writeBlock(file, columnMajor, 0, columnMajor.numRows());
  }

  // ___________________________________________________________________________
  static Block read(const CompressedBlockFile& file,
                    const BlockMetadata& metadata,
                    const AllocatorWithLimit<Id>& allocator) {
    return Block{readBlock<NumCols>(file, metadata, allocator)};
  }
};

}  // namespace compressedIdTable
}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_IDTABLE_ROWMAJORMERGEBLOCK_H
