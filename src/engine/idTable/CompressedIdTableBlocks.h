// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKS_H
#define QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKS_H

#include <cstddef>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdTable.h"
#include "util/CompressedBlockFile.h"
#include "util/Exception.h"

// Store a block of an `IdTable` in a `CompressedBlockFile` and read it back.
// This is the shared layer between the two users of that representation: the
// `CompressedExternalIdTableWriter` (see `CompressedExternalIdTable.h`), which
// keeps whole presorted runs on disk, and the
// `CompressedIdTableBlockStorage` (see `CompressedIdTableBlockStorage.h`),
// which spills the output blocks of the parallel merge.
namespace ad_utility::compressedIdTable {

// The metadata of a single compressed block of an `IdTable`. An `IdTable` is
// stored column-major, so each column is compressed separately, which means
// that a block consists of one compressed byte range per column.
//
// NOTE: The number of rows is stored explicitly, and not derived from the
// `uncompressedSize_` of the first column, so that a block of a table with zero
// columns is still well-defined.
struct BlockMetadata {
  size_t numRows_ = 0;
  std::vector<CompressedBlockFile::BlockMetadata> columns_;

  // The number of columns of the block.
  size_t numColumns() const { return columns_.size(); }
};

// Compress the rows `[beginRow, endRow)` of the column `columnIdx` of `table`
// and append them to `file`.
template <typename Table>
CompressedBlockFile::BlockMetadata writeColumn(CompressedBlockFile& file,
                                               const Table& table,
                                               size_t columnIdx,
                                               size_t beginRow, size_t endRow) {
  AD_CONTRACT_CHECK(beginRow <= endRow && endRow <= table.numRows());
  decltype(auto) column = table.getColumn(columnIdx);
  return file.appendBlock(column.data() + beginRow,
                          (endRow - beginRow) * sizeof(Id));
}

// Compress the rows `[beginRow, endRow)` of all the columns of `table` and
// append them to `file`, one column after the other. Return the metadata of the
// resulting block.
template <typename Table>
BlockMetadata writeBlock(CompressedBlockFile& file, const Table& table,
                         size_t beginRow, size_t endRow) {
  AD_CONTRACT_CHECK(beginRow <= endRow && endRow <= table.numRows());
  BlockMetadata metadata;
  metadata.numRows_ = endRow - beginRow;
  metadata.columns_.reserve(table.numColumns());
  for (size_t columnIdx : ql::views::iota(size_t{0}, table.numColumns())) {
    metadata.columns_.push_back(
        writeColumn(file, table, columnIdx, beginRow, endRow));
  }
  return metadata;
}

// Allocate (via the `allocator`) and size a block that can hold the rows of
// `metadata`. The contents of that block are unspecified; use
// `readColumnIntoBlock` to fill them.
template <size_t NumCols = 0>
IdTableStatic<NumCols> makeBlock(const BlockMetadata& metadata,
                                 const AllocatorWithLimit<Id>& allocator) {
  IdTableStatic<NumCols> block{metadata.numColumns(), allocator};
  block.resize(metadata.numRows_);
  return block;
}

// Read and decompress the column `columnIdx` of the block that is described by
// `metadata` from `file` into `block`, which has to have been obtained from
// `makeBlock(metadata, ...)`.
//
// NOTE: This may be called concurrently for distinct values of `columnIdx` on
// the same `block`, because the columns of an `IdTable` are disjoint and
// `CompressedBlockFile::readBlock` is thread-safe.
template <size_t NumCols = 0>
void readColumnIntoBlock(const CompressedBlockFile& file,
                         const BlockMetadata& metadata, size_t columnIdx,
                         IdTableStatic<NumCols>& block) {
  decltype(auto) column = block.getColumn(columnIdx);
  AD_CORRECTNESS_CHECK(column.size() == metadata.numRows_);
  file.readBlock(metadata.columns_.at(columnIdx), column.data());
}

// Read and decompress the whole block that is described by `metadata` from
// `file`, one column after the other.
//
// NOTE: The columns are deliberately read sequentially, so that this spawns no
// threads of its own and can be called concurrently from many threads.
template <size_t NumCols = 0>
IdTableStatic<NumCols> readBlock(const CompressedBlockFile& file,
                                 const BlockMetadata& metadata,
                                 const AllocatorWithLimit<Id>& allocator) {
  auto block = makeBlock<NumCols>(metadata, allocator);
  for (size_t columnIdx : ql::views::iota(size_t{0}, metadata.numColumns())) {
    readColumnIntoBlock<NumCols>(file, metadata, columnIdx, block);
  }
  return block;
}

}  // namespace ad_utility::compressedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKS_H
