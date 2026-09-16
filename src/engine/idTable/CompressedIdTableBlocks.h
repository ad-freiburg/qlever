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
#include <range/v3/view/zip.hpp>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdTable.h"
#include "util/CompressedBlockFile.h"
#include "util/Exception.h"

// Store a block of an `IdTable` in a `CompressedBlockFile` and read it back.
// This is the codec of the block storage that a follow-up PR will use to spill
// the output blocks of the parallel merge to disk. It lives in a header of its
// own, because it is the part of that storage that is purely about bytes and
// can hence be read and tested without any of the asynchronous machinery.
//
// NOTE: The `CompressedExternalIdTableWriter` (see
// `CompressedExternalIdTable.h`) stores its blocks in a very similar way, but
// deliberately does not share this code: its metadata are organized per column
// and not per block, because it writes whole presorted runs in one go, whereas
// that storage writes single blocks of several chunks interleaved.
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
  for (const auto& column : table.getColumns()) {
    metadata.columns_.push_back(file.appendBlock(
        column.data() + beginRow, (endRow - beginRow) * sizeof(Id)));
  }
  return metadata;
}

// Read and decompress the block that is described by `metadata` from `file`,
// one column after the other, into a block that is allocated via `allocator`.
//
// NOTE: The columns are deliberately read sequentially, so that this spawns no
// threads of its own and can be called concurrently from many threads.
template <size_t NumCols = 0>
IdTableStatic<NumCols> readBlock(const CompressedBlockFile& file,
                                 const BlockMetadata& metadata,
                                 const AllocatorWithLimit<Id>& allocator) {
  IdTableStatic<NumCols> block{metadata.numColumns(), allocator};
  block.resize(metadata.numRows_);
  AD_CORRECTNESS_CHECK(block.numColumns() == metadata.numColumns());
  for (auto [columnMetadata, column] :
       ::ranges::views::zip(metadata.columns_, block.getColumns())) {
    AD_CORRECTNESS_CHECK(column.size() == metadata.numRows_);
    file.readBlock(columnMetadata, column.data());
  }
  return block;
}

}  // namespace ad_utility::compressedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKS_H
