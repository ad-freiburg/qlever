// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKSTORAGE_H
#define QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKSTORAGE_H

// A model of the `parallelBlockMerge::BlockStorageConcept` that spills the
// output blocks of the merge to disk. Its only user is the `InOrderBlockSink`,
// which is coroutine-based, so this whole header is empty when
// `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set, see
// `util/parallelBlockMerge/BlockStorage.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/any_io_executor.hpp>
#include <cstddef>
#include <string>
#include <utility>

#include "engine/idTable/CompressedIdTableBlocks.h"
#include "engine/idTable/IdTable.h"
#include "util/CompressedBlockFile.h"
#include "util/parallelBlockMerge/SpillingBlockStorage.h"

namespace ad_utility {

namespace net = boost::asio;

// The `parallelBlockMerge::SpillingBlockStorage` for blocks of type
// `IdTableStatic<NumCols>`, which the merge phase of the external sorter uses,
// see `util/parallelBlockMerge/SpillingBlockStorage.h` and
// `compressedIdTable::IdTableBlockCodec`.
template <size_t NumCols = 0>
using CompressedIdTableBlockStorage = parallelBlockMerge::SpillingBlockStorage<
    compressedIdTable::IdTableBlockCodec<NumCols>>;

// A factory for a `CompressedIdTableBlockStorage`, for the constructor of
// `InOrderBlockSink`. The `allocator` is the one for the blocks that are read
// back, see `compressedIdTable::IdTableBlockCodec`, and the other arguments are
// those of the constructor of `parallelBlockMerge::SpillingBlockStorage`.
template <size_t NumCols>
auto makeCompressedIdTableStorageFactory(
    net::any_io_executor ioExecutor, std::string filenamePrefix,
    AllocatorWithLimit<Id> allocator, size_t maxBufferedBlocksPerChunk,
    CompressedBlockFile::CompressionLevel compressionLevel =
        ZSTD_DEFAULT_LEVEL) {
  return parallelBlockMerge::makeSpillingBlockStorageFactory(
      std::move(ioExecutor), std::move(filenamePrefix),
      compressedIdTable::IdTableBlockCodec<NumCols>{std::move(allocator)},
      maxBufferedBlocksPerChunk, compressionLevel);
}

}  // namespace ad_utility

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKSTORAGE_H
