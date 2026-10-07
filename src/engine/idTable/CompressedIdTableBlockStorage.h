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
#include "util/parallelBlockMerge/BlockStorage.h"
#include "util/parallelBlockMerge/SpillingBlockStorage.h"

namespace ad_utility {

namespace net = boost::asio;

// The `parallelBlockMerge::SpillingBlockStorage` for blocks of type
// `IdTableStatic<NumCols>`, which the merge phase of the external sorter uses.
// All the actual work is done by that generic storage (see
// `util/parallelBlockMerge/SpillingBlockStorage.h`), this class only fixes the
// codec (`compressedIdTable::IdTableBlockCodec`) and takes the `allocator` of
// that codec directly.
template <size_t NumCols = 0>
class CompressedIdTableBlockStorage
    : public parallelBlockMerge::SpillingBlockStorage<
          compressedIdTable::IdTableBlockCodec<NumCols>> {
 public:
  using Codec = compressedIdTable::IdTableBlockCodec<NumCols>;
  using Base = parallelBlockMerge::SpillingBlockStorage<Codec>;

  // Construct from the `ioExecutor` on which the compression and the writes
  // are run, the prefix of the names of the files to spill to, the `allocator`
  // for the blocks that are read back, the number of blocks that the chunk
  // which the consumer currently reads keeps in memory, and the
  // `compressionLevel` of the spilled blocks. See the constructor of
  // `parallelBlockMerge::SpillingBlockStorage` for the details.
  CompressedIdTableBlockStorage(
      net::any_io_executor ioExecutor, std::string filenamePrefix,
      AllocatorWithLimit<Id> allocator, size_t maxBufferedBlocksPerChunk,
      CompressedBlockFile::CompressionLevel compressionLevel =
          ZSTD_DEFAULT_LEVEL)
      : Base{std::move(ioExecutor), std::move(filenamePrefix),
             Codec{std::move(allocator)}, maxBufferedBlocksPerChunk,
             compressionLevel} {}
};

// A factory for a `CompressedIdTableBlockStorage`, for the constructor of
// `InOrderBlockSink`. The arguments are those of the constructor of that class.
template <size_t NumCols>
auto makeCompressedIdTableStorageFactory(
    net::any_io_executor ioExecutor, std::string filenamePrefix,
    AllocatorWithLimit<Id> allocator, size_t maxBufferedBlocksPerChunk,
    CompressedBlockFile::CompressionLevel compressionLevel =
        ZSTD_DEFAULT_LEVEL) {
  return [ioExecutor = std::move(ioExecutor),
          filenamePrefix = std::move(filenamePrefix),
          allocator = std::move(allocator), maxBufferedBlocksPerChunk,
          compressionLevel](
             [[maybe_unused]] const parallelBlockMerge::Strand& strand) {
    // NOTE: This storage brings a strand of its own, so the one that the
    // sink offers is not needed.
    return CompressedIdTableBlockStorage<NumCols>{
        ioExecutor, filenamePrefix, allocator, maxBufferedBlocksPerChunk,
        compressionLevel};
  };
}

}  // namespace ad_utility

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_IDTABLE_COMPRESSEDIDTABLEBLOCKSTORAGE_H
