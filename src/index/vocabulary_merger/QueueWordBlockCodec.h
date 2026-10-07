// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORDBLOCKCODEC_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORDBLOCKCODEC_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "engine/idTable/ExternalIdTableSorterMergeConfig.h"
#include "index/vocabulary_merger/QueueWord.h"
#include "util/CompressedBlockFile.h"
#include "util/Exception.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/Serializer.h"

// The spilling of the output blocks of the parallel vocabulary merge, which are
// vectors of `QueueWord`s. The codec itself is purely about bytes and hence
// available in both modes, whereas the storage and its factory (see below) are
// coroutine-based and therefore only exist when
// `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is not set.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <boost/asio/any_io_executor.hpp>

#include "util/parallelBlockMerge/SpillingBlockStorage.h"
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace ad_utility::vocabulary_merger {

// The `parallelBlockMerge::SpillingBlockCodec` for blocks of `QueueWord`s.
// All the fields of all the words of a block (the word itself, whether it is
// externalized, its local index, and the index of its partial vocabulary) are
// serialized into a single byte buffer, which is then stored (and compressed
// at the level of the file) as a single block of the `CompressedBlockFile`.
class QueueWordBlockCodec {
 public:
  using Block = std::vector<detail::QueueWord>;
  using BlockMetadata = CompressedBlockFile::BlockMetadata;

  // Append the `block` to the `file` and return its metadata.
  BlockMetadata writeBlock(CompressedBlockFile& file,
                           const Block& block) const {
    serialization::ByteBufferWriteSerializer serializer;
    // Reserve the exact size of the serialized block: the number of words,
    // and for each word the size and the bytes of the string, the `bool`, the
    // local index, and the index of the partial vocabulary.
    size_t numBytes = sizeof(uint64_t);
    for (const auto& word : block) {
      numBytes += sizeof(uint64_t) + word.iriOrLiteral().size() + sizeof(bool) +
                  2 * sizeof(uint64_t);
    }
    serializer.reserve(numBytes);
    serializer << static_cast<uint64_t>(block.size());
    for (const auto& word : block) {
      serializer << word.entry_;
      serializer << static_cast<uint64_t>(word.partialFileId_);
    }
    const auto& bytes = serializer.data();
    return file.appendBlock(bytes.data(), bytes.size());
  }

  // Read the block that is described by `metadata` back from the `file`.
  Block readBlock(const CompressedBlockFile& file,
                  const BlockMetadata& metadata) const {
    std::vector<char> bytes(metadata.uncompressedSize_);
    file.readBlock(metadata, bytes.data());
    serialization::ByteBufferReadSerializer serializer{std::move(bytes)};
    uint64_t numWords = 0;
    serializer >> numWords;
    Block block;
    block.reserve(numWords);
    for (uint64_t i = 0; i < numWords; ++i) {
      auto& word = block.emplace_back();
      uint64_t partialFileId = 0;
      serializer >> word.entry_;
      serializer >> partialFileId;
      word.partialFileId_ = partialFileId;
    }
    AD_CORRECTNESS_CHECK(serializer.getCurrentPosition() ==
                         metadata.uncompressedSize_);
    return block;
  }
};

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

static_assert(parallelBlockMerge::SpillingBlockCodec<QueueWordBlockCodec>);

// The storage that spills the output blocks of the parallel vocabulary merge,
// see `parallelBlockMerge::SpillingBlockStorage`.
using QueueWordBlockStorage =
    parallelBlockMerge::SpillingBlockStorage<QueueWordBlockCodec>;

// A factory for a `QueueWordBlockStorage`, for `parallelBlockMergeToRange`.
// Every chunk of the merge spills to a file of its own, the name of which
// starts with the `filenamePrefix` (which therefore has to be unique among the
// merges that run at the same time, see
// `SpillingBlockStorage::spillFilename`). The compression and the writes run
// on the `ioExecutor`. The chunk that the consumer currently reads keeps up to
// `maxBufferedBlocksPerChunk` blocks in memory, and all other blocks are
// spilled with the given `compressionLevel` (by default the same cheap level
// as the merge phase of the external sorter).
inline auto makeQueueWordBlockStorageFactory(
    boost::asio::any_io_executor ioExecutor, std::string filenamePrefix,
    size_t maxBufferedBlocksPerChunk,
    CompressedBlockFile::CompressionLevel compressionLevel =
        compressedExternalIdTable::MERGE_PHASE_SPILL_COMPRESSION) {
  return parallelBlockMerge::makeSpillingBlockStorageFactory(
      std::move(ioExecutor), std::move(filenamePrefix), QueueWordBlockCodec{},
      maxBufferedBlocksPerChunk, compressionLevel);
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORDBLOCKCODEC_H
