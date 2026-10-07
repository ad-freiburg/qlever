// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYINPUT_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYINPUT_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "index/IndexBuilderTypes.h"
#include "index/PartialVocabularyFilenames.h"
#include "index/vocabulary_merger/PartialVocabularySkipPointers.h"
#include "index/vocabulary_merger/QueueWord.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/Iterators.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/BufferedPreadReadSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

namespace ad_utility::vocabulary_merger {

// A single block of a partial vocabulary words file, read lazily (see the LAZY
// BLOCKS note at `ad_utility::parallelBlockMerge::InputConcept`): it owns a
// `BufferedPreadReadSerializer` that starts at the byte offset of the block
// and yields exactly the words of the block as `QueueWord`s, one at a time.
class PartialVocabularyBlock
    : public ad_utility::InputRangeFromGet<detail::QueueWord> {
 private:
  ad_utility::serialization::BufferedPreadReadSerializer reader_;
  uint64_t numRemainingWords_;
  size_t partialFileId_;

 public:
  // Read the `numWords` words that start at the byte offset `byteOffset` of
  // the `file` through a buffer of `bufferSize` bytes, and attribute them to
  // the partial vocabulary with the index `partialFileId`.
  PartialVocabularyBlock(std::shared_ptr<const ad_utility::File> file,
                         uint64_t byteOffset, uint64_t numWords,
                         size_t partialFileId, MemorySize bufferSize)
      : reader_{std::move(file), byteOffset, bufferSize},
        numRemainingWords_{numWords},
        partialFileId_{partialFileId} {}

  // Yield the next word, see `ad_utility::InputRangeFromGet`.
  std::optional<detail::QueueWord> get() override {
    if (numRemainingWords_ == 0) {
      return std::nullopt;
    }
    --numRemainingWords_;
    TripleComponentWithIndex word;
    reader_ >> word;
    return detail::QueueWord{std::move(word), partialFileId_};
  }
};

// Expose the partial vocabulary words files (as written by
// `writePartialVocabularyToFile`) as an
// `ad_utility::parallelBlockMerge::InputConcept`. Every file is one presorted
// run, and its blocks are exactly the blocks that are described by its skip
// pointers (see `PartialVocabularySkipPointers.h`), which are read into memory
// at construction. The blocks are lazy, so the memory that the merge needs for
// its input is only the read buffer of each block that is currently being
// merged, see `readBufferSizeForBudget` below.
class PartialVocabularyInput {
 public:
  using Element = detail::QueueWord;
  using value_type = detail::QueueWord;
  using Block = PartialVocabularyBlock;
  using OutputBlock = std::vector<detail::QueueWord>;

  // The bounds for the size of the read buffer of a single block, see
  // `readBufferSizeForBudget`.
  static constexpr MemorySize minReadBufferSize = MemorySize::bytes(64 << 10);
  static constexpr MemorySize maxReadBufferSize = MemorySize::bytes(4 << 20);

 private:
  // The metadata of a single block, which is available without any I/O.
  struct BlockMetadata {
    uint64_t byteOffset_;
    // The number of bytes that the words of the block occupy in the file.
    uint64_t numBytes_;
    uint64_t numWords_;
    Element firstElement_;
    Element lastElement_;
  };

  // A single run, that is a single partial vocabulary words file.
  struct Run {
    // The `File` is shared by all the blocks of the run, which read it
    // concurrently via `pread`.
    std::shared_ptr<const ad_utility::File> file_;
    std::vector<BlockMetadata> blocks_;
  };

  std::vector<Run> runs_;
  MemorySize readBufferSize_;

 public:
  // Open the `numPartialVocabularies` partial vocabulary words files with the
  // given `basename` (see `partialVocabularyWordsFilename`) and read their
  // skip pointers. Each block that is read uses a buffer of `readBufferSize`
  // bytes (or less, if the block is smaller).
  PartialVocabularyInput(std::string_view basename,
                         size_t numPartialVocabularies,
                         MemorySize readBufferSize)
      : readBufferSize_{readBufferSize} {
    AD_CONTRACT_CHECK(readBufferSize_.getBytes() > 0);
    runs_.reserve(numPartialVocabularies);
    for (size_t runIdx = 0; runIdx < numPartialVocabularies; ++runIdx) {
      auto filename = partialVocabularyWordsFilename(basename, runIdx);
      auto skipPointers = readPartialVocabularySkipPointers(filename);
      Run& run = runs_.emplace_back();
      run.file_ = std::make_shared<const ad_utility::File>(filename, "r");
      run.blocks_.reserve(skipPointers.numBlocks());
      for (size_t blockIdx = 0; blockIdx < skipPointers.numBlocks();
           ++blockIdx) {
        auto& pointer = skipPointers.skipPointers_[blockIdx];
        run.blocks_.push_back(BlockMetadata{
            pointer.byteOffset_,
            skipPointers.byteEndOfBlock(blockIdx) - pointer.byteOffset_,
            skipPointers.numWordsInBlock(blockIdx),
            Element{std::move(pointer.firstWord_), runIdx},
            Element{std::move(pointer.lastWord_), runIdx}});
      }
    }
  }

  // Return the size of the read buffer of a single block, such that
  // `numChunksInFlight` chunks, each of which reads one block of each of the
  // `numRuns` runs at the same time, stay within the `inputBudget`. The result
  // is clamped to `[minReadBufferSize, maxReadBufferSize]`, so the budget may
  // be exceeded if it is too small, see `maxNumChunksInFlightForBudget`.
  static MemorySize readBufferSizeForBudget(MemorySize inputBudget,
                                            size_t numRuns,
                                            size_t numChunksInFlight) {
    size_t numBuffers = std::max<size_t>(1, numRuns * numChunksInFlight);
    size_t bytes =
        std::clamp(inputBudget.getBytes() / numBuffers,
                   minReadBufferSize.getBytes(), maxReadBufferSize.getBytes());
    return MemorySize::bytes(bytes);
  }

  // Return the largest number of chunks in flight (at most
  // `desiredNumChunksInFlight`, but at least one) for which the read buffers
  // of all the `numRuns` runs still fit into the `inputBudget` when they have
  // the minimal size `minReadBufferSize`.
  static size_t maxNumChunksInFlightForBudget(MemorySize inputBudget,
                                              size_t numRuns,
                                              size_t desiredNumChunksInFlight) {
    size_t bytesPerChunk =
        std::max<size_t>(1, numRuns) * minReadBufferSize.getBytes();
    size_t maxNumChunks = inputBudget.getBytes() / bytesPerChunk;
    return std::max<size_t>(1,
                            std::min(desiredNumChunksInFlight, maxNumChunks));
  }

  // ________________________________________________________________________
  size_t numRuns() const { return runs_.size(); }

  // ________________________________________________________________________
  size_t numBlocks(size_t runIdx) const {
    return runs_.at(runIdx).blocks_.size();
  }

  // ________________________________________________________________________
  size_t numElementsInBlock(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx).numWords_;
  }

  // ________________________________________________________________________
  const Element& firstElement(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx).firstElement_;
  }

  // ________________________________________________________________________
  const Element& lastElement(size_t runIdx, size_t blockIdx) const {
    return block(runIdx, blockIdx).lastElement_;
  }

  // Return the lazy block with the given indices. No I/O happens before the
  // block is iterated over. This function is thread-safe, and it may be called
  // several times for the same block, every call yields an independent block.
  Block getBlock(size_t runIdx, size_t blockIdx) const {
    const auto& metadata = block(runIdx, blockIdx);
    // A block never needs a buffer that is larger than the block itself.
    auto bufferSize = MemorySize::bytes(std::min(
        readBufferSize_.getBytes(), std::max<size_t>(1, metadata.numBytes_)));
    return Block{runs_.at(runIdx).file_, metadata.byteOffset_,
                 metadata.numWords_, runIdx, bufferSize};
  }

  // ________________________________________________________________________
  OutputBlock makeEmptyBlock() const { return OutputBlock{}; }

  // ________________________________________________________________________
  template <typename T>
  void appendToBlock(OutputBlock& outputBlock, T&& element) const {
    outputBlock.push_back(AD_FWD(element));
  }

  // ________________________________________________________________________
  MemorySize memorySizeOfElement(const Element& element) const {
    return detail::sizeOfQueueWord(element);
  }

 private:
  // Return the metadata of the block with the given indices.
  const BlockMetadata& block(size_t runIdx, size_t blockIdx) const {
    return runs_.at(runIdx).blocks_.at(blockIdx);
  }
};

static_assert(
    ad_utility::parallelBlockMerge::InputConcept<PartialVocabularyInput>);

}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_PARTIALVOCABULARYINPUT_H
