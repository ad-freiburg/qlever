// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTCOMMITTER_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTCOMMITTER_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "index/vocabulary_merger/Segment.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/ProgressBar.h"

// The stage of the vocabulary merger that commits the segments (see
// `index/vocabulary_merger/Segment.h`) in the order of the vocabulary. This is
// not part of the public interface of `index/VocabularyMerger.h`.
namespace ad_utility::vocabulary_merger::detail {

// A range of the words of one sub-vocabulary of a segment (see `Segment`) that
// belong to one block of that sub-vocabulary (see `BlockPieces`). The piece
// keeps its segment alive.
struct SegmentPiece {
  std::shared_ptr<const Segment> segment_;
  uint8_t sub_;
  size_t firstWord_;
  size_t numWords_;

  // Return the words of the sub-vocabulary of the segment that this piece
  // belongs to.
  const WordBlock& words() const { return segment_->wordBlocks_[sub_]; }
};

// The pieces of consecutive segments that together form one block of a
// sub-vocabulary: `numWords_` words, which start at the position
// `firstPosition_` of the sub-vocabulary. A block typically spans several
// segments; its words are gathered into a `WordBlock` by `gatherWords`.
struct BlockPieces {
  std::vector<SegmentPiece> pieces_;
  size_t numWords_ = 0;
  uint64_t firstPosition_ = 0;
};

// Gather the words of the `block` into one `WordBlock` (see
// `index/vocabulary/VocabularyTypes.h`), which also carries their precomputed
// payload. This is per-word work, which the `SegmentCommitter` leaves to its
// sink.
inline WordBlock gatherWords(const BlockPieces& block) {
  WordBlock result;
  result.firstPosition_ = block.firstPosition_;
  result.isExternal_.reserve(block.numWords_);
  if (!block.pieces_.empty()) {
    result.payloadSize_ = block.pieces_.front().words().payloadSize_;
    result.payload_.reserve(block.numWords_ * result.payloadSize_);
  }
  for (const SegmentPiece& piece : block.pieces_) {
    const WordBlock& words = piece.words();
    for (size_t i = piece.firstWord_; i < piece.firstWord_ + piece.numWords_;
         ++i) {
      result.push(words.word(i), words.isExternal_[i], words.payload(i));
    }
  }
  return result;
}

// Commit the segments of the merged vocabulary (see `Segment`) in order: assign
// the global IDs (see `IdConverterForSegment`), maintain the metadata, and cut
// the words of each sub-vocabulary into blocks of the `blockSize()` of its
// block writer (see `ParallelWordWriterBase`), which are aligned to the global
// position (so that the files are the same as if the words were written one by
// one). A full block is handed to the `BlockSink` right away, the last partial
// block of each sub-vocabulary by `finish()`.
//
// This class does no per-word work (see `gatherWords`) and nothing
// asynchronous; the writing of the blocks and of the ID map entries is the job
// of the caller, which hands the blocks to the writer of the vocabulary and the
// ID map entries to the writers of the partial ID maps.
class SegmentCommitter {
 public:
  // The sink for the blocks of a sub-vocabulary, called with the index of the
  // sub-vocabulary and the block, in the order of the blocks.
  using BlockSink = absl::AnyInvocable<void(uint8_t sub, BlockPieces block)>;

 private:
  // The block size of each sub-vocabulary.
  std::vector<size_t> blockSizes_;
  BlockSink blockSink_;
  IdConverterForSegment idConverter_;
  VocabularyMetaData metaData_;
  ad_utility::ProgressBar progressBar_{metaData_.numWordsTotal(),
                                       "Words merged: "};
  // The block of each sub-vocabulary that is currently being filled.
  std::vector<BlockPieces> openBlocks_;

 public:
  // Construct from the `writer` of the vocabulary (only its block sizes are
  // used) and the `blockSink`.
  SegmentCommitter(const ParallelWordWriterBase& writer, BlockSink blockSink)
      : blockSink_{std::move(blockSink)},
        openBlocks_(writer.numSubVocabularies()) {
    for (uint8_t sub = 0; sub < writer.numSubVocabularies(); ++sub) {
      blockSizes_.push_back(writer.blockWriter(sub).blockSize());
      AD_CONTRACT_CHECK(blockSizes_.back() > 0);
    }
  }

  // The number of sub-vocabularies of the vocabulary that is written.
  uint8_t numSubVocabularies() const {
    return static_cast<uint8_t>(blockSizes_.size());
  }

  // Commit the next `segment`. Return the converter that turns the
  // segment-local IDs of this segment into the global ones (the caller needs
  // it for writing the ID map entries of the segment).
  IdConverterForSegment commit(const std::shared_ptr<const Segment>& segment) {
    AD_CONTRACT_CHECK(segment->wordBlocks_.size() == numSubVocabularies());
    const IdConverterForSegment converter = std::exchange(
        idConverter_, segment->idConverterForNextSegment(idConverter_));
    metaData_.addSegment(
        segment->metaData_,
        [&converter](SegmentLocalId id) { return converter.globalIndexOf(id); },
        segment->numBlankNodes_);
    for (uint8_t sub = 0; sub < numSubVocabularies(); ++sub) {
      addWordsToBlocks(segment, sub);
    }
    if (progressBar_.update()) {
      AD_LOG_INFO << progressBar_.getProgressString() << std::flush;
    }
    return converter;
  }

  // Hand the last (partial) block of each sub-vocabulary to the sink and
  // return the metadata of the merged vocabulary. After this, no more segments
  // may be committed.
  VocabularyMetaData finish() {
    for (uint8_t sub = 0; sub < numSubVocabularies(); ++sub) {
      flushBlock(sub);
    }
    return std::move(metaData_);
  }

  // The progress bar over the committed words, for the final log message.
  ad_utility::ProgressBar& progressBar() { return progressBar_; }

 private:
  // Add the words of the sub-vocabulary `sub` of the `segment` to the open
  // block of that sub-vocabulary, flushing the block whenever it is full.
  void addWordsToBlocks(const std::shared_ptr<const Segment>& segment,
                        uint8_t sub) {
    const size_t blockSize = blockSizes_[sub];
    const size_t numWords = segment->wordBlocks_[sub].numWords();
    size_t firstWord = 0;
    while (firstWord < numWords) {
      BlockPieces& block = openBlocks_[sub];
      size_t numWordsOfPiece =
          std::min(numWords - firstWord, blockSize - block.numWords_);
      block.pieces_.push_back(
          SegmentPiece{segment, sub, firstWord, numWordsOfPiece});
      block.numWords_ += numWordsOfPiece;
      firstWord += numWordsOfPiece;
      if (block.numWords_ == blockSize) {
        flushBlock(sub);
      }
    }
  }

  // Hand the open block of the sub-vocabulary `sub` to the sink and open the
  // next one. Do nothing if the block is empty.
  void flushBlock(uint8_t sub) {
    BlockPieces& openBlock = openBlocks_[sub];
    if (openBlock.numWords_ == 0) {
      return;
    }
    BlockPieces block = std::exchange(
        openBlock,
        BlockPieces{{}, 0, openBlock.firstPosition_ + openBlock.numWords_});
    blockSink_(sub, std::move(block));
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTCOMMITTER_H
