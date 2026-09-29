// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTCOMMITTER_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTCOMMITTER_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "index/vocabulary_merger/IdMapWriters.h"
#include "index/vocabulary_merger/Segment.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "util/GlobalExecutor.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/PostAndGetFuture.h"
#include "util/ProgressBar.h"
#include "util/TaskQueue.h"
#include "util/Timer.h"

// The stage of the vocabulary merger that commits the segments in the order
// of the vocabulary (see the comment above `mergeVocabulary` in
// `index/VocabularyMerger.h`). This is not part of the public interface of
// that header.
namespace ad_utility::vocabulary_merger::detail {

// Commit the segments of the merged vocabulary (see `Segment`) in order: assign
// the global IDs (see `SegmentBases`), maintain the metadata, hand the ID map
// entries to the `IdMapWriters`, and hand the words to the block writers of
// the vocabulary (see `ParallelWordWriterBase`). The words of a sub-vocabulary
// are cut into blocks of `blockSize()` words that are aligned to the global
// position (so that the files are the same as if the words were written one
// by one); a block typically spans several segments. A full block is prepared
// on the global thread pool (see `BlockWriterBase::prepare`) and then
// appended by a thread of its own per sub-vocabulary (`append`), in order.
//
// NOTE: All the member functions are called by a single thread, the one that
// runs `mergeVocabulary`.
class SegmentCommitter {
 private:
  // A range of the words of a sub-vocabulary of a segment that belongs to an
  // open block. The segment is kept alive by the piece.
  struct Piece {
    std::shared_ptr<const Segment> segment_;
    uint8_t sub_;
    size_t firstWord_;
    size_t numWords_;
  };

  // A block of a sub-vocabulary that is being filled with pieces.
  struct OpenBlock {
    std::vector<Piece> pieces_;
    size_t numWords_ = 0;
    uint64_t firstPosition_ = 0;
  };

  ParallelWordWriterBase& writer_;
  size_t numSubs_;
  SegmentBases bases_;
  VocabularyMetaData metaData_;
  ad_utility::ProgressBar progressBar_{metaData_.numWordsTotal(),
                                       "Words merged: "};
  std::vector<OpenBlock> openBlocks_;
  // The first exception that a prepare or append task threw, if any (an
  // exception must not escape the thread of a queue). The remaining blocks
  // are then skipped and the exception is rethrown by `finish()`.
  std::atomic<bool> hasFailed_{false};
  std::exception_ptr exception_;
  std::mutex exceptionMutex_;
  IdMapWriters idMapWriters_;
  // The queues of the append tasks, one single-threaded queue per
  // sub-vocabulary. NOTE: Declared after everything that their tasks use.
  std::vector<std::unique_ptr<ad_utility::TaskQueue<false>>> appendQueues_;
  std::atomic<uint64_t> prepareBusyMs_{0};
  std::atomic<uint64_t> appendBusyMs_{0};
  size_t numBlocks_ = 0;

 public:
  // Construct from the `writer` of the vocabulary (which has to stay alive,
  // and which is not finished by this class) and the files of the partial ID
  // maps (see `IdMapWriters`).
  SegmentCommitter(ParallelWordWriterBase& writer,
                   ad_utility::InputRangeTypeErased<std::string> idMapFilenames)
      : writer_{writer},
        numSubs_{writer.numSubVocabularies()},
        openBlocks_(numSubs_),
        idMapWriters_{std::move(idMapFilenames),
                      VOCAB_MERGER_NUM_ID_MAP_WRITER_THREADS} {
    for (size_t sub = 0; sub < numSubs_; ++sub) {
      appendQueues_.push_back(std::make_unique<ad_utility::TaskQueue<false>>(
          std::max<size_t>(1, ad_utility::globalExecutorNumThreads()), 1,
          "Writing the merged vocabulary"));
    }
  }

  // Commit the next `segment`.
  void commit(std::shared_ptr<const Segment> segment) {
    if (hasFailed()) {
      return;
    }
    AD_CONTRACT_CHECK(segment->words_.size() == numSubs_);
    const SegmentBases bases = bases_;
    bases_ = segment->basesOfNextSegment(bases_);
    metaData_.addSegment(
        segment->metaData_,
        [&bases](SegmentLocalId id) { return bases.globalIndexOf(id); },
        segment->numBlankNodes_);
    idMapWriters_.write(segment, bases);
    for (uint8_t sub = 0; sub < numSubs_; ++sub) {
      addWordsToBlocks(segment, sub);
    }
    if (progressBar_.update()) {
      AD_LOG_INFO << progressBar_.getProgressString() << std::flush;
    }
  }

  // Whether one of the tasks has thrown an exception. The caller should then
  // stop committing segments; the exception is rethrown by `finish()`.
  bool hasFailed() const { return hasFailed_; }

  // Write the last (partial) blocks, wait until all the blocks and ID map
  // entries are written, and return the metadata of the merged vocabulary.
  // Rethrow the first exception that one of the tasks threw, if any. After
  // this, no more segments may be committed.
  VocabularyMetaData finish() {
    for (uint8_t sub = 0; sub < numSubs_; ++sub) {
      flushBlock(sub);
    }
    for (auto& queue : appendQueues_) {
      queue->finish();
    }
    idMapWriters_.finish();
    if (exception_) {
      std::rethrow_exception(exception_);
    }
    AD_LOG_INFO << progressBar_.getFinalProgressString() << std::flush;
    AD_LOG_INFO << "Time spent by the stages behind the vocabulary merge: "
                   "preparing "
                << numBlocks_ << " blocks " << prepareBusyMs_
                << " ms, appending them " << appendBusyMs_
                << " ms, writing the ID maps " << idMapWriters_.busyMs()
                << " ms" << std::endl;
    return std::move(metaData_);
  }

 private:
  // Add the words of the sub-vocabulary `sub` of the `segment` to the open
  // block of that sub-vocabulary, flushing the block whenever it is full.
  void addWordsToBlocks(const std::shared_ptr<const Segment>& segment,
                        uint8_t sub) {
    const size_t blockSize = writer_.blockWriter(sub).blockSize();
    AD_CONTRACT_CHECK(blockSize > 0);
    const size_t numWords = segment->words_[sub].numWords();
    size_t firstWord = 0;
    while (firstWord < numWords) {
      OpenBlock& block = openBlocks_[sub];
      size_t numWordsOfPiece =
          std::min(numWords - firstWord, blockSize - block.numWords_);
      block.pieces_.push_back(Piece{segment, sub, firstWord, numWordsOfPiece});
      block.numWords_ += numWordsOfPiece;
      firstWord += numWordsOfPiece;
      if (block.numWords_ == blockSize) {
        flushBlock(sub);
      }
    }
  }

  // Gather the words of the pieces of an open block into one `WordBlock`.
  static WordBlock gather(const OpenBlock& block) {
    WordBlock result;
    result.firstPosition_ = block.firstPosition_;
    size_t numBytes = 0;
    for (const Piece& piece : block.pieces_) {
      const auto& offsets = piece.segment_->words_[piece.sub_].offsets_;
      numBytes += offsets[piece.firstWord_ + piece.numWords_] -
                  offsets[piece.firstWord_];
    }
    result.data_.reserve(numBytes);
    result.offsets_.reserve(block.numWords_ + 1);
    result.isExternal_.reserve(block.numWords_);
    for (const Piece& piece : block.pieces_) {
      const WordBlock& words = piece.segment_->words_[piece.sub_];
      for (size_t i = piece.firstWord_; i < piece.firstWord_ + piece.numWords_;
           ++i) {
        result.push(words.word(i), words.isExternal_[i]);
      }
    }
    return result;
  }

  // Prepare the open block of the sub-vocabulary `sub` on the thread pool and
  // queue its appending. Do nothing if the block is empty.
  void flushBlock(uint8_t sub) {
    OpenBlock& openBlock = openBlocks_[sub];
    if (openBlock.numWords_ == 0) {
      return;
    }
    OpenBlock block = std::exchange(
        openBlock,
        OpenBlock{{}, 0, openBlock.firstPosition_ + openBlock.numWords_});
    ++numBlocks_;
    auto& blockWriter = writer_.blockWriter(sub);
    // NOTE: The future is shared, because a `std::function` (which the queue
    // stores) has to be copyable.
    auto prepared =
        std::make_shared<std::future<std::unique_ptr<PreparedBlockBase>>>(
            ad_utility::postAndGetFuture(
                ad_utility::globalExecutor(),
                [this, &blockWriter, block = std::move(block)]() {
                  ad_utility::Timer timer{ad_utility::Timer::Started};
                  auto result = blockWriter.prepare(gather(block));
                  prepareBusyMs_ += timer.msecs().count();
                  return result;
                }));
    appendQueues_[sub]->push([this, &blockWriter, prepared]() {
      runAndCatchException([this, &blockWriter, &prepared]() {
        auto block = prepared->get();
        ad_utility::Timer timer{ad_utility::Timer::Started};
        blockWriter.append(std::move(block));
        appendBusyMs_ += timer.msecs().count();
      });
    });
  }

  // Run the `task` on the thread of one of the queues, and store the exception
  // that it throws (if any) instead of letting it escape that thread. Once a
  // task has failed, the remaining tasks are skipped.
  template <typename F>
  void runAndCatchException(const F& task) {
    if (hasFailed_) {
      return;
    }
    try {
      task();
    } catch (...) {
      std::lock_guard lock{exceptionMutex_};
      if (!exception_) {
        exception_ = std::current_exception();
      }
      hasFailed_ = true;
    }
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTCOMMITTER_H
