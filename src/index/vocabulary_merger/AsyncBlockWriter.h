// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_ASYNCBLOCKWRITER_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_ASYNCBLOCKWRITER_H

#include <algorithm>
#include <atomic>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
#include <utility>
#include <vector>

#include "index/vocabulary/VocabularyTypes.h"
#include "index/vocabulary_merger/SegmentCommitter.h"
#include "util/AsioHelpers.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Executor.h"
#include "util/GlobalExecutor.h"
#include "util/TaskQueueOnExecutor.h"
#include "util/Timer.h"

// The stage of the vocabulary merger that writes the blocks of words to the
// vocabulary (see the comment above `mergeVocabulary` in
// `index/VocabularyMerger.h`). This is not part of the public interface of
// that header.
namespace ad_utility::vocabulary_merger::detail {

// The state that the `AsyncBlockWriterPerSubVocabulary`s of an
// `AsyncBlockWriter` share (see below).
struct AsyncBlockWriterSharedState {
  // The first exception that a prepare or append task threw, if any. The
  // remaining blocks are then skipped and the exception is rethrown by
  // `AsyncBlockWriter::finish()`.
  ad_utility::ExceptionCollector exceptionCollector_;
  // The total time that the tasks have spent preparing and appending the
  // blocks, in milliseconds.
  std::atomic<uint64_t> prepareBusyMs_{0};
  std::atomic<uint64_t> appendBusyMs_{0};

  // Whether one of the tasks has thrown an exception that has not been
  // rethrown yet.
  bool hasFailed() const { return exceptionCollector_.hasException(); }
};

// Write the blocks of words of one sub-vocabulary that the `SegmentCommitter`
// cuts (see `BlockPieces`) to the block writer of that sub-vocabulary (see
// `ParallelWordWriterBase` in `index/vocabulary/VocabularyTypes.h`). A block is
// gathered and prepared (see `gatherWords` and `BlockWriterBase::prepare`) by a
// task on the global thread pool (see `util/GlobalExecutor.h`), which runs
// concurrently for different blocks, and then appended to the vocabulary (the
// `AppendBlock` that `prepare` returns) in the order of the blocks.
//
// The order is restored on a strand of the pool, without ever blocking a
// thread: the pending blocks form a queue that only code on the strand
// touches. A block is registered at the back of that queue (which also starts
// its prepare task), the completion of its prepare task is posted onto the
// strand, and whenever the block at the front of the queue is prepared, it is
// appended and removed. A `TaskQueueOnExecutor` bounds the number of pending
// blocks (its `pushAsync` keeps a block in flight until it is appended).
//
// NOTE: This is written without coroutines, because the vocabulary merge is
// also compiled in the reduced C++17 build.
//
// NOTE: All the member functions are called by a single thread, which must not
// be a thread of the global executor (see `TaskQueueOnExecutor`).
class AsyncBlockWriterPerSubVocabulary {
 private:
  // A block that is being prepared or waiting to be appended. Only touched on
  // the `strand_`.
  struct PendingBlock {
    // The index of the block within the sub-vocabulary.
    size_t index_;
    // Set once the prepare task has completed, either with the prepared block
    // or with the exception that the task threw.
    bool isPrepared_ = false;
    AppendBlock appendBlock_;
    std::exception_ptr exception_;
    // The callback that releases the slot of the block in the `queue_`, see
    // `TaskQueueOnExecutor::pushAsync`.
    ad_utility::TaskQueueOnExecutor::Done done_;
  };

  BlockWriterBase& blockWriter_;
  AsyncBlockWriterSharedState& sharedState_;
  boost::asio::strand<qlever::Executor> strand_;
  // The number of blocks registered so far, which is the index of the next
  // one. Only touched on the `strand_`.
  size_t numBlocks_ = 0;
  // The pending blocks in the order of their indices. Only touched on the
  // `strand_`.
  std::deque<PendingBlock> pendingBlocks_;
  // The queue whose tasks register the blocks on the `strand_`. NOTE: Declared
  // last, because its destructor waits for the blocks to be appended, which
  // uses the members declared above.
  ad_utility::TaskQueueOnExecutor queue_;

 public:
  // Construct from the `blockWriter` of the sub-vocabulary and the
  // `sharedState`, which both have to stay alive. At most `maxNumPendingBlocks`
  // blocks are being prepared or waiting to be appended at the same time.
  AsyncBlockWriterPerSubVocabulary(BlockWriterBase& blockWriter,
                                   AsyncBlockWriterSharedState& sharedState,
                                   size_t maxNumPendingBlocks)
      : blockWriter_{blockWriter},
        sharedState_{sharedState},
        strand_{boost::asio::make_strand(ad_utility::globalExecutor())},
        queue_{strand_, maxNumPendingBlocks, "Writing the merged vocabulary"} {}

  // Write the next `block`. Block while the maximal number of blocks is
  // pending.
  void push(BlockPieces block) {
    queue_.pushAsync([this, block = std::move(block)](
                         ad_utility::TaskQueueOnExecutor::Done done) mutable {
      // This runs on the strand: register the block and start its prepare
      // task, whose completion is posted back onto the strand.
      const size_t index = numBlocks_++;
      pendingBlocks_.push_back(
          PendingBlock{index, false, AppendBlock{}, nullptr, std::move(done)});
      auto prepare = [this, block = std::move(block)]() {
        ad_utility::Timer timer{ad_utility::Timer::Started};
        auto result = blockWriter_.prepare(gatherWords(block));
        sharedState_.prepareBusyMs_ += timer.msecs().count();
        return result;
      };
      ad_utility::runFunctionOnExecutor(
          ad_utility::globalExecutor(), std::move(prepare),
          boost::asio::bind_executor(
              strand_, [this, index](std::exception_ptr exception,
                                     AppendBlock appendBlock) {
                blockIsPrepared(index, std::move(exception),
                                std::move(appendBlock));
              }));
    });
  }

  // Wait until all the blocks are appended. After this, no more blocks may be
  // pushed.
  void finish() { queue_.finish(); }

 private:
  // Store the result of the prepare task of the block with the given `index`
  // (the `appendBlock`, or the `exception` that the task threw), and append
  // all the blocks at the front of the queue that are prepared, in order.
  // PRECONDITION: This runs on the `strand_`.
  void blockIsPrepared(size_t index, std::exception_ptr exception,
                       AppendBlock appendBlock) {
    auto& pending = pendingBlocks_;
    AD_CORRECTNESS_CHECK(!pending.empty() && index >= pending.front().index_ &&
                         index - pending.front().index_ < pending.size());
    PendingBlock& block = pending[index - pending.front().index_];
    block.isPrepared_ = true;
    block.exception_ = std::move(exception);
    block.appendBlock_ = std::move(appendBlock);
    while (!pending.empty() && pending.front().isPrepared_) {
      PendingBlock next = std::move(pending.front());
      pending.pop_front();
      appendPendingBlock(std::move(next));
    }
  }

  // Append the prepared `block`, unless a task has failed (then the block is
  // skipped), and release its slot in the queue.
  // PRECONDITION: This runs on the `strand_`.
  void appendPendingBlock(PendingBlock block) {
    if (block.exception_) {
      sharedState_.exceptionCollector_(std::move(block.exception_));
    } else if (!sharedState_.hasFailed()) {
      try {
        ad_utility::Timer timer{ad_utility::Timer::Started};
        std::move(block.appendBlock_)();
        sharedState_.appendBusyMs_ += timer.msecs().count();
      } catch (...) {
        sharedState_.exceptionCollector_(std::current_exception());
      }
    }
    std::move(block.done_)();
  }
};

// Write the blocks of words that the `SegmentCommitter` cuts to the block
// writers of a `ParallelWordWriterBase`: dispatch each block to the
// `AsyncBlockWriterPerSubVocabulary` (see above) of its sub-vocabulary, which
// do all the work.
//
// NOTE: All the member functions are called by a single thread, which must not
// be a thread of the global executor (see `TaskQueueOnExecutor`).
class AsyncBlockWriter {
 private:
  AsyncBlockWriterSharedState sharedState_;
  size_t numBlocks_ = 0;
  // NOTE: Declared after everything that the tasks use, because the
  // destructors of the writers wait for their tasks.
  std::vector<std::unique_ptr<AsyncBlockWriterPerSubVocabulary>>
      subVocabularies_;

 public:
  // Construct from the `writer` of the vocabulary, which has to stay alive,
  // and which is not finished by this class.
  explicit AsyncBlockWriter(ParallelWordWriterBase& writer) {
    for (uint8_t sub = 0; sub < writer.numSubVocabularies(); ++sub) {
      subVocabularies_.push_back(
          std::make_unique<AsyncBlockWriterPerSubVocabulary>(
              writer.blockWriter(sub), sharedState_,
              std::max<size_t>(1, ad_utility::globalExecutorNumThreads())));
    }
  }

  // Write the next `block` of the sub-vocabulary `sub`. Do nothing if a task
  // has failed.
  void push(uint8_t sub, BlockPieces block) {
    if (hasFailed()) {
      return;
    }
    ++numBlocks_;
    subVocabularies_.at(sub)->push(std::move(block));
  }

  // Whether one of the tasks has thrown an exception that `finish()` has not
  // rethrown yet. The caller should then stop pushing blocks.
  bool hasFailed() const { return sharedState_.hasFailed(); }

  // Wait until all the blocks are appended, and rethrow the first exception
  // that one of the tasks threw, if any. After this, no more blocks may be
  // pushed. The `writer` is not finished.
  void finish() {
    for (auto& subVocabulary : subVocabularies_) {
      subVocabulary->finish();
    }
    sharedState_.exceptionCollector_.rethrowIfException();
  }

  // The number of blocks pushed so far, and the total time that the tasks
  // have spent preparing and appending them, in milliseconds.
  size_t numBlocks() const { return numBlocks_; }
  uint64_t prepareBusyMs() const { return sharedState_.prepareBusyMs_; }
  uint64_t appendBusyMs() const { return sharedState_.appendBusyMs_; }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_ASYNCBLOCKWRITER_H
