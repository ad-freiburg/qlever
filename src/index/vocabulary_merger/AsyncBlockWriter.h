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
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <utility>
#include <vector>

#include "index/vocabulary/VocabularyTypes.h"
#include "index/vocabulary_merger/SegmentCommitter.h"
#include "util/ExceptionHandling.h"
#include "util/GlobalExecutor.h"
#include "util/TaskQueueOnExecutor.h"
#include "util/Timer.h"

// The stage of the vocabulary merger that writes the blocks of words to the
// vocabulary (see the comment above `mergeVocabulary` in
// `index/VocabularyMerger.h`). This is not part of the public interface of
// that header.
namespace ad_utility::vocabulary_merger::detail {

// Write the blocks of words that the `SegmentCommitter` cuts (see
// `BlockPieces`) to the block writers of a `ParallelWordWriterBase` (see
// `index/vocabulary/VocabularyTypes.h`). A block is gathered and prepared (see
// `gatherWords` and `BlockWriterBase::prepare`) by a task on the global thread
// pool (see `util/GlobalExecutor.h`), and then appended to the vocabulary (the
// `AppendBlock` that `prepare` returns) by a task on a strand of that pool,
// one strand per sub-vocabulary, in the order of the blocks.
//
// NOTE: All the member functions are called by a single thread, which must not
// be a thread of the global executor (see `TaskQueueOnExecutor`). The append
// task of a block waits for the prepare task of the same block. That cannot
// deadlock, because the prepare task was posted before the append task and
// never waits for anything itself, and because of the strand at most one
// append task per sub-vocabulary runs (and hence waits) at any time.
class AsyncBlockWriter {
 private:
  ParallelWordWriterBase& writer_;
  // The first exception that a prepare or append task threw, if any. The
  // remaining blocks are then skipped and the exception is rethrown by
  // `finish()`.
  ad_utility::ExceptionCollector exceptionCollector_;
  std::atomic<uint64_t> prepareBusyMs_{0};
  std::atomic<uint64_t> appendBusyMs_{0};
  size_t numBlocks_ = 0;
  // The queue of the append tasks of each sub-vocabulary, see above. A queued
  // append task holds the future of its prepare task, so the bound of the
  // queue also bounds the number of blocks that are prepared or waiting to be
  // appended per sub-vocabulary. NOTE: Declared after everything that the
  // tasks use, because the destructor of a queue waits for its tasks.
  std::vector<std::unique_ptr<ad_utility::TaskQueueOnExecutor>> appendQueues_;

 public:
  // Construct from the `writer` of the vocabulary, which has to stay alive,
  // and which is not finished by this class.
  explicit AsyncBlockWriter(ParallelWordWriterBase& writer) : writer_{writer} {
    for (uint8_t sub = 0; sub < writer_.numSubVocabularies(); ++sub) {
      appendQueues_.push_back(std::make_unique<ad_utility::TaskQueueOnExecutor>(
          boost::asio::make_strand(ad_utility::globalExecutor()),
          std::max<size_t>(1, ad_utility::globalExecutorNumThreads()),
          "Writing the merged vocabulary"));
    }
  }

  // Write the next `block` of the sub-vocabulary `sub`. Do nothing if a task
  // has failed.
  void push(uint8_t sub, BlockPieces block) {
    if (hasFailed()) {
      return;
    }
    ++numBlocks_;
    auto& blockWriter = writer_.blockWriter(sub);
    auto prepare = [this, &blockWriter, block = std::move(block)]() {
      ad_utility::Timer timer{ad_utility::Timer::Started};
      auto result = blockWriter.prepare(gatherWords(block));
      prepareBusyMs_ += timer.msecs().count();
      return result;
    };
    std::future<AppendBlock> prepared =
        boost::asio::post(ad_utility::globalExecutor(),
                          boost::asio::use_future(std::move(prepare)));
    appendQueues_[sub]->push(exceptionCollector_.wrap(
        [this, prepared = std::move(prepared)]() mutable {
          // Skip the remaining blocks once a task has failed.
          if (hasFailed()) {
            return;
          }
          // NOTE: This rethrows the exception of the prepare task, if any.
          auto appendBlock = prepared.get();
          ad_utility::Timer timer{ad_utility::Timer::Started};
          std::move(appendBlock)();
          appendBusyMs_ += timer.msecs().count();
        }));
  }

  // Whether one of the tasks has thrown an exception that `finish()` has not
  // rethrown yet. The caller should then stop pushing blocks.
  bool hasFailed() const { return exceptionCollector_.hasException(); }

  // Wait until all the blocks are appended, and rethrow the first exception
  // that one of the tasks threw, if any. After this, no more blocks may be
  // pushed. The `writer` is not finished.
  void finish() {
    for (auto& queue : appendQueues_) {
      queue->finish();
    }
    exceptionCollector_.rethrowIfException();
  }

  // The number of blocks pushed so far, and the total time that the tasks
  // have spent preparing and appending them, in milliseconds.
  size_t numBlocks() const { return numBlocks_; }
  uint64_t prepareBusyMs() const { return prepareBusyMs_; }
  uint64_t appendBusyMs() const { return appendBusyMs_; }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_ASYNCBLOCKWRITER_H
