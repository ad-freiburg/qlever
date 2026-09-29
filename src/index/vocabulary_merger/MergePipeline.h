// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEPIPELINE_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEPIPELINE_H

#include <atomic>
#include <deque>
#include <exception>
#include <future>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/IdMapBatch.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "index/vocabulary_merger/VocabularyWriter.h"
#include "index/vocabulary_merger/WordBatch.h"
#include "util/GlobalExecutor.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/PostAndGetFuture.h"
#include "util/RegexSet.h"
#include "util/TaskQueue.h"
#include "util/Timer.h"

// The asynchronous part of the merging pipeline of the vocabulary merger (see
// the comment above `mergeVocabulary` in `index/VocabularyMerger.h`), which is
// not part of the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// The stages of the merging pipeline that run asynchronously to the merging
// thread (stages 2 to 4 in the comment above `mergeVocabulary`). Use the
// `VocabularyMergePipeline` alias below; the type of the third stage is a
// template parameter only so that the tests can inject a writer that fails
// (the real `IdMapBatchWriter` cannot, see `runAndCatchException`).
//
// NOTE: Each of the queues has exactly one worker thread, so the batches are
// processed in exactly the order in which the merging thread creates them, and
// the state of the individual stages requires no further synchronization.
template <typename IdMapBatchWriterT>
class VocabularyMergePipelineImpl {
 private:
  // NOTE: The order of the following declarations is important, because the
  // members are destroyed in the reverse order of their declaration, and the
  // destructor of a queue blocks until all its pending tasks have been run. The
  // rule is that a queue has to be declared after every member that its tasks
  // use, and after every queue that its tasks push to: the tasks of the
  // `wordWriterQueue_` use the `vocabularyWriter_` and push to both of the
  // other queues, and the tasks of the `idMapWriterQueue_` use the
  // `idMapBatchWriter_`.
  IdMapBatchWriterT idMapBatchWriter_;
  VocabularyWriter vocabularyWriter_;
  // The first exception that one of the stages threw, if any, and a flag that
  // says whether that has happened. An exception must not escape the thread of
  // a queue (that would terminate the process), so it is stored here and
  // rethrown by `finish()`. The flag is checked by all the threads (see
  // `hasFailed()`), so that the merging stops early and the batches that are
  // still queued are skipped; the `exception_ptr` itself is only read after all
  // the queues have been finished.
  std::atomic<bool> hasFailed_{false};
  std::exception_ptr exception_;
  // Only the first exception is stored, and the stages run on different
  // threads, so the storing has to be synchronized.
  std::mutex exceptionMutex_;
  ad_utility::TaskQueue<false> idMapWriterQueue_{
      VOCAB_MERGER_WORD_BATCH_QUEUE_SIZE, 1, "Writing the ID maps"};
  // The destruction of the merged words of the batches runs on the global
  // thread pool (see `util/GlobalExecutor.h`), one task per batch; these are
  // the tasks that are not known to be done yet, oldest first. Freeing the
  // strings of a batch is slow enough (and slower still on a thread other than
  // the ones that allocated them) that a single thread of its own fell behind.
  std::deque<std::future<void>> pendingDestructions_;
  std::mutex pendingDestructionsMutex_;
  ad_utility::TaskQueue<false> wordWriterQueue_{
      VOCAB_MERGER_WORD_BATCH_QUEUE_SIZE, 1, "Writing the merged vocabulary"};
  // The time that each of the three stages has spent working (as opposed to
  // waiting for its queue), in milliseconds, for the log line of `finish()`.
  std::atomic<uint64_t> writerBusyMs_{0};
  std::atomic<uint64_t> idMapBusyMs_{0};
  std::atomic<uint64_t> destructionBusyMs_{0};

 public:
  // Create the pipeline. The `idMapFilenames` are the files of the partial ID
  // maps, one per partial vocabulary and in the order of the partial
  // vocabularies (see `IdMapBatchWriter`).
  explicit VocabularyMergePipelineImpl(
      ad_utility::InputRangeTypeErased<std::string> idMapFilenames)
      : idMapBatchWriter_{std::move(idMapFilenames)} {}

  // Asynchronously process a single `batch` of merged words: write its
  // distinct words to the vocabulary (via the `wordCallback` and the
  // `blankNodeIriRegexes`), then write its index mappings and destroy the
  // merged words that it was created from. Block if the pipeline is busy.
  //
  // NOTE: The `wordCallback` and the `blankNodeIriRegexes` are captured *by
  // reference* into the asynchronous task, so both of them have to stay alive
  // (and must not be modified from the outside) until `finish()` has
  // returned.
  CPP_template_2(typename C)(requires WordCallback<C>) void push(
      WordBatch batch, C& wordCallback,
      const ad_utility::RegexSet& blankNodeIriRegexes) {
    wordWriterQueue_.push([this, batch = std::move(batch), &wordCallback,
                           &blankNodeIriRegexes]() mutable {
      runAndCatchException([this, &batch, &wordCallback,
                            &blankNodeIriRegexes]() {
        ad_utility::Timer timer{ad_utility::Timer::Started};
        auto idMapBatch = vocabularyWriter_.writeWordsToVocabulary(
            batch.uniqueWords_, std::move(batch.localIdxMappings_),
            wordCallback, blankNodeIriRegexes);
        writerBusyMs_ += timer.msecs().count();

        // The merged words are no longer needed. Their destruction (which
        // involves freeing one string per word) is expensive enough to be done
        // by yet another thread. NOTE: The `clear()` is the actual work of this
        // task; it happens on the queue's thread, as does the destruction of
        // the (then empty) buffers.
        destroyOnPool(std::move(batch.mergedWordBuffers_));

        idMapWriterQueue_.push([this, idMapBatch = std::move(idMapBatch)]() {
          runAndCatchException([this, &idMapBatch]() {
            ad_utility::Timer timer{ad_utility::Timer::Started};
            idMapBatchWriter_.writeBatch(idMapBatch);
            idMapBusyMs_ += timer.msecs().count();
          });
        });
      });
    });
  }

  // Hand the destruction of the `buffers` of a batch to the global thread
  // pool, and first wait for the oldest pending destructions if too many of
  // them are in flight (they hold the memory of their batches).
  template <typename Buffers>
  void destroyOnPool(Buffers buffers) {
    waitForPendingDestructions(VOCAB_MERGER_WORD_BATCH_QUEUE_SIZE);
    auto future = ad_utility::postAndGetFuture(
        ad_utility::globalExecutor(),
        [this, buffers = std::move(buffers)]() mutable {
          ad_utility::Timer timer{ad_utility::Timer::Started};
          buffers.clear();
          destructionBusyMs_ += timer.msecs().count();
        });
    std::lock_guard lock{pendingDestructionsMutex_};
    pendingDestructions_.push_back(std::move(future));
  }

  // Wait until at most `maxNumPending` destructions are pending.
  void waitForPendingDestructions(size_t maxNumPending) {
    std::lock_guard lock{pendingDestructionsMutex_};
    while (pendingDestructions_.size() > maxNumPending) {
      pendingDestructions_.front().get();
      pendingDestructions_.pop_front();
    }
  }

  // Whether one of the stages has thrown an exception. The caller should then
  // stop pushing batches; the exception is rethrown by `finish()`.
  bool hasFailed() const { return hasFailed_; }

  // Wait until all the batches that were pushed have been processed
  // completely, close the ID maps, and return the metadata of the merged
  // vocabulary. Rethrow the first exception that one of the stages threw, if
  // any. After this, no more batches may be pushed.
  VocabularyMetaData finish() {
    // NOTE: The order is important, see the declaration of the members.
    wordWriterQueue_.finish();
    waitForPendingDestructions(0);
    idMapWriterQueue_.finish();
    // Propagate an exception from one of the stages to the caller. NOTE: All
    // the queues have been joined, so reading `exception_` here is safe. The ID
    // maps are deliberately not finished on this path (their destructors do
    // that, and they do not throw), so that a failure of that cleanup cannot
    // hide the original exception.
    if (exception_) {
      std::rethrow_exception(exception_);
    }
    idMapBatchWriter_.finish();
    vocabularyWriter_.logFinalProgress();
    AD_LOG_INFO << "Time spent by the stages of the vocabulary merge: writing "
                   "the vocabulary "
                << writerBusyMs_ << " ms, writing the ID maps " << idMapBusyMs_
                << " ms, destroying the merged words " << destructionBusyMs_
                << " ms" << std::endl;
    return std::move(vocabularyWriter_.metaData());
  }

 private:
  // Run the `task` on the thread of one of the queues, and store the exception
  // that it throws (if any) instead of letting it escape that thread (see
  // `exception_`). Once a batch has failed, the remaining batches are skipped,
  // because their words could no longer be written consistently anyway.
  //
  // NOTE: Of the two stages that are wrapped in this, only the writing of the
  // words can currently fail (via the `wordCallback`); the writing of the ID
  // maps cannot, because a failed write to a file is silently ignored
  // (`FileWriteSerializer::serializeBytes` in
  // `util/Serializer/FileSerializer.h` discards the number of bytes that
  // `ad_utility::File::write` returns). The wrapping of the latter is
  // deliberate nevertheless, so that a future ID map writer that does report
  // its errors doesn't terminate the process.
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

// The pipeline as it is used by `mergeVocabulary`.
using VocabularyMergePipeline = VocabularyMergePipelineImpl<IdMapBatchWriter>;
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_MERGEPIPELINE_H
