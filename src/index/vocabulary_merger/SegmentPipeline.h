// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTPIPELINE_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTPIPELINE_H

#include <absl/cleanup/cleanup.h>
#include <absl/functional/any_invocable.h>

#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "index/vocabulary_merger/AsyncBlockWriter.h"
#include "index/vocabulary_merger/Concepts.h"
#include "index/vocabulary_merger/IdMapWriters.h"
#include "index/vocabulary_merger/MergeBlock.h"
#include "index/vocabulary_merger/Segment.h"
#include "index/vocabulary_merger/SegmentCommitter.h"
#include "index/vocabulary_merger/VocabularyMetaData.h"
#include "util/Exception.h"
#include "util/GlobalExecutor.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/NoCopyNoMove.h"
#include "util/ParseableDuration.h"
#include "util/RegexSet.h"
#include "util/Timer.h"

// The stages behind the parallel merge of the partial vocabularies (see
// `mergeVocabulary` in `index/VocabularyMerger.h`, whose current pipeline a
// follow-up PR replaces with the `SegmentPipeline` below). This is not part of
// the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// Cut the blocks of merged words, which arrive in the order of the vocabulary,
// into segments (see `Segment`) of at least `numWordsPerSegment` words. A cut
// between two blocks is only allowed if the last word of the one differs from
// the first word of the other, so that no word spans two segments. For each
// segment, the cutter returns the task that builds it (see `buildSegment`),
// which the caller can run on any thread.
//
// NOTE: The tasks refer to this object, so it has to stay alive until all of
// them have completed.
CPP_template(typename W)(requires WordComparator<W>) class SegmentCutter {
 public:
  // The task that builds a segment.
  using SegmentTask = absl::AnyInvocable<std::shared_ptr<const Segment>()>;

 private:
  const ParallelWordWriterBase& writer_;
  const ad_utility::RegexSet& blankNodeIriRegexes_;
  size_t numPartialVocabularies_;
  const W& comparator_;
  size_t numWordsPerSegment_;
  // The blocks of the segment that is being cut, and their number of words.
  std::vector<MergedBlock> currentBlocks_;
  size_t currentNumWords_ = 0;
  // The total time that the tasks have spent building the segments.
  ad_utility::timer::ThreadSafeTimer segmentBusyTime_;

 public:
  // Construct from the arguments of `buildSegment`, which all have to stay
  // alive until all the tasks have completed, and the minimal number of words
  // per segment.
  SegmentCutter(const ParallelWordWriterBase& writer,
                const ad_utility::RegexSet& blankNodeIriRegexes,
                size_t numPartialVocabularies, const W& comparator,
                size_t numWordsPerSegment)
      : writer_{writer},
        blankNodeIriRegexes_{blankNodeIriRegexes},
        numPartialVocabularies_{numPartialVocabularies},
        comparator_{comparator},
        numWordsPerSegment_{numWordsPerSegment} {}

  // Add the next `block` of merged words. If this completes a segment (which
  // then consists of the blocks before the `block`), return the task that
  // builds it.
  std::optional<SegmentTask> push(MergedBlock block) {
    if (block.empty()) {
      return std::nullopt;
    }
    std::optional<SegmentTask> task;
    bool canCutHere = currentNumWords_ >= numWordsPerSegment_ &&
                      currentBlocks_.back().back().iriOrLiteral() !=
                          block.front().iriOrLiteral();
    if (canCutHere) {
      task = cutSegment();
    }
    currentNumWords_ += block.size();
    currentBlocks_.push_back(std::move(block));
    return task;
  }

  // Return the task that builds the last segment, which consists of the
  // remaining blocks, or `std::nullopt` if there are none. After this, no more
  // blocks may be pushed.
  std::optional<SegmentTask> finish() {
    if (currentBlocks_.empty()) {
      return std::nullopt;
    }
    return cutSegment();
  }

  // The total time that the tasks have spent building the segments.
  ad_utility::ParseableDuration<std::chrono::milliseconds> segmentBusyTime()
      const {
    return segmentBusyTime_.msecs();
  }

 private:
  // Return the task that builds the segment of the `currentBlocks_`, and start
  // the next segment.
  SegmentTask cutSegment() {
    SegmentTask task = [this, blocks = std::move(currentBlocks_)]() mutable {
      auto measurement = segmentBusyTime_.startMeasurement();
      return std::make_shared<const Segment>(
          buildSegment(std::move(blocks), writer_, blankNodeIriRegexes_,
                       numPartialVocabularies_, comparator_));
    };
    currentBlocks_.clear();
    currentNumWords_ = 0;
    return task;
  }
};

// Turn the blocks of merged words, which arrive in the order of the vocabulary,
// into the merged vocabulary and the partial ID maps:
//
// 1. The blocks are cut into segments by a `SegmentCutter` (see above), and
//    each segment is built by a task on the global thread pool.
// 2. The finished segments are committed in order (see `SegmentCommitter`),
//    which hands their blocks of words to the `AsyncBlockWriter` and their ID
//    map entries to the `IdMapWriters`. At most `maxSegmentsInFlight` segments
//    are being built or waiting to be committed at the same time; a `push`
//    that would exceed this commits the oldest segment first.
//
// NOTE: All the member functions are called by a single thread (the one that
// receives the merged blocks), which must not be a thread of the global
// executor.
//
// NOTE: The pipeline is neither copyable nor movable, because the tasks and
// the sink of the `committer_` refer to it.
CPP_template(typename W)(requires WordComparator<W>) class SegmentPipeline
    : public ad_utility::NoCopyNoMove {
 private:
  size_t maxSegmentsInFlight_;
  SegmentCutter<W> cutter_;
  IdMapWriters idMapWriters_;
  AsyncBlockWriter blockWriter_;
  SegmentCommitter committer_;
  // The segments that are being built or waiting to be committed, in order.
  std::deque<std::future<std::shared_ptr<const Segment>>> segments_;
  // The time since the construction, and the time spent inside `push` and
  // `finish` (the rest is spent waiting for merged blocks).
  ad_utility::Timer totalTimer_{ad_utility::Timer::Started};
  ad_utility::Timer commitTimer_{ad_utility::Timer::Stopped};

 public:
  // Construct from the `writer` of the vocabulary (which has to stay alive,
  // and which is not finished by this class), the files of the partial ID maps
  // (see `IdMapWriters`), the arguments of `buildSegment` that the segments
  // need, and the bounds on the segments.
  SegmentPipeline(ParallelWordWriterBase& writer,
                  ad_utility::InputRangeTypeErased<std::string> idMapFilenames,
                  size_t numPartialVocabularies,
                  const ad_utility::RegexSet& blankNodeIriRegexes,
                  const W& comparator, size_t maxSegmentsInFlight,
                  size_t numWordsPerSegment = VOCAB_MERGER_SEGMENT_NUM_WORDS)
      : maxSegmentsInFlight_{maxSegmentsInFlight},
        cutter_{writer, blankNodeIriRegexes, numPartialVocabularies, comparator,
                numWordsPerSegment},
        idMapWriters_{std::move(idMapFilenames),
                      VOCAB_MERGER_NUM_ID_MAP_WRITER_THREADS},
        blockWriter_{writer},
        committer_{writer, [this](uint8_t sub, BlockPieces block) {
                     blockWriter_.push(sub, std::move(block));
                   }} {
    AD_CONTRACT_CHECK(maxSegmentsInFlight_ > 0);
  }

  // Wait for the tasks that are still building segments. This only happens if
  // an exception was thrown before `finish()` committed all the segments, and
  // is required because the tasks use the `cutter_` and the arguments of the
  // constructor. NOTE: A future whose `get()` threw (the segment that failed)
  // is no longer valid, and its task has completed. NOTE: `wait()` never
  // rethrows the exception that is stored in a future, so this destructor
  // can't throw and needs no `noexcept(false)`.
  ~SegmentPipeline() {
    for (auto& segment : segments_) {
      if (segment.valid()) {
        segment.wait();
      }
    }
  }

  // Push the next `block` of merged words. Do nothing if a stage has failed
  // (the exception is rethrown by `finish()`).
  void push(MergedBlock block) {
    commitTimer_.cont();
    absl::Cleanup stopTimer = [this]() { commitTimer_.stop(); };
    if (hasFailed()) {
      return;
    }
    postSegment(cutter_.push(std::move(block)));
    commitFinishedSegments(segments_.size() >= maxSegmentsInFlight_);
  }

  // Whether one of the stages has failed (and `finish()` has not rethrown the
  // exception yet). The caller should then stop pushing blocks.
  bool hasFailed() const {
    return blockWriter_.hasFailed() || idMapWriters_.hasFailed();
  }

  // Build and commit the remaining segments, wait until all the blocks and ID
  // map entries are written, and return the metadata of the merged vocabulary.
  // Rethrow the first exception that one of the stages threw, if any. After
  // this, no more blocks may be pushed.
  VocabularyMetaData finish() {
    commitTimer_.cont();
    postSegment(cutter_.finish());
    while (!segments_.empty()) {
      commitFinishedSegments(true);
    }
    VocabularyMetaData metaData = committer_.finish();
    commitTimer_.stop();
    using Milliseconds =
        ad_utility::ParseableDuration<std::chrono::milliseconds>;
    AD_LOG_INFO << "Time spent by the thread behind the merge: waiting for "
                   "merged blocks "
                << Milliseconds{totalTimer_.msecs() - commitTimer_.msecs()}
                << ", committing the segments "
                << Milliseconds{commitTimer_.msecs()}
                << "; building the segments " << cutter_.segmentBusyTime()
                << std::endl;
    blockWriter_.finish();
    idMapWriters_.finish();
    AD_LOG_INFO << committer_.progressBar().getFinalProgressString()
                << std::flush;
    AD_LOG_INFO
        << "Time spent by the stages behind the vocabulary merge: "
           "preparing "
        << blockWriter_.numBlocks() << " blocks "
        << blockWriter_.prepareBusyTime() << ", appending them "
        << blockWriter_.appendBusyTime() << ", writing the ID maps "
        << Milliseconds{std::chrono::duration_cast<std::chrono::milliseconds>(
               idMapWriters_.busyTime())}
        << std::endl;
    return metaData;
  }

 private:
  // Post the `task` that builds a segment (if any) onto the global thread
  // pool.
  void postSegment(std::optional<typename SegmentCutter<W>::SegmentTask> task) {
    if (!task.has_value()) {
      return;
    }
    segments_.push_back(
        boost::asio::post(ad_utility::globalExecutor(),
                          boost::asio::use_future(std::move(task.value()))));
  }

  // Commit the segments that are done, in order, and at least one segment if
  // `waitForOne` is set. Rethrow the exception of a segment whose building
  // failed.
  void commitFinishedSegments(bool waitForOne) {
    while (!segments_.empty()) {
      auto& next = segments_.front();
      if (!waitForOne &&
          next.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        break;
      }
      std::shared_ptr<const Segment> segment = next.get();
      segments_.pop_front();
      waitForOne = false;
      IdConverterForSegment converter = committer_.commit(segment);
      idMapWriters_.write(std::move(segment), converter);
    }
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTPIPELINE_H
