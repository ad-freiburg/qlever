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
#include <limits>
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
#include "index/vocabulary_merger/MemoryBudget.h"
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

// The stages behind the parallel merge of the partial vocabularies (see the
// comment above `mergeVocabulary` in `index/VocabularyMerger.h`). This is not
// part of the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// Cut the blocks of merged words, which arrive in the order of the vocabulary,
// into segments (see `Segment`) of at least `numWordsPerSegment` words or at
// least `numBytesPerSegment` of merged blocks (see `MergedBlock::memorySize`),
// whichever comes first. A cut between two blocks is only allowed if the last
// word of the one differs from the first word of the other, so that no word
// spans two segments. For each segment, the cutter returns the task that
// builds it (see `buildSegment`), which the caller can run on any thread.
//
// NOTE: The tasks refer to this object, so it has to stay alive until all of
// them have completed.
CPP_template(typename W)(requires WordComparator<W>) class SegmentCutter {
 public:
  // The task that builds a segment.
  using SegmentTask = absl::AnyInvocable<Segment()>;

 private:
  const ParallelWordWriterBase& writer_;
  const ad_utility::RegexSet& blankNodeIriRegexes_;
  size_t numPartialVocabularies_;
  const W& comparator_;
  size_t numWordsPerSegment_;
  size_t numBytesPerSegment_;
  // The blocks of the segment that is being cut, their number of words, and
  // their memory.
  std::vector<MergedBlock> currentBlocks_;
  size_t currentNumWords_ = 0;
  size_t currentNumBytes_ = 0;
  // The total time that the tasks have spent building the segments.
  ad_utility::timer::ThreadSafeTimer segmentBusyTime_;

 public:
  // Construct from the arguments of `buildSegment`, which all have to stay
  // alive until all the tasks have completed, and the minimal number of words
  // and of bytes per segment (see above). The number of words has to be at
  // least one.
  SegmentCutter(const ParallelWordWriterBase& writer,
                const ad_utility::RegexSet& blankNodeIriRegexes,
                size_t numPartialVocabularies, const W& comparator,
                size_t numWordsPerSegment,
                size_t numBytesPerSegment = std::numeric_limits<size_t>::max())
      : writer_{writer},
        blankNodeIriRegexes_{blankNodeIriRegexes},
        numPartialVocabularies_{numPartialVocabularies},
        comparator_{comparator},
        numWordsPerSegment_{numWordsPerSegment},
        numBytesPerSegment_{numBytesPerSegment} {
    AD_CONTRACT_CHECK(numWordsPerSegment_ > 0);
  }

  // Add the next `block` of merged words. If this completes a segment (which
  // then consists of the blocks before the `block`), return the task that
  // builds it.
  std::optional<SegmentTask> push(MergedBlock block) {
    if (block.empty()) {
      return std::nullopt;
    }
    std::optional<SegmentTask> task;
    bool isLargeEnough = currentNumWords_ >= numWordsPerSegment_ ||
                         currentNumBytes_ >= numBytesPerSegment_;
    bool canCutHere =
        isLargeEnough && currentBlocks_.back().back().iriOrLiteral() !=
                             block.front().iriOrLiteral();
    if (canCutHere) {
      task = cutSegment();
    }
    currentNumWords_ += block.size();
    currentNumBytes_ += block.memorySize().getBytes();
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
      return buildSegment(std::move(blocks), writer_, blankNodeIriRegexes_,
                          numPartialVocabularies_, comparator_);
    };
    currentBlocks_.clear();
    currentNumWords_ = 0;
    currentNumBytes_ = 0;
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
// The memory of all the data behind the merge (the merged blocks that are
// cut, the segments that are being built, and the words and ID map entries of
// the committed segments until they are written) is accounted for in a
// `MemoryBudget` of `memoryLimit`. Before a `push` adds a block, it waits until
// the block fits into the budget, see `waitForMemory`. The words and the ID map
// entries of a committed segment are held separately, so that each is released
// as soon as its writer is done with it.
//
// NOTE 1: All the member functions are called by a single thread, the one that
// runs `mergeVocabulary`, which must not be a thread of the global executor.
//
// NOTE 2: The pipeline is neither copyable nor movable, because the tasks and
// the sink of the `committer_` refer to it.
CPP_template(typename W)(requires WordComparator<W>) class SegmentPipeline
    : public ad_utility::NoCopyNoMove {
 private:
  using Reservation = MemoryBudget::Reservation;
  // A segment that is being built or waiting to be committed, and the memory
  // that is reserved for it (see `push`).
  struct InFlightSegment {
    std::future<Segment> segment_;
    Reservation reservation_;
  };
  // Data that is shared by several stages, together with the reservation of
  // its memory, which is released after the data has been destroyed (the
  // members are destroyed in reverse order), see `makeShared`.
  template <typename T>
  struct WithReservation {
    Reservation reservation_;
    T value_;
  };

  // NOTE: The budget is declared first, because all the other members (and
  // the tasks that they wait for in their destructors) hold reservations.
  MemoryBudget memoryBudget_;
  size_t maxSegmentsInFlight_;
  SegmentCutter<W> cutter_;
  IdMapWriters idMapWriters_;
  AsyncBlockWriter blockWriter_;
  SegmentCommitter committer_;
  // The memory reserved for the blocks of the segment that the `cutter_` is
  // currently cutting.
  Reservation currentSegmentReservation_;
  // The segments that are being built or waiting to be committed, in order.
  std::deque<InFlightSegment> segments_;
  // The time since the construction, and the time spent inside `push` and
  // `finish` (the rest is spent waiting for merged blocks).
  ad_utility::Timer totalTimer_{ad_utility::Timer::Started};
  ad_utility::Timer commitTimer_{ad_utility::Timer::Stopped};

 public:
  // Construct from the `writer` of the vocabulary (which has to stay alive,
  // and which is not finished by this class), the files of the partial ID maps
  // (see `IdMapWriters`), the arguments of `buildSegment` and
  // `mergeVocabulary` that the segments need, and the `memoryLimit` of the
  // data behind the merge (see above).
  SegmentPipeline(ParallelWordWriterBase& writer,
                  ad_utility::InputRangeTypeErased<std::string> idMapFilenames,
                  size_t numPartialVocabularies,
                  const ad_utility::RegexSet& blankNodeIriRegexes,
                  const W& comparator, size_t maxSegmentsInFlight,
                  ad_utility::MemorySize memoryLimit,
                  size_t numWordsPerSegment = VOCAB_MERGER_SEGMENT_NUM_WORDS)
      : memoryBudget_{memoryLimit},
        maxSegmentsInFlight_{maxSegmentsInFlight},
        cutter_{writer,
                blankNodeIriRegexes,
                numPartialVocabularies,
                comparator,
                numWordsPerSegment,
                numBytesPerSegment(memoryLimit, maxSegmentsInFlight)},
        idMapWriters_{std::move(idMapFilenames),
                      VOCAB_MERGER_NUM_ID_MAP_WRITER_GROUPS},
        blockWriter_{writer, &memoryBudget_},
        committer_{writer,
                   [this](uint8_t sub, BlockPieces block) {
                     blockWriter_.push(sub, std::move(block));
                   }},
        currentSegmentReservation_{
            memoryBudget_.reserve(ad_utility::MemorySize::bytes(0))} {
    AD_CONTRACT_CHECK(maxSegmentsInFlight_ > 0);
  }

  // The memory of the merged blocks after which a segment is cut (see
  // `SegmentCutter`), given the `memoryLimit` and the `maxSegmentsInFlight`:
  // a segment that is being built takes about twice the memory of its blocks
  // (the blocks and the segment), and its words and ID map entries (about the
  // memory of its blocks again) stay until they are written, so a quarter of
  // the share of each segment of the budget. The minimum avoids tiny segments
  // (with a tiny budget, `push` then waits until the segments are written).
  static size_t numBytesPerSegment(ad_utility::MemorySize memoryLimit,
                                   size_t maxSegmentsInFlight) {
    return std::max(
        memoryLimit.getBytes() / (4 * std::max<size_t>(maxSegmentsInFlight, 1)),
        VOCAB_MERGER_MIN_SEGMENT_MEMORY.getBytes());
  }

  // Wait for the tasks that are still building segments. This only happens if
  // an exception was thrown before `finish()` committed all the segments, and
  // is required because the tasks use the `cutter_` and the arguments of the
  // constructor.
  //
  // NOTE 1: A future whose `get()` threw (the segment that failed) is no
  // longer valid, and its task has completed.
  //
  // NOTE 2: `wait()` never rethrows the exception that is stored in a future,
  // so this destructor can't throw and needs no `noexcept(false)`.
  ~SegmentPipeline() {
    for (auto& segment : segments_) {
      if (segment.segment_.valid()) {
        segment.segment_.wait();
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
    // The block is accounted for twice: for itself, and for the segment that
    // is built from it (see `numBytesPerSegment`). The estimate is replaced by
    // the actual memory of the segment when it is committed.
    const size_t numBytes = 2 * block.memorySize().getBytes();
    waitForMemory(numBytes);
    postSegment(cutter_.push(std::move(block)));
    currentSegmentReservation_.grow(numBytes);
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
        << "; peak memory of the data behind the merge "
        << memoryBudget_.peak().asString() << " (limit "
        << memoryBudget_.limit().asString() << ")" << std::endl;
    return metaData;
  }

 private:
  // Post the `task` that builds a segment (if any) onto the global thread
  // pool.
  void postSegment(std::optional<typename SegmentCutter<W>::SegmentTask> task) {
    if (!task.has_value()) {
      return;
    }
    segments_.push_back(InFlightSegment{
        boost::asio::post(ad_utility::globalExecutor(),
                          boost::asio::use_future(std::move(task.value()))),
        std::exchange(
            currentSegmentReservation_,
            memoryBudget_.reserve(ad_utility::MemorySize::bytes(0)))});
  }

  // Block until `numBytes` more fit into the memory budget. While segments are
  // in flight, commit the oldest one (which replaces the estimate of its
  // memory by the actual one, and hands its data to the writers, which release
  // it). Otherwise wait for the writers to release memory, but only as long as
  // they have pending work: the remaining memory is then held by this thread
  // (the blocks of the current segment and the open blocks of the
  // `committer_`), and the budget is exceeded.
  void waitForMemory(size_t numBytes) {
    const auto size = ad_utility::MemorySize::bytes(numBytes);
    while (!memoryBudget_.waitUntilFits(size, [this]() {
      return !segments_.empty() || (blockWriter_.numPendingBlocks() == 0 &&
                                    idMapWriters_.numPendingTasks() == 0);
    })) {
      if (segments_.empty()) {
        return;
      }
      commitFinishedSegments(true);
    }
  }

  // Return a `shared_ptr` to the `value`, whose memory `size` is accounted
  // for in the budget until the last copy of the pointer is gone.
  template <typename T>
  std::shared_ptr<const T> makeShared(T value, ad_utility::MemorySize size) {
    auto holder = std::make_shared<WithReservation<T>>(
        WithReservation<T>{memoryBudget_.reserve(size), std::move(value)});
    return {holder, &holder->value_};
  }

  // Commit the segments that are done, in order, and at least one segment if
  // `waitForOne` is set. Rethrow the exception of a segment whose building
  // failed.
  void commitFinishedSegments(bool waitForOne) {
    while (!segments_.empty()) {
      auto& next = segments_.front();
      if (!waitForOne && next.segment_.wait_for(std::chrono::seconds(0)) !=
                             std::future_status::ready) {
        break;
      }
      Segment segment = next.segment_.get();
      // The estimate is released after the actual memory is reserved below.
      Reservation estimate = std::move(next.reservation_);
      segments_.pop_front();
      waitForOne = false;
      // The ID map entries and the words are held (and released) separately.
      auto entriesSize = memorySizeOf(segment.idMapEntries_);
      auto entries = makeShared(std::move(segment.idMapEntries_), entriesSize);
      auto wordsSize = segment.memorySizeOfWords();
      auto words = makeShared(std::move(segment), wordsSize);
      IdConverterForSegment converter = committer_.commit(words);
      idMapWriters_.write(std::move(entries), converter);
    }
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTPIPELINE_H
