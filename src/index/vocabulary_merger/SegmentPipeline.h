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

#include <atomic>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
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
#include "util/RegexSet.h"
#include "util/Timer.h"

// The stages behind the parallel merge of the partial vocabularies (see the
// comment above `mergeVocabulary` in `index/VocabularyMerger.h`). This is not
// part of the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// Turn the blocks of merged words, which arrive in the order of the vocabulary,
// into the merged vocabulary and the partial ID maps:
//
// 1. The blocks are cut into segments (see `Segment`) of at least
//    `numWordsPerSegment` words, each of which is built by a task on the global
//    thread pool (see `buildSegment`). A cut between two blocks is only
//    allowed if the last word of the one differs from the first word of the
//    other, so that no word spans two segments.
// 2. The finished segments are committed in order (see `SegmentCommitter`),
//    which hands their blocks of words to the `AsyncBlockWriter` and their ID
//    map entries to the `IdMapWriters`. At most `maxSegmentsInFlight` segments
//    are being built or waiting to be committed at the same time; a `push`
//    that would exceed this commits the oldest segment first.
//
// NOTE: All the member functions are called by a single thread, the one that
// runs `mergeVocabulary`, which must not be a thread of the global executor.
CPP_template(typename W)(requires WordComparator<W>) class SegmentPipeline {
 private:
  ParallelWordWriterBase& writer_;
  const ad_utility::RegexSet& blankNodeIriRegexes_;
  size_t numPartialVocabularies_;
  const W& comparator_;
  size_t maxSegmentsInFlight_;
  size_t numWordsPerSegment_;
  IdMapWriters idMapWriters_;
  AsyncBlockWriter blockWriter_;
  SegmentCommitter committer_;
  // The segments that are being built or waiting to be committed, in order.
  std::deque<std::future<std::shared_ptr<const Segment>>> segments_;
  // The blocks of the segment that is being cut, and their number of words.
  std::vector<MergedBlock> currentBlocks_;
  size_t currentNumWords_ = 0;
  // The time since the construction, the time spent inside `push` and
  // `finish` (the rest is spent waiting for merged blocks), and the total time
  // that the tasks have spent building the segments.
  ad_utility::Timer totalTimer_{ad_utility::Timer::Started};
  ad_utility::Timer commitTimer_{ad_utility::Timer::Stopped};
  std::atomic<uint64_t> segmentBusyMs_{0};

 public:
  // Construct from the `writer` of the vocabulary (which has to stay alive,
  // and which is not finished by this class), the files of the partial ID maps
  // (see `IdMapWriters`), and the arguments of `buildSegment` and
  // `mergeVocabulary` that the segments need.
  SegmentPipeline(ParallelWordWriterBase& writer,
                  ad_utility::InputRangeTypeErased<std::string> idMapFilenames,
                  size_t numPartialVocabularies,
                  const ad_utility::RegexSet& blankNodeIriRegexes,
                  const W& comparator, size_t maxSegmentsInFlight,
                  size_t numWordsPerSegment = VOCAB_MERGER_SEGMENT_NUM_WORDS)
      : writer_{writer},
        blankNodeIriRegexes_{blankNodeIriRegexes},
        numPartialVocabularies_{numPartialVocabularies},
        comparator_{comparator},
        maxSegmentsInFlight_{maxSegmentsInFlight},
        numWordsPerSegment_{numWordsPerSegment},
        idMapWriters_{std::move(idMapFilenames),
                      VOCAB_MERGER_NUM_ID_MAP_WRITER_THREADS},
        blockWriter_{writer},
        committer_{writer, [this](uint8_t sub, BlockPieces block) {
                     blockWriter_.push(sub, std::move(block));
                   }} {
    AD_CONTRACT_CHECK(maxSegmentsInFlight_ > 0);
  }

  // Push the next `block` of merged words. Do nothing if a stage has failed
  // (the exception is rethrown by `finish()`).
  void push(MergedBlock block) {
    commitTimer_.cont();
    absl::Cleanup stopTimer = [this]() { commitTimer_.stop(); };
    if (hasFailed()) {
      return;
    }
    if (!block.empty()) {
      bool canCutHere = currentNumWords_ >= numWordsPerSegment_ &&
                        currentBlocks_.back().back().iriOrLiteral() !=
                            block.front().iriOrLiteral();
      if (canCutHere) {
        postSegment();
      }
      currentNumWords_ += block.size();
      currentBlocks_.push_back(std::move(block));
    }
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
    postSegment();
    while (!segments_.empty()) {
      commitFinishedSegments(true);
    }
    VocabularyMetaData metaData = committer_.finish();
    commitTimer_.stop();
    AD_LOG_INFO << "Time spent by the thread behind the merge: waiting for "
                   "merged blocks "
                << (totalTimer_.msecs() - commitTimer_.msecs())
                << ", committing the segments " << commitTimer_.msecs()
                << "; building the segments " << segmentBusyMs_ << " ms"
                << std::endl;
    blockWriter_.finish();
    idMapWriters_.finish();
    AD_LOG_INFO << committer_.progressBar().getFinalProgressString()
                << std::flush;
    AD_LOG_INFO << "Time spent by the stages behind the vocabulary merge: "
                   "preparing "
                << blockWriter_.numBlocks() << " blocks "
                << blockWriter_.prepareBusyMs() << " ms, appending them "
                << blockWriter_.appendBusyMs() << " ms, writing the ID maps "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       idMapWriters_.busyTime())
                       .count()
                << " ms" << std::endl;
    return metaData;
  }

 private:
  // Post the task that builds the segment of the `currentBlocks_` and start
  // the next segment. Do nothing if there are no blocks.
  void postSegment() {
    if (currentBlocks_.empty()) {
      return;
    }
    segments_.push_back(boost::asio::post(
        ad_utility::globalExecutor(),
        boost::asio::use_future(
            [this, blocks = std::move(currentBlocks_)]() mutable {
              ad_utility::Timer timer{ad_utility::Timer::Started};
              auto segment = std::make_shared<const Segment>(
                  buildSegment(std::move(blocks), writer_, blankNodeIriRegexes_,
                               numPartialVocabularies_, comparator_));
              segmentBusyMs_ += timer.msecs().count();
              return segment;
            })));
    currentBlocks_.clear();
    currentNumWords_ = 0;
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
