// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPWRITERS_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPWRITERS_H

#include <algorithm>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/Segment.h"
#include "util/ExceptionHandling.h"
#include "util/GlobalExecutor.h"
#include "util/Iterators.h"
#include "util/Math.h"
#include "util/TaskQueueOnExecutor.h"
#include "util/Timer.h"

// The writers of the partial ID maps of the vocabulary merger (see the comment
// above `mergeVocabulary` in `index/VocabularyMerger.h`). This is not part of
// the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// Write the entries of the partial ID maps (see `IdMapWriter`), one map per
// partial vocabulary, from the segments of the merged vocabulary (see
// `Segment`). The maps are split into groups of consecutive partial
// vocabularies, and the entries of a segment are written by one task per
// group. The tasks of a group run on a strand of the global executor (see
// `util/GlobalExecutor.h`) of its own, so no two tasks ever write the same map
// at the same time, and the order of the entries within a map does not matter
// (see `IdMapFromPartialIdMapFile`), so the tasks need no further
// synchronization.
//
// NOTE: All the member functions are called by a single thread, the one that
// commits the segments of the merged vocabulary in order (the caller of this
// class, which a follow-up PR adds), which must not be a thread of the global
// executor (see `TaskQueueOnExecutor`).
class IdMapWriters {
 private:
  std::vector<IdMapWriter> idMapWriters_;
  // The number of maps per group (the last group may have fewer).
  size_t groupSize_;
  // The first exception that a task threw, if any. The remaining segments are
  // then skipped and the exception is rethrown by `finish()`.
  ad_utility::ExceptionCollector exceptionCollector_;
  // The total time that the tasks have spent writing.
  ad_utility::timer::ThreadSafeTimer busyTime_;
  // The queue of the tasks of each group, see above. The queues are bounded,
  // so a segment whose entries cannot be written yet blocks the thread that
  // commits the segments. The queues are held by `std::unique_ptr` because a
  // `TaskQueueOnExecutor` (which holds a mutex and a condition variable) can be
  // neither copied nor moved, which a `std::vector` of the queues themselves
  // would require.
  //
  // NOTE: Declared last, because the destructor of a queue waits for its
  // tasks, which use the members declared above.
  std::vector<std::unique_ptr<ad_utility::TaskQueueOnExecutor>> queues_;

 public:
  // Create the ID map of each of the partial vocabularies, in the files
  // `idMapFilenames` (at least one), in the order of the partial vocabularies,
  // written in `numGroups` groups (fewer if there are fewer maps than that).
  IdMapWriters(ad_utility::InputRangeTypeErased<std::string> idMapFilenames,
               size_t numGroups)
      : idMapWriters_{::ranges::to_vector(
            ql::views::transform(idMapFilenames, makeIdMapWriter))},
        groupSize_{ad_utility::ceilDiv(
            idMapWriters_.size(),
            std::clamp<size_t>(numGroups, 1, idMapWriters_.size()))} {
    for (size_t i = 0;
         i < ad_utility::ceilDiv(idMapWriters_.size(), groupSize_); ++i) {
      queues_.push_back(std::make_unique<ad_utility::TaskQueueOnExecutor>(
          boost::asio::make_strand(ad_utility::globalExecutor()),
          VOCAB_MERGER_ID_MAP_QUEUE_SIZE, "Writing the ID maps"));
    }
  }

  size_t numPartialVocabularies() const { return idMapWriters_.size(); }
  size_t numGroups() const { return queues_.size(); }

  // Write the entries of the `segment`, whose segment-local IDs are turned
  // into global ones with the `converter` of that segment. The `segment` is
  // kept alive until its entries are written. Do nothing if a task has failed.
  void write(std::shared_ptr<const Segment> segment,
             IdConverterForSegment converter) {
    AD_CONTRACT_CHECK(segment->idMapEntries_.size() ==
                      numPartialVocabularies());
    if (hasFailed()) {
      return;
    }
    // The writer and the entries of each map, side by side, in groups.
    auto groups = ::ranges::views::chunk(
        ::ranges::views::zip(idMapWriters_, segment->idMapEntries_),
        groupSize_);
    for (auto&& [groupIndex, group] : ::ranges::views::enumerate(groups)) {
      // The task runs after `write` has returned, so it captures by value.
      // Each task needs its own copies (not moves), but they are cheap: the
      // `group` is a view (a few iterators into `idMapWriters_` and the
      // `idMapEntries_` of the `segment`), the `segment` is a `shared_ptr`
      // (which keeps the entries that the `group` points to alive), and the
      // `converter` is a small array of counts.
      queues_[groupIndex]->push(
          exceptionCollector_.wrap([this, group, segment, converter]() mutable {
            // Skip the remaining segments once a task has failed.
            if (hasFailed()) {
              return;
            }
            auto measurement = busyTime_.startMeasurement();
            for (auto&& [writer, entries] : group) {
              for (const SegmentIdMapEntry& entry : entries) {
                writer.push(IdMapEntry{VocabIndex::make(entry.localIndex_),
                                       converter.globalIdOf(entry.id_)});
              }
            }
          }));
    }
  }

  // Whether one of the tasks has thrown an exception that `finish()` has not
  // rethrown yet. The caller should then stop writing segments.
  bool hasFailed() const { return exceptionCollector_.hasException(); }

  // Wait until all the entries are written, flush and close the maps, and
  // rethrow the first exception that one of the tasks threw, if any. After
  // this, no more segments may be written.
  void finish() {
    for (auto& queue : queues_) {
      queue->finish();
    }
    for (auto& writer : idMapWriters_) {
      writer.finish();
    }
    exceptionCollector_.rethrowIfException();
  }

  // The total time that the tasks have spent writing.
  ad_utility::Timer::Duration busyTime() const { return busyTime_.value(); }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPWRITERS_H
