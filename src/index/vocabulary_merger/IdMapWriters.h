// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPWRITERS_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPWRITERS_H

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/Segment.h"
#include "util/Iterators.h"
#include "util/TaskQueue.h"
#include "util/Timer.h"

// The writers of the partial ID maps of the vocabulary merger (see the comment
// above `mergeVocabulary` in `index/VocabularyMerger.h`). This is not part of
// the public interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// Write the entries of the partial ID maps (see `IdMapWriter`), one map per
// partial vocabulary, from the segments of the merged vocabulary (see
// `Segment`). The maps are written by several threads, each of which owns the
// maps of every `numThreads`-th partial vocabulary; the entries of a segment
// are handed to all of them at once. No two threads ever write the same map,
// and the order of the entries within a map does not matter (see
// `IdMapFromPartialIdMapFile`), so the threads need no synchronization.
class IdMapWriters {
 private:
  std::vector<IdMapWriter> idMapWriters_;
  // One queue with a single thread per group of maps, see above. The queues
  // are bounded, so a segment whose entries cannot be written yet blocks the
  // thread that commits the segments.
  std::vector<std::unique_ptr<ad_utility::TaskQueue<false>>> queues_;
  // The time that the threads have spent writing, in milliseconds.
  std::atomic<uint64_t> busyMs_{0};

 public:
  // Create the ID map of each of the partial vocabularies, in the files
  // `idMapFilenames`, in the order of the partial vocabularies, written by
  // `numThreads` threads.
  IdMapWriters(ad_utility::InputRangeTypeErased<std::string> idMapFilenames,
               size_t numThreads) {
    // NOTE: A manual loop, because an `IdMapWriter` is not assignable.
    for (const std::string& filename : idMapFilenames) {
      idMapWriters_.emplace_back(makeIdMapWriter(filename));
    }
    numThreads =
        std::max<size_t>(1, std::min(numThreads, idMapWriters_.size()));
    for (size_t i = 0; i < numThreads; ++i) {
      queues_.push_back(std::make_unique<ad_utility::TaskQueue<false>>(
          VOCAB_MERGER_ID_MAP_QUEUE_SIZE, 1, "Writing the ID maps"));
    }
  }

  size_t numPartialVocabularies() const { return idMapWriters_.size(); }

  // Write the entries of the `segment`, whose segment-local IDs are turned
  // into global ones with the `bases` of that segment. The `segment` is kept
  // alive until its entries are written.
  void write(std::shared_ptr<const Segment> segment, SegmentBases bases) {
    AD_CONTRACT_CHECK(segment->idMapRunStarts_.size() ==
                      numPartialVocabularies() + 1);
    const size_t numThreads = queues_.size();
    for (size_t k = 0; k < numThreads; ++k) {
      queues_[k]->push([this, k, numThreads, segment, bases]() {
        ad_utility::Timer timer{ad_utility::Timer::Started};
        const auto& entries = segment->idMapEntries_;
        const auto& runStarts = segment->idMapRunStarts_;
        for (size_t p = k; p < numPartialVocabularies(); p += numThreads) {
          auto& writer = idMapWriters_[p];
          for (size_t i = runStarts[p]; i < runStarts[p + 1]; ++i) {
            const auto& entry = entries[i];
            writer.push(IdMapEntry{VocabIndex::make(entry.localIndex_),
                                   bases.globalIdOf(entry.id_)});
          }
        }
        busyMs_ += timer.msecs().count();
      });
    }
  }

  // Wait until all the entries are written, then flush and close the maps.
  // After this, no more segments may be written.
  void finish() {
    for (auto& queue : queues_) {
      queue->finish();
    }
    for (auto& writer : idMapWriters_) {
      writer.finish();
    }
  }

  // The total time that the threads have spent writing, in milliseconds.
  uint64_t busyMs() const { return busyMs_; }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPWRITERS_H
