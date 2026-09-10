// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPBATCH_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPBATCH_H

#include <cstddef>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "global/Id.h"
#include "index/PartialVocabularyFilenames.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/WordBatch.h"
#include "util/Log.h"

// The third stage of the merging pipeline of the vocabulary merger (see the
// comment above `mergeVocabulary` in `index/VocabularyMerger.h`), which writes
// the entries of the partial ID maps. This is not part of the public interface
// of that header.
namespace ad_utility::vocabulary_merger::detail {

// The index mappings for a complete merged batch of words. `globalIds_` stores
// the global IDs for the words in this batch,
// `LocalIdxToBatchMapping::indexOfWordInBatch_` is an index into `globalIds_`.
struct IdMapBatch {
  LocalIdxToBatchMappings localIdxMappings_;
  std::vector<Id> globalIds_;
};

// The third stage of the merging pipeline: write the entries of a complete
// `IdMapBatch` to the partial ID maps, one of which is created per partial
// vocabulary.
//
// NOTE: This class is used exclusively by the single thread of the
// `idMapWriterQueue_` of the `VocabularyMergePipeline`. Therefore it does not
// need to be threadsafe.
class IdMapBatchWriter {
 private:
  // The ID map writers, one per partial vocabulary.
  std::vector<IdMapWriter> idMapWriters_;

 public:
  // Create the ID map for each of the partial vocabularies, in the files
  // `partialVocabularyIdMapFilename(basename, idx)` for each `idx` in
  // `[0, numPartialVocabularies)`.
  // NOTE: That the number of partial vocabularies fits into the `uint32_t` of
  // a `LocalIdxToBatchMapping` is checked by `mergeVocabulary` (see
  // `index/VocabularyMergerImpl.h`).
  IdMapBatchWriter(const std::string& basename, size_t numPartialVocabularies) {
    // NOTE: We deliberately use the range constructor of `std::vector` and not
    // `::ranges::to_vector`. The latter goes via `std::vector::assign`, which
    // requires the elements to be assignable, which an `IdMapWriter`
    // deliberately is not (see `index/vocabulary_merger/IdMap.h`).
    auto writers =
        partialVocabularyIdMapFilenames(basename, numPartialVocabularies) |
        ql::views::transform([](const std::string& filename) {
          return makeIdMapWriter(filename);
        });
    idMapWriters_ = std::vector<IdMapWriter>(ql::ranges::begin(writers),
                                             ql::ranges::end(writers));
  }

  // Write all the mappings of the `batch` to their respective ID maps.
  //
  // TODO<optimization> The `mappings_` are in merge order, so the loop below
  // round-robins over the `idMapWriters_` instead of grouping the entries by
  // the file they belong to. Stably partitioning (or sorting) the mappings by
  // their `partialVocabularyIndex_` before the loop would give each
  // `IdMapWriter` one contiguous run per batch and thus a more useful external
  // access pattern. This is local to this stage and doesn't affect any of the
  // other stages of the pipeline.
  void writeBatch(const IdMapBatch& batch) {
    AD_LOG_TRACE << "Start writing a batch of ID map entries\n";
    const auto& globalIds = batch.globalIds_;
    const auto& localIdxMappings = batch.localIdxMappings_;
    // NOTE: We deliberately use a manual loop and not a range-based one,
    // because only the first `numMappings_` elements of the `mappings_` are
    // initialized (see `LocalIdxToBatchMappings` in
    // `index/vocabulary_merger/WordBatch.h`).
    for (size_t i = 0; i < localIdxMappings.numMappings_; ++i) {
      const auto& mapping = localIdxMappings.mappings_[i];
      idMapWriters_[mapping.partialVocabularyIndex_].push(
          IdMapEntry{mapping.indexOfWordInPartialVocabulary_,
                     globalIds[mapping.indexOfWordInBatch_]});
    }
  }

  // Flush and close all the ID maps. After this, no more batches may be
  // written. NOTE: This is also done implicitly by the destructor, because the
  // destructor of an `IdMapWriter` calls its `finish()`.
  void finish() {
    for (auto& idMapWriter : idMapWriters_) {
      idMapWriter.finish();
    }
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_IDMAPBATCH_H
