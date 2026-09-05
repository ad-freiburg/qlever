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

#include <absl/strings/str_cat.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/IdMap.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/UninitializedAllocator.h"
#include "util/Views.h"

// The intermediate stages of the vocabulary merger (see
// `index/VocabularyMerger.h`) that deal with the mapping from local indices
// (local to a partial vocabulary) to the index in a merged batch of words, for
// which the global ID is not yet known.
namespace ad_utility::vocabulary_merger::detail {

// A mapping from an `indexOfWordInPartialVocabulary_` (an index of a word in
// the `partialVocabularyIndex_`-th partial vocabulary) to the corresponding
// index of the word in a merged batch (`indexOfWordInBatch_`) of words (for
// which the global IDs are not yet known).
//
// NOTE: The declaration order deliberately deviates from the logical order of
// the members, such that the struct is exactly 16 and not 24 bytes large.
// There is one such mapping per merged word, and they are written scattered
// over all the partial ID maps, so both the memory footprint and the cache
// pressure of this struct matter.
struct LocalIdxToBatchMapping {
  uint32_t partialVocabularyIndex_;
  uint32_t indexOfWordInBatch_;
  uint64_t indexOfWordInPartialVocabulary_;
};
static_assert(sizeof(LocalIdxToBatchMapping) == 16,
              "The members of a `LocalIdxToBatchMapping` have to be declared "
              "such that no padding is required, see the comment above");

// All the `LocalIdxToBatchMapping`s for a single batch of merged words. NOTE:
// We deliberately do not use a plain vector with `push_back`, but a plain
// array with a manual index for maximal performance (the `push_back` overhead
// was measurable on the hot path).
struct LocalIdxToBatchMappings {
  ad_utility::UninitializedVector<LocalIdxToBatchMapping> mappings_;
  size_t numMappings_ = 0;
};

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
// NOTE: This class is used exclusively by the thread of the
// `idMapWriterQueue_` of the `VocabularyMergePipeline`.
class IdMapBatchWriter {
 private:
  // The ID map writers, one per partial vocabulary.
  std::vector<IdMapWriter> idMapWriters_;

 public:
  // Create the ID map for each of the `numFiles` partial vocabularies. The
  // filenames are `basename + PARTIAL_VOCAB_IDMAP_INFIX + i`.
  IdMapBatchWriter(const std::string& basename, size_t numFiles) {
    // The index of the partial vocabulary is stored in a `uint32_t` for each of
    // the (very many) mappings, see `LocalIdxToBatchMapping`.
    AD_CORRECTNESS_CHECK(numFiles <= std::numeric_limits<uint32_t>::max());
    // NOTE: We deliberately use the range constructor of `std::vector` and not
    // `::ranges::to_vector`. The latter goes via `std::vector::assign`, which
    // requires the elements to be assignable, which an `IdMapWriter`
    // deliberately is not (see `index/vocabulary_merger/IdMap.h`).
    auto writers = ad_utility::integerRange(numFiles) |
                   ql::views::transform([&basename](size_t i) {
                     return IdMapWriter{
                         absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i)};
                   });
    idMapWriters_ = std::vector<IdMapWriter>(ql::ranges::begin(writers),
                                             ql::ranges::end(writers));
  }

  // Write all the mappings of the `batch` to their respective ID maps.
  void writeBatch(const IdMapBatch& batch) {
    AD_LOG_TRACE << "Start writing a batch of ID map entries\n";
    const auto& globalIds = batch.globalIds_;
    const auto& localIdxMappings = batch.localIdxMappings_;
    for (size_t i = 0; i < localIdxMappings.numMappings_; ++i) {
      const auto& mapping = localIdxMappings.mappings_[i];
      idMapWriters_[mapping.partialVocabularyIndex_].push_back(
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
