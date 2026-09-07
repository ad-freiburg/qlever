// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDBATCH_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDBATCH_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "backports/concepts.h"
#include "global/VocabIndex.h"
#include "index/vocabulary_merger/QueueWord.h"
#include "util/UninitializedAllocator.h"

// A batch of merged words, which is the unit of work that the first stage of
// the vocabulary merger (see `index/vocabulary_merger/WordBatchBuilder.h`)
// hands to the stage that writes the words and the partial ID maps. This is
// not part of the public interface of `index/VocabularyMerger.h`.
namespace ad_utility::vocabulary_merger::detail {

// A mapping from an `indexOfWordInPartialVocabulary_` (an index of a word in
// the `partialVocabularyIndex_`-th partial vocabulary) to the corresponding
// index of the word in a merged batch (`indexOfWordInBatch_`) of words, for
// which the global ID is not yet known.
//
// NOTE: The declaration order deliberately deviates from the logical order of
// the members, such that the struct is exactly 16 and not 24 bytes large.
// There is one such mapping per merged word, and they are written scattered
// over all the partial ID maps, so both the memory footprint and the cache
// pressure of this struct matter.
struct LocalIdxToBatchMapping {
  uint32_t partialVocabularyIndex_;
  uint32_t indexOfWordInBatch_;
  VocabIndex indexOfWordInPartialVocabulary_;
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

// A unique word from the merged vocabulary (after deduplication between the
// partial vocabularies has been performed) together with the information
// whether the word is to be externalized. NOTE: The word is stored in a
// non-owning `string_view` for performance reasons, so the batch has to keep
// all the words alive. The only exception is the first word of a batch, see
// `WordBatch::carriedOverWord_`.
struct UniqueWord {
  std::string_view word_;
  bool isExternal_;
};

// A batch of merged words, as it is handed from the merging thread to the
// thread that writes the words to the vocabulary.
struct WordBatch {
  std::vector<UniqueWord> uniqueWords_;
  LocalIdxToBatchMappings localIdxMappings_;
  // The buffers of merged words that back the `string_view`s of the
  // `uniqueWords_` (see there).
  std::vector<std::vector<QueueWord>> mergedWordBuffers_;
  // The first of the `uniqueWords_` is typically carried over from the
  // previous batch (see `WordBatchBuilder`), in which case it was merged from
  // a buffer that belongs to that previous batch and this batch has to own its
  // own copy of it.
  //
  // NOTE: The indirection via the `unique_ptr` is required because a
  // `WordBatch` is moved several times on its way to the writing thread, which
  // (because of the short string optimization) would invalidate a
  // `string_view` into a plain `std::string` member.
  std::unique_ptr<std::string> carriedOverWord_;
};

// Concept for a callback that consumes a complete `WordBatch`.
template <typename T>
CPP_concept WordBatchCallback = std::is_invocable_v<const T&, WordBatch>;

// The number of index mappings (which is the same as the number of merged
// words) that are collected in a single batch. A single buffer of merged
// words only contains a rather small number of words (currently 100), which
// would be much too fine-grained for a task queue.
inline constexpr size_t wordBatchSize = 100'000;

// The maximal number of batches that may be waiting in the queue of the
// writing thread. NOTE: A batch keeps all the merged words alive that it was
// created from (typically a few megabytes, see `wordBatchSize`), so this also
// determines the additional memory footprint of the writing.
inline constexpr size_t wordBatchQueueSize = 3;
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDBATCH_H
