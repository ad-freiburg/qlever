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

// The value that is stored in `LocalIdxToBatchMapping::indexOfWordInBatch_` as
// long as the index of the word within its batch is not yet known (see
// `WordBatchBuilder::commitPendingWord`). Deliberately a bogus bit pattern
// with the highest bit set (and in particular not `0`), such that a mapping
// for which that index was never filled in is easy to spot.
inline constexpr uint32_t indexOfWordInBatchDummy = 0xDEADBEEF;

// The maximal number of distinct words in a single batch. Because of this
// limit, every actual `indexOfWordInBatch_` is smaller than the
// `indexOfWordInBatchDummy` above (which has its highest bit set), so the
// dummy can never be confused with an actual index. NOTE: The limit is checked
// in `WordBatchBuilder::commitPendingWord`, and it is far larger than the
// batch sizes that are used in practice (see `VOCAB_MERGER_WORD_BATCH_SIZE`).
inline constexpr uint32_t maxNumUniqueWordsPerBatch = uint32_t{1} << 31;
static_assert(maxNumUniqueWordsPerBatch <= indexOfWordInBatchDummy,
              "The dummy has to be an upper bound for the actual indices of "
              "the words within a batch, see the comment above");

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

  // Create an empty batch, with the buffers for `numWordsPerBatch` merged
  // words already allocated.
  explicit WordBatch(size_t numWordsPerBatch) {
    // The mappings are stored in a vector with a `default_init_allocator`, so
    // this `resize` is a plain allocation that doesn't touch the memory.
    localIdxMappings_.mappings_.resize(numWordsPerBatch);
    // There is exactly one index mapping per merged word, and the number of
    // distinct words is at most the number of merged words, so this is an
    // upper bound for all but the rare batch that slightly overshoots the
    // `numWordsPerBatch` (a batch is only handed on once a complete buffer of
    // merged words has been added to it).
    uniqueWords_.reserve(numWordsPerBatch);
  }

  // A batch is empty as long as no index mapping has been added to it. NOTE:
  // There is exactly one index mapping per merged word, so an empty batch also
  // has no `uniqueWords_`.
  bool empty() const { return localIdxMappings_.numMappings_ == 0; }
};

// Concept for a callback that consumes a complete `WordBatch`.
template <typename T>
CPP_concept WordBatchCallback = std::is_invocable_v<const T&, WordBatch>;
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDBATCH_H
