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
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "index/IndexBuilderTypes.h"
#include "index/vocabulary_merger/IdMapBatch.h"
#include "util/MemorySize/MemorySize.h"

// The data that the individual stages of the vocabulary merger (see
// `index/VocabularyMerger.h`) hand to each other. In contrast to
// `index/vocabulary_merger/IdMapBatch.h`, these types also carry the words
// themselves.
namespace ad_utility::vocabulary_merger::detail {

// Helper `struct` for a word from a partial vocabulary.
struct QueueWord {
  QueueWord() = default;
  QueueWord(TripleComponentWithIndex&& v, size_t file)
      : entry_(std::move(v)), partialFileId_(file) {}
  TripleComponentWithIndex entry_;  // the word, its local ID and the
                                    // information if it will be externalized
  size_t partialFileId_;  // from which partial vocabulary did this word come

  [[nodiscard]] const bool& isExternal() const { return entry_.isExternal(); }
  [[nodiscard]] bool& isExternal() { return entry_.isExternal(); }

  [[nodiscard]] const std::string& iriOrLiteral() const {
    return entry_.iriOrLiteral();
  }

  [[nodiscard]] std::string& iriOrLiteral() { return entry_.iriOrLiteral(); }

  [[nodiscard]] const auto& id() const { return entry_.index_; }
};

// Compute the memory footprint of a `QueueWord`, which the parallel merging
// needs to limit its memory consumption.
struct SizeOfQueueWord {
  ad_utility::MemorySize operator()(const QueueWord& q) const {
    return ad_utility::MemorySize::bytes(sizeof(QueueWord) +
                                         q.entry_.iriOrLiteral().size());
  }
};
inline constexpr SizeOfQueueWord sizeOfQueueWord{};

// A word that occurs in the merged vocabulary for the first time, together
// with the information whether it is to be externalized.
struct UniqueWord {
  // NOTE: This typically is a view into one of the `mergedWordBuffers_` of the
  // `WordBatch` that this word belongs to. Those buffers are deliberately
  // kept alive until the batch has been written to the vocabulary, such that
  // the merging thread never has to copy or move a single word. The only
  // exception is the first word of a batch, see `WordBatch::carriedOverWord_`.
  std::string_view word_;
  bool isExternal_;
};

// A batch of merged words, as it is handed from the merging thread to the
// thread that writes the words to the vocabulary.
struct WordBatch {
  std::vector<UniqueWord> uniqueWords_;
  QueuedIdMapBatch queuedIdMapBatch_;
  // The buffers of merged words that back the `string_view`s of the
  // `uniqueWords_` (see there).
  std::vector<std::vector<QueueWord>> mergedWordBuffers_;
  // The first of the `uniqueWords_` is typically carried over from the
  // previous batch (see `WordBatchBuilder`), in which case it was merged from
  // a buffer that belongs to that previous batch and this batch has to own its
  // own copy of it.
  //
  // NOTE: The indirection via the `unique_ptr` is required because a
  // `WordBatch` is moved several times on its way through the pipeline, which
  // (because of the short string optimization) would invalidate a
  // `string_view` into a plain `std::string` member.
  std::unique_ptr<std::string> carriedOverWord_;
};

// Concept for a callback that consumes a complete `WordBatch`.
template <typename T>
CPP_concept WordBatchCallback = std::is_invocable_v<const T&, WordBatch>;

// The number of ID map entries (which is the same as the number of merged
// words) that are collected in a single batch. A single buffer of merged
// words only contains a rather small number of words (currently 100), which
// would be much too fine-grained for a task queue.
inline constexpr size_t idMapEntryBatchSize = 100'000;

// The maximal number of batches that may be waiting in each of the queues of
// the pipeline. NOTE: A batch keeps all the merged words alive that it was
// created from (typically a few megabytes, see `idMapEntryBatchSize`), so this
// also determines the memory footprint of the pipeline, which currently is in
// the order of a hundred megabytes.
inline constexpr size_t queueSize = 3;
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_WORDBATCH_H
