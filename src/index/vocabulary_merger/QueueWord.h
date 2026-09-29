// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORD_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORD_H

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "index/IndexBuilderTypes.h"
#include "util/MemorySize/MemorySize.h"

// The words that the vocabulary merger (see `index/VocabularyMerger.h`) reads
// from the partial vocabularies and merges. This is not part of the public
// interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// Helper `struct` for a word from a partial vocabulary. While the partial
// vocabularies are merged, the further occurrences of the same word in other
// partial vocabularies are collected in the `moreOccurrences_` (see
// `PartialVocabularyRunsInput::appendToBlock`), so that the words that leave
// the merge are distinct within a block, with one entry per occurrence.
struct QueueWord {
  // An occurrence of a word in a partial vocabulary: the index of the partial
  // vocabulary and the local index of the word in it.
  using Occurrence = std::pair<uint32_t, uint64_t>;

  QueueWord() = default;
  QueueWord(TripleComponentWithIndex&& v, size_t file)
      : entry_(std::move(v)), partialFileId_(file) {}
  // The word, its local ID, and the information whether it will be
  // externalized (in any of its occurrences).
  TripleComponentWithIndex entry_;
  size_t partialFileId_;  // from which partial vocabulary did this word come
  // The occurrences of the same word in other partial vocabularies than the
  // `partialFileId_`, see above. Empty for a word that was read from a file.
  std::vector<Occurrence> moreOccurrences_;

  [[nodiscard]] bool& isExternal() { return entry_.isExternal(); }
  // NOTE: The `const` overloads are needed because the first stage of the
  // merging (see `index/vocabulary_merger/WordBatchBuilder.h`) only reads the
  // merged words; it never modifies them.
  [[nodiscard]] const bool& isExternal() const { return entry_.isExternal(); }

  [[nodiscard]] std::string& iriOrLiteral() { return entry_.iriOrLiteral(); }
  [[nodiscard]] const std::string& iriOrLiteral() const {
    return entry_.iriOrLiteral();
  }

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
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORD_H
