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

#include "index/IndexBuilderTypes.h"
#include "util/MemorySize/MemorySize.h"

// The words that the vocabulary merger (see `index/VocabularyMerger.h`) reads
// from the partial vocabularies and merges. This is not part of the public
// interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// Helper `struct` for a word from a partial vocabulary.
struct QueueWord {
  QueueWord() = default;
  QueueWord(TripleComponentWithIndex&& v, size_t file)
      : entry_(std::move(v)), partialFileId_(file) {}
  // The word, its local ID, and the information whether it will be
  // externalized.
  TripleComponentWithIndex entry_;
  size_t partialFileId_;  // from which partial vocabulary did this word come

  [[nodiscard]] bool& isExternal() { return entry_.isExternal(); }

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
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORD_H
