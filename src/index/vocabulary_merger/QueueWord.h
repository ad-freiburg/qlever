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

// The words that the vocabulary merger (see `index/VocabularyMerger.h`) reads
// from the partial vocabularies and merges. This is not part of the public
// interface of that header.
namespace ad_utility::vocabulary_merger::detail {

// Helper `struct` for a word from a partial vocabulary.
struct QueueWord {
  QueueWord() = default;
  QueueWord(TripleComponentWithIndex&& v, size_t file, uint64_t geoSortKey = 0)
      : entry_(std::move(v)), partialFileId_(file), geoSortKey_(geoSortKey) {}
  // The word, its local ID, and the information whether it will be
  // externalized.
  TripleComponentWithIndex entry_;
  size_t partialFileId_;  // from which partial vocabulary did this word come
  // The geo sort key of the word (see `ItemVecEntry`), computed when the word
  // was read from its partial vocabulary (see `PartialVocabularyRunsInput`).
  uint64_t geoSortKey_ = 0;

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
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_QUEUEWORD_H
