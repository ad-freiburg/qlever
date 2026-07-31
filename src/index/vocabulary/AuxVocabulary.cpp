// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/AuxVocabulary.h"

#include <utility>

#include "backports/algorithm.h"
#include "util/Exception.h"

// ____________________________________________________________________________
AuxVocabulary::AuxVocabulary(std::vector<std::string> words)
    : words_{std::move(words)} {}

// ____________________________________________________________________________
std::string_view AuxVocabulary::operator[](AuxVocabIndex index) const {
  AD_CONTRACT_CHECK(index.get() < words_.size());
  return words_[index.get()];
}

// ____________________________________________________________________________
std::optional<AuxVocabIndex> AuxVocabulary::getId(std::string_view word) const {
  // NOTE: A linear scan is fine, because this class currently only holds the
  // handful of words that a unit test explicitly puts into it. The actual
  // implementation will perform a binary search in the vocabulary on disk.
  auto it = ql::ranges::find(words_, word);
  if (it == words_.end()) {
    return std::nullopt;
  }
  return AuxVocabIndex::make(static_cast<uint64_t>(it - words_.begin()));
}
