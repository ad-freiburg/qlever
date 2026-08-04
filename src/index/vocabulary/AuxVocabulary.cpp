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

// _____________________________________________________________________________
AuxVocabulary::AuxVocabulary(std::vector<std::string> words)
    : words_{std::move(words)} {
  AD_CONTRACT_CHECK(ql::ranges::is_sorted(words_),
                    "The words of an auxiliary vocabulary have to be sorted");
  AD_CONTRACT_CHECK(ql::ranges::adjacent_find(words_) == words_.end(),
                    "The words of an auxiliary vocabulary have to be distinct");
}

// _____________________________________________________________________________
std::string_view AuxVocabulary::operator[](AuxVocabIndex index) const {
  AD_CONTRACT_CHECK(index.get() < words_.size());
  return words_[index.get()];
}

// _____________________________________________________________________________
std::optional<AuxVocabIndex> AuxVocabulary::getId(std::string_view word) const {
  auto it = ql::ranges::lower_bound(words_, word);
  if (it == words_.end() || *it != word) {
    return std::nullopt;
  }
  return AuxVocabIndex::make(static_cast<uint64_t>(it - words_.begin()));
}
