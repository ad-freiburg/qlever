// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/SecondaryVocabulary.h"

#include <utility>

#include "backports/algorithm.h"
#include "util/Exception.h"

// _____________________________________________________________________________
SecondaryVocabulary::SecondaryVocabulary(std::vector<std::string> words)
    : words_{std::move(words)} {
  AD_CONTRACT_CHECK(ql::ranges::is_sorted(words_),
                    "The words of a secondary vocabulary have to be sorted");
  AD_CONTRACT_CHECK(ql::ranges::adjacent_find(words_) == words_.end(),
                    "The words of a secondary vocabulary have to be distinct");
}

// _____________________________________________________________________________
std::string_view SecondaryVocabulary::operator[](
    SecondaryVocabIndex index) const {
  AD_CONTRACT_CHECK(index.get() < words_.size());
  return words_[index.get()];
}

// _____________________________________________________________________________
std::optional<SecondaryVocabIndex> SecondaryVocabulary::getId(
    std::string_view word) const {
  auto it = ql::ranges::lower_bound(words_, word);
  if (it == words_.end() || *it != word) {
    return std::nullopt;
  }
  return SecondaryVocabIndex::make(static_cast<uint64_t>(it - words_.begin()));
}

// _____________________________________________________________________________
void SecondaryVocabulary::setMainVocabComparator(
    const TripleComponentComparator& comparator) {
  mainVocabComparator_ = &comparator;
}

// _____________________________________________________________________________
int SecondaryVocabulary::compareWordTo(SecondaryVocabIndex index,
                                       std::string_view word) const {
  return mainVocabComparator().compare((*this)[index], word,
                                       LocaleManager::Level::TOTAL);
}

// _____________________________________________________________________________
int SecondaryVocabulary::compareWords(SecondaryVocabIndex a,
                                      SecondaryVocabIndex b) const {
  return compareWordTo(a, (*this)[b]);
}

// _____________________________________________________________________________
const TripleComponentComparator& SecondaryVocabulary::mainVocabComparator()
    const {
  AD_CONTRACT_CHECK(
      mainVocabComparator_ != nullptr,
      "A `SecondaryVocabulary` can only compare its words semantically once "
      "the comparator of the vocabulary of the main index has been set, see "
      "`SecondaryVocabulary::setMainVocabComparator`");
  return *mainVocabComparator_;
}
