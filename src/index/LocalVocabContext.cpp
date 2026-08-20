// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/LocalVocabContext.h"

#include "util/Exception.h"

// The destructor is defined out of line, such that the vtable is emitted in
// exactly one translation unit.
LocalVocabContext::~LocalVocabContext() = default;

// _____________________________________________________________________________
auto LocalVocabContext::lookupWordInVocabularies(std::string_view word) const
    -> IdOrVocabBounds {
  auto [lower, upper] = getPositionOfWord(word);
  if (lower != upper) {
    // The word is contained in the vocabulary of the main index, in which case
    // the range consists of exactly that one word.
    AD_CORRECTNESS_CHECK(upper.get() == lower.get() + 1);
    return Id::makeFromVocabIndex(lower);
  }
  // The word is not in the vocabulary of the main index, so it may be in the
  // auxiliary vocabulary. Note that the two vocabularies are disjoint, so we
  // only have to look there if the lookup above has failed.
  if (auto auxIndex = getAuxVocabIndex(word); auxIndex.has_value()) {
    return Id::makeFromAuxVocabIndex(auxIndex.value());
  }
  return VocabBounds{lower, upper};
}
