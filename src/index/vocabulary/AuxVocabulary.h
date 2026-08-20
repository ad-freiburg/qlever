// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_AUXVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_AUXVOCABULARY_H

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "global/IndexTypes.h"

// The auxiliary vocabulary of an index. It stores exactly those words that were
// added by updates after the main index was built and that are not part of the
// vocabulary of that main index. The `Id`s of its words have their own
// `Datatype` (`Datatype::AuxVocabIndex`), so all of them are greater than all
// `Id`s of the main vocabulary when compared bitwise, which is what makes those
// words mergeable into a scan of the main index.
//
// TODO<joka921> This currently is a minimal placeholder that simply holds all
// its words in RAM and that is only ever filled explicitly by unit tests (see
// `IndexImpl::setAuxVocabForTesting`). The actual implementation stores the
// words on disk, using the same vocabulary type (and hence the same split into
// sub-vocabularies) as the main index, and it additionally stores, for each of
// its words, the position at which that word would be sorted into the main
// vocabulary. The latter is what a *semantic* (that is, by string value)
// comparison of an `Id` of this vocabulary with an `Id` of the main vocabulary
// needs; it follows together with the auxiliary index itself.
class AuxVocabulary {
 private:
  // The words, in strictly ascending order, see the constructor.
  std::vector<std::string> words_;

 public:
  AuxVocabulary() = default;

  // Create a vocabulary that holds the given `words`, none of which may be
  // contained in the vocabulary of the main index. The words have to be in
  // strictly ascending order with respect to `std::string`'s comparison, which
  // is checked, so that they can be looked up by binary search.
  //
  // NOTE: The actual implementation will instead order its words by the
  // comparator of the vocabulary of the main index at the `TOTAL` level, which
  // this placeholder has no access to. The two orders do not agree in general,
  // so the words that the unit tests use are deliberately chosen such that they
  // do (see `test/AuxVocabularyTest.cpp`).
  explicit AuxVocabulary(std::vector<std::string> words);

  // Return the number of words.
  size_t numWords() const { return words_.size(); }

  // Return the word with the given index.
  std::string_view operator[](AuxVocabIndex index) const;

  // Look up `word`. Return its index if it is contained in this vocabulary, and
  // `std::nullopt` otherwise.
  std::optional<AuxVocabIndex> getId(std::string_view word) const;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_AUXVOCABULARY_H
