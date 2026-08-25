// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_SECONDARYVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_SECONDARYVOCABULARY_H

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "global/IndexTypes.h"
#include "index/vocabulary/StringSortComparator.h"

// The secondary vocabulary of an index. It stores words that were added after
// the main index was built and that are not part of the vocabulary of that
// main index, so that data containing such words can be persisted and reloaded
// (in particular delta triples and materialized views, whose new words
// otherwise only exist as `LocalVocabEntry`s, that is, as pointers into the
// memory of one process). The `Id`s of its words have their own `Datatype`
// (`Datatype::SecondaryVocabIndex`), so all of them are greater than all `Id`s
// of the main vocabulary when compared bitwise, which is what makes those
// words mergeable into a scan of the main index.
//
// A *semantic* (that is, by string value) comparison of a word of this
// vocabulary therefore must not use the `Id`s, but has to go through
// `compareWordTo` and `compareWords` below.
//
// TODO<joka921> This currently is a minimal placeholder that simply holds all
// its words in RAM and that is only ever filled explicitly by unit tests (see
// `IndexImpl::setSecondaryVocabForTesting`). The actual implementation stores
// the words on disk, using the same vocabulary type (and hence the same split
// into sub-vocabularies) as the main index, and it additionally stores, for
// each of its words, the position at which that word would be sorted into the
// main vocabulary. The latter makes the semantic comparisons below cheap: they
// currently have to look up the words and run the (expensive) collation of the
// main vocabulary on them, whereas the stored positions can simply be compared
// to each other and to a `VocabIndex`.
class SecondaryVocabulary {
 private:
  // The words, in strictly ascending order, see the constructor.
  std::vector<std::string> words_;

  // The comparator of the vocabulary of the main index, see
  // `setMainVocabComparator`. It is a pointer and not a reference, so that this
  // class stays assignable, and it is `nullptr` until it has been set.
  const TripleComponentComparator* mainVocabComparator_ = nullptr;

 public:
  SecondaryVocabulary() = default;

  // Create a vocabulary that holds the given `words`, none of which may be
  // contained in the vocabulary of the main index. The words have to be in
  // strictly ascending order with respect to `std::string`'s comparison, which
  // is checked, so that they can be looked up by binary search.
  //
  // NOTE: The actual implementation will instead order its words by the
  // comparator of the vocabulary of the main index at the `TOTAL` level, and
  // look them up accordingly. The two orders do not agree in general, so the
  // words that the unit tests use are deliberately chosen such that they do
  // (see `test/SecondaryVocabularyTest.cpp`). This only concerns the lookup in
  // `getId` below; the semantic comparisons are unaffected, because they always
  // go through `mainVocabComparator_` and never through the order of `words_`.
  explicit SecondaryVocabulary(std::vector<std::string> words);

  // Return the number of words.
  size_t numWords() const { return words_.size(); }

  // Return the word with the given index.
  std::string_view operator[](SecondaryVocabIndex index) const;

  // Look up `word`. Return its index if it is contained in this vocabulary, and
  // `std::nullopt` otherwise.
  std::optional<SecondaryVocabIndex> getId(std::string_view word) const;

  // Set the comparator of the vocabulary of the main index, which this
  // vocabulary needs in order to compare its words semantically (that is, by
  // string value) to the words of that vocabulary. It has to be set before any
  // of the comparisons below is called. NOTE: This is deliberately a separate
  // setter and not a constructor argument, because a `SecondaryVocabulary` is
  // created before it is attached to an index (see
  // `IndexImpl::setSecondaryVocabForTesting`).
  void setMainVocabComparator(const TripleComponentComparator& comparator);

  // Semantically compare the word with the given `index` to `word`, using the
  // comparator of the vocabulary of the main index at the `TOTAL` level (which
  // is the level at which that vocabulary is sorted). Return a value less than,
  // equal to, or greater than zero if the word with the given `index` is
  // smaller than, equal to, or greater than `word`.
  int compareWordTo(SecondaryVocabIndex index, std::string_view word) const;

  // Semantically compare the words with the given indices, see `compareWordTo`.
  int compareWords(SecondaryVocabIndex a, SecondaryVocabIndex b) const;

 private:
  // Return the comparator of the vocabulary of the main index. Fail if it has
  // not been set yet, see `setMainVocabComparator`.
  const TripleComponentComparator& mainVocabComparator() const;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SECONDARYVOCABULARY_H
