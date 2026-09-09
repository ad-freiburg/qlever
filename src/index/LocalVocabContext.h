// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_LOCALVOCABCONTEXT_H
#define QLEVER_SRC_INDEX_LOCALVOCABCONTEXT_H

#include <optional>
#include <string_view>
#include <utility>
#include <variant>

#include "global/Id.h"
#include "global/VocabIndex.h"

namespace ad_utility {
class BlankNodeManager;
}

// The interface to the vocabularies of an index that the comparison of words
// and `Id`s requires. It serves exactly two purposes:
//
// 1. A `LocalVocabEntry` stores a word that has to be comparable to the words
// in the index's vocabulary, no matter whether it is contained in that
// vocabulary or not. It therefore needs to look up where in the vocabulary the
// word is stored, or would be stored, which is what this interface provides.
//
// 2. The semantic (that is, by string value) comparison of `Id`s has to compare
// the words of two `Id`s, and to locate the word of an `Id` in the vocabulary
// of the main index. See `compareIdsSemantically` and
// `getSemanticPositionInMainVocab` below.
//
// NOTE: The only intended caller of those two functions is
// `valueIdComparators` (see `global/ValueIdComparators.h`), which does not use
// them yet: it still compares the `Id`s themselves, which is only semantically
// correct as long as the index has no secondary vocabulary, see the detailed
// note at `valueIdComparators::detail::compareIdsImpl`. Making it use these
// functions is a follow-up PR (the `TODO<joka921>` there).
//
// NOTE: This is deliberately restricted to the operations that those two
// purposes actually require, and in particular does not expose the vocabulary
// itself. Comparing two `Id`s calls into this interface and into
// `LocalVocabEntry` (see `global/ValueId.h`), so every library that compares
// `Id`s depends on both. Keeping this interface abstract and minimal is what
// allows those libraries to be independent of the (much larger) `index`
// library.
//
// The only implementation of this interface is `LocalVocabContextImpl`. There
// must be exactly one instance of it per index, because
// `LocalVocabEntry::compareThreeWay` checks that two entries belong to the same
// index by comparing the context POINTERS.
class LocalVocabContext {
 public:
  // The bounds of the range in the vocabulary in which a word is stored, or in
  // which it would be stored if it is not contained. As usual, the lower bound
  // is inclusive and the upper bound is not, so `first == second` means that
  // the word is not contained in the vocabulary.
  using VocabBounds = std::pair<VocabIndex, VocabIndex>;

  // The result of `lookupWordInVocabularies` below: either the `Id` of a word
  // that is contained in one of the vocabularies of the index, or the bounds of
  // the position of a word that is contained in none of them.
  using IdOrVocabBounds = std::variant<Id, VocabBounds>;

  virtual ~LocalVocabContext();

  // Compare the two given words using the collation of the vocabulary at the
  // `TOTAL` level. Return a value less than, equal to, or greater than zero if
  // `a` is smaller than, equal to, or greater than `b`.
  virtual int compareWords(std::string_view a, std::string_view b) const = 0;

  // Return the bounds of `word` in the vocabulary, see `VocabBounds`.
  virtual VocabBounds getPositionOfWord(std::string_view word) const = 0;

  // Semantically (that is, by string value) compare the two given `Id`s, both
  // of which have to be of one of the `ValueId::stringTypes_` (`VocabIndex`,
  // `LocalVocabIndex`, or `SecondaryVocabIndex`). Return a value less than,
  // equal to, or greater than zero if `a` is smaller than, equal to, or greater
  // than `b`.
  //
  // NOTE: This is deliberately separate from the comparison of the `Id`s
  // themselves (`ValueId::compareThreeWay`), which implements the order in
  // which the index scans emit their `Id`s (the *internal* order). The two
  // orders differ as soon as the index has a secondary vocabulary (see
  // `index/vocabulary/SecondaryVocabulary.h`): a word of that vocabulary is
  // positioned after *all* words of the main vocabulary in the internal order,
  // no matter what it is, which is exactly what makes such words mergeable into
  // a scan of the main index, but which has nothing to do with their string
  // values.
  //
  // TODO<joka921> This currently looks up the word of each `Id` in the
  // vocabularies, which is expensive. Once the secondary vocabulary stores the
  // position of each of its words in the main vocabulary (see
  // `index/vocabulary/SecondaryVocabulary.h`), this can compare those positions
  // instead.
  virtual int compareIdsSemantically(Id a, Id b) const = 0;

  // Return the position at which the word of the given `Id` is, or would be,
  // stored in the vocabulary of the main index (see `VocabBounds`). The `Id`
  // has to be of one of the `ValueId::stringTypes_`. In contrast to
  // `LocalVocabEntry::positionInVocab()`, this is always a position in the
  // *main* vocabulary, also for a word that is stored in the secondary
  // vocabulary, which is what a semantic comparison needs (see
  // `compareIdsSemantically` above).
  virtual VocabBounds getSemanticPositionInMainVocab(Id id) const = 0;

  // Return true iff this index has a secondary vocabulary at all (see
  // `index/vocabulary/SecondaryVocabulary.h`). This is the cheap check that
  // lets `LocalVocabEntry::compareThreeWay` skip the (expensive) lookup of the
  // position in the vocabulary, which is only needed if there is a secondary
  // vocabulary. It is also the check with which `valueIdComparators` will be
  // able to skip the semantic comparisons above entirely, which is the hotter
  // of the two use cases: without a secondary vocabulary the internal order of
  // the `Id`s already is their semantic order, so none of those comparisons is
  // needed in the first place.
  virtual bool hasSecondaryVocabulary() const = 0;

  // Look up `word` in the secondary vocabulary of this index (see
  // `index/vocabulary/SecondaryVocabulary.h`). Return `std::nullopt` if it is
  // not contained there, and in particular also if the index has no secondary
  // vocabulary at all. Note that the secondary vocabulary is disjoint from the
  // vocabulary of the main index, so this only has to be called if
  // `getPositionOfWord` above has already reported that `word` is not contained
  // in the latter.
  virtual std::optional<SecondaryVocabIndex> getSecondaryVocabIndex(
      std::string_view word) const = 0;

  // Try to encode `word` directly in an `Id` instead of looking it up in the
  // vocabulary (see the `EncodedIriManager`). Return `std::nullopt` if `word`
  // cannot be encoded that way.
  virtual std::optional<Id> encodeAsId(std::string_view word) const = 0;

  // Look up `word` in the vocabularies of this index. Return its `Id` if it is
  // contained in the vocabulary of the main index (an `Id` of type
  // `VocabIndex`) or in the secondary vocabulary (an `Id` of type
  // `SecondaryVocabIndex`), and else the bounds of the position at which it
  // would be sorted into the vocabulary of the main index (in which case the
  // two bounds are equal, see `VocabBounds`).
  //
  // NOTE: This function is deliberately not virtual, but implemented in terms
  // of the virtual functions above. It is the single place that knows how the
  // two vocabularies play together, so that all its callers agree on that, in
  // particular `LocalVocabEntry::positionInVocabExpensiveCase()` and
  // `toValueIdOrBounds()` (see `index/TripleComponentConversions.h`). Those two
  // have to agree, because the latter passes the position that it computes to
  // the corresponding constructor of `LocalVocabEntry`, which checks it against
  // the former.
  //
  // NOTE: This function does not try to encode `word` directly in an `Id` (see
  // `encodeAsId` above), which its callers have to do themselves if they want
  // it, and before calling this function.
  IdOrVocabBounds lookupWordInVocabularies(std::string_view word) const;

  // Return the manager for the blank nodes of this index. NOTE: This is the one
  // function here that serves neither of the two purposes described at the top
  // of this file. It is required when a complete `LocalVocab` is deserialized
  // (see `deserializeLocalVocab` in `util/Serializer/TripleSerializer.h`),
  // which has only this interface at hand, so that the blank node manager would
  // otherwise have to be threaded through the same call chains a second time.
  virtual ad_utility::BlankNodeManager* getBlankNodeManager() const = 0;
};

#endif  // QLEVER_SRC_INDEX_LOCALVOCABCONTEXT_H
