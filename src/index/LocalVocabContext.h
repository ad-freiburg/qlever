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

// The interface that a `LocalVocabEntry` requires from the index it belongs to.
// A `LocalVocabEntry` stores a word that has to be comparable to the words in
// the index's vocabulary, no matter whether it is contained in that vocabulary
// or not. It therefore needs to look up where in the vocabulary the word is
// stored, or would be stored, which is what this interface provides.
//
// NOTE: This is deliberately restricted to the operations that
// `LocalVocabEntry` actually performs, and in particular does not expose the
// vocabulary itself. Comparing two `Id`s calls into `LocalVocabEntry` (see
// `global/ValueId.h`), so every library that compares `Id`s depends on
// `LocalVocabEntry`. Keeping this interface abstract and minimal is what allows
// those libraries to be independent of the (much larger) `index` library.
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
  // that is contained in the vocabulary of the index, or the upper and lower
  // bound of a word that is not contained in the vocabulary.
  using IdOrVocabBounds = std::variant<Id, VocabBounds>;

  virtual ~LocalVocabContext();

  // Compare the two given words using the collation of the vocabulary at the
  // `TOTAL` level. Return a value less than, equal to, or greater than zero if
  // `a` is smaller than, equal to, or greater than `b`.
  virtual int compareWords(std::string_view a, std::string_view b) const = 0;

  // Return the bounds of `word` in the vocabulary, see `VocabBounds`.
  virtual VocabBounds getPositionOfWord(std::string_view word) const = 0;

  // Try to encode `word` directly in an `Id` instead of looking it up in the
  // vocabulary (see the `EncodedIriManager`). Return `std::nullopt` if `word`
  // cannot be encoded that way.
  virtual std::optional<Id> encodeAsId(std::string_view word) const = 0;

  // Look up `word` in the vocabularies of this index. Return its `Id` if it is
  // contained in the vocabulary of the main index (an `Id` of type
  // `VocabIndex`), and else the bounds of the position at which it would be
  // sorted into that vocabulary (in which case the two bounds are equal, see
  // `VocabBounds`).
  //
  // NOTE: This function is deliberately not virtual, but implemented in terms
  // of the virtual functions above. It is the single place that knows how the
  // vocabularies of an index play together, so that all its callers agree on
  // that, in particular `LocalVocabEntry::positionInVocabExpensiveCase()` and
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
  // function here that `LocalVocabEntry` itself does not need. It is required
  // when a complete `LocalVocab` is deserialized (see `deserializeLocalVocab`
  // in `util/Serializer/TripleSerializer.h`), which has only this interface at
  // hand, so that the blank node manager would otherwise have to be threaded
  // through the same call chains a second time.
  virtual ad_utility::BlankNodeManager* getBlankNodeManager() const = 0;
};

#endif  // QLEVER_SRC_INDEX_LOCALVOCABCONTEXT_H
