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
// The only implementation of this interface is in `IndexImpl`. There must be
// exactly one instance of it per index, because
// `LocalVocabEntry::compareThreeWay` checks that two entries belong to the same
// index by comparing the context POINTERS.
class LocalVocabContext {
 public:
  // The bounds of the range in the vocabulary in which a word is stored, or in
  // which it would be stored if it is not contained. As usual, the lower bound
  // is inclusive and the upper bound is not, so `first == second` means that
  // the word is not contained in the vocabulary.
  using VocabBounds = std::pair<VocabIndex, VocabIndex>;

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

  // Return the manager for the blank nodes of this index. NOTE: This is the one
  // function here that `LocalVocabEntry` itself does not need. It is required
  // when a complete `LocalVocab` is deserialized (see `deserializeLocalVocab`
  // in `util/Serializer/TripleSerializer.h`), which has only this interface at
  // hand, so that the blank node manager would otherwise have to be threaded
  // through the same call chains a second time.
  virtual ad_utility::BlankNodeManager* getBlankNodeManager() const = 0;
};

#endif  // QLEVER_SRC_INDEX_LOCALVOCABCONTEXT_H
