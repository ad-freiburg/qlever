// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/LocalVocabContextImpl.h"

#include "index/LocalVocabEntry.h"
#include "util/Exception.h"

// _____________________________________________________________________________
int LocalVocabContextImpl::compareWords(std::string_view a,
                                        std::string_view b) const {
  return vocabulary_->getCaseComparator().compare(a, b,
                                                  LocaleManager::Level::TOTAL);
}

// _____________________________________________________________________________
auto LocalVocabContextImpl::getPositionOfWord(std::string_view word) const
    -> VocabBounds {
  return vocabulary_->getPositionOfWord(word);
}

// _____________________________________________________________________________
std::string LocalVocabContextImpl::getWordOfStringTypedId(Id id) const {
  switch (id.getDatatype()) {
    case Datatype::VocabIndex:
      return std::string{(*vocabulary_)[id.getVocabIndex()]};
    case Datatype::SecondaryVocabIndex:
      AD_CORRECTNESS_CHECK(
          hasSecondaryVocabulary(),
          "An `Id` of type `SecondaryVocabIndex` can only belong to an index "
          "that has a secondary vocabulary");
      return std::string{(**secondaryVocab_)[id.getSecondaryVocabIndex()]};
    case Datatype::LocalVocabIndex:
      return id.getLocalVocabIndex()->toStringRepresentation();
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
int LocalVocabContextImpl::compareSecondaryVocabIdTo(Id secondaryId,
                                                     Id other) const {
  // NOTE: The explicit check is required, because the `*secondaryVocab_` that
  // the calls below dereference would already be undefined behavior if there is
  // no secondary vocabulary, no matter in which order the arguments of those
  // calls are evaluated.
  AD_CORRECTNESS_CHECK(hasSecondaryVocabulary());
  auto index = secondaryId.getSecondaryVocabIndex();
  // If both words are stored in the secondary vocabulary, then compare them by
  // their indices, which spares us materializing the word of `other` as a
  // `std::string`.
  if (other.getDatatype() == Datatype::SecondaryVocabIndex) {
    return (*secondaryVocab_)
        ->compareWords(index, other.getSecondaryVocabIndex());
  }
  return (*secondaryVocab_)
      ->compareWordTo(index, getWordOfStringTypedId(other));
}

// _____________________________________________________________________________
int LocalVocabContextImpl::compareIdsSemantically(Id a, Id b) const {
  // Delegate a comparison that involves a word of the secondary vocabulary to
  // that vocabulary, which is the class that knows how to compare its words.
  if (a.getDatatype() == Datatype::SecondaryVocabIndex) {
    return compareSecondaryVocabIdTo(a, b);
  }
  if (b.getDatatype() == Datatype::SecondaryVocabIndex) {
    // The call compares the word of `b` to the word of `a`, which is the
    // reverse of what we need, so negate the result.
    return -compareSecondaryVocabIdTo(b, a);
  }
  return compareWords(getWordOfStringTypedId(a), getWordOfStringTypedId(b));
}

// _____________________________________________________________________________
auto LocalVocabContextImpl::getSemanticPositionInMainVocab(Id id) const
    -> VocabBounds {
  // A word that is stored in the vocabulary of the main index sits at exactly
  // the position of its `Id`, so we can shortcut the lookup.
  if (id.getDatatype() == Datatype::VocabIndex) {
    auto index = id.getVocabIndex();
    return {index, VocabIndex::make(index.get() + 1)};
  }
  return getPositionOfWord(getWordOfStringTypedId(id));
}

// _____________________________________________________________________________
bool LocalVocabContextImpl::hasSecondaryVocabulary() const {
  return *secondaryVocab_ != nullptr;
}

// _____________________________________________________________________________
std::optional<SecondaryVocabIndex>
LocalVocabContextImpl::getSecondaryVocabIndex(std::string_view word) const {
  if (!hasSecondaryVocabulary()) {
    return std::nullopt;
  }
  return (*secondaryVocab_)->getId(word);
}

// _____________________________________________________________________________
std::optional<Id> LocalVocabContextImpl::encodeAsId(
    std::string_view word) const {
  return encodedIriManager_->encode(word);
}

// _____________________________________________________________________________
ad_utility::BlankNodeManager* LocalVocabContextImpl::getBlankNodeManager()
    const {
  AD_CONTRACT_CHECK(*blankNodeManager_);
  return blankNodeManager_->get();
}
