// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/LocalVocabContextImpl.h"

#include "util/Exception.h"

// ____________________________________________________________________________
int LocalVocabContextImpl::compareWords(std::string_view a,
                                        std::string_view b) const {
  return vocabulary_->getCaseComparator().compare(a, b,
                                                  LocaleManager::Level::TOTAL);
}

// ____________________________________________________________________________
auto LocalVocabContextImpl::getPositionOfWord(std::string_view word) const
    -> VocabBounds {
  return vocabulary_->getPositionOfWord(word);
}

// ____________________________________________________________________________
bool LocalVocabContextImpl::hasAuxVocabulary() const {
  return *auxVocab_ != nullptr;
}

// ____________________________________________________________________________
std::optional<AuxVocabIndex> LocalVocabContextImpl::getAuxVocabIndex(
    std::string_view word) const {
  if (!hasAuxVocabulary()) {
    return std::nullopt;
  }
  return (*auxVocab_)->getId(word);
}

// ____________________________________________________________________________
std::optional<Id> LocalVocabContextImpl::encodeAsId(
    std::string_view word) const {
  return encodedIriManager_->encode(word);
}

// ____________________________________________________________________________
ad_utility::BlankNodeManager* LocalVocabContextImpl::getBlankNodeManager()
    const {
  AD_CONTRACT_CHECK(*blankNodeManager_);
  return blankNodeManager_->get();
}
