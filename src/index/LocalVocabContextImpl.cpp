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

// _____________________________________________________________________________
VocabIndex LocalVocabContextImpl::getBeginOfIrisInVocabulary() const {
  uint64_t beginOfIris = beginOfIris_.load();
  if (beginOfIris == beginOfIrisNotComputed_) {
    // `TripleComponentComparator` first compares the first characters bytewise,
    // so all literals (`"...`) come before all IRIs (`<...>`). A binary search
    // on that character finds the boundary independently of the collation.
    // Computing this concurrently is harmless, all threads store the same
    // value.
    size_t lower = 0;
    size_t upper = vocabulary_->size();
    while (lower < upper) {
      size_t middle = lower + (upper - lower) / 2;
      // Compare as `unsigned char`, like the `std::strncmp` in the comparator.
      const auto& word = (*vocabulary_)[VocabIndex::make(middle)];
      if (word.empty() || static_cast<unsigned char>(word.front()) <
                              static_cast<unsigned char>('<')) {
        lower = middle + 1;
      } else {
        upper = middle;
      }
    }
    beginOfIris = lower;
    beginOfIris_.store(beginOfIris);
  }
  return VocabIndex::make(beginOfIris);
}
