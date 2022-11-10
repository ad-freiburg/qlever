// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/LocalVocab.h"

#include "absl/strings/str_cat.h"
#include "global/Id.h"
#include "global/ValueId.h"

// _____________________________________________________________________________
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContained(
    const std::string& word) {
  // The following code avoids computing the hash for `word` twice in case we
  // see it for the first time (note that hashing a string is not cheap). The
  // return value of the `insert` operation is a pair, where `result.first` is
  // an iterator to the (already existing or newly inserted) key-value pair, and
  // `result.second` is a `bool`, which is `true` if and only if the value was
  // newly inserted.
  auto [wordInMapAndIndex, isNewWord] =
      wordsToIdsMap_.insert({word, nextFreeIndex_});
  const auto& [wordInMap, index] = *wordInMapAndIndex;
  if (isNewWord) {
    idsToWordsMap_.push_back(&wordInMap);
    nextFreeIndex_ = LocalVocabIndex::make(idsToWordsMap_.size());
  }
  return index;
}

// _____________________________________________________________________________
const std::string& LocalVocab::getWord(LocalVocabIndex localVocabIndex) const {
  if (localVocabIndex.get() > idsToWordsMap_.size()) {
    throw std::runtime_error(absl::StrCat(
        "LocalVocab error: request for word with local vocab index ",
        localVocabIndex.get(), ", but size of local vocab is only ",
        idsToWordsMap_.size(), ", please contact the developers"));
  }
  return *(idsToWordsMap_[localVocabIndex.get()]);
}

// _____________________________________________________________________________
std::shared_ptr<LocalVocab> LocalVocab::mergeLocalVocabsIfOneIsEmpty(
    const std::shared_ptr<LocalVocab>& localVocab1,
    const std::shared_ptr<LocalVocab>& localVocab2) {
  AD_CHECK(localVocab1 != nullptr);
  AD_CHECK(localVocab2 != nullptr);
  bool isLocalVocab1Empty = localVocab1->empty();
  bool isLocalVocab2Empty = localVocab2->empty();
  if (!isLocalVocab1Empty && !isLocalVocab2Empty) {
    throw std::runtime_error(
        "Merging of two non-empty local vocabularies is currently not "
        "supported, please contact the developers");
  }
  return !isLocalVocab1Empty ? localVocab1 : localVocab2;
}
