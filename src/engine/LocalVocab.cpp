// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/LocalVocab.h"

#include "absl/strings/str_cat.h"
#include "global/Id.h"
#include "global/ValueId.h"

// _____________________________________________________________________________
LocalVocab LocalVocab::clone() const {
  LocalVocab localVocabClone;
  // First, make a deep copy of the `absl::node_hash_map` holding the actual
  // map of strings to indexes.
  localVocabClone.wordsToIndexesMap_ = this->wordsToIndexesMap_;
  // The next free index should be the same.
  localVocabClone.nextFreeIndex_ = this->nextFreeIndex_;
  // The map from local ids to strings stores pointers to strings. So we cannot
  // just copy these from `this->indexesToWordsMap_` to `localVocabClone`, but
  // we need to make sure to store the pointers to the strings of the new map
  // `localVocabClone.wordsToIndexesMap_`.
  //
  // NOTE: An alternative algorithm would be to sort the word-index pairs in
  // `wordsToIndexesMap_` by index and then fill `indexesToWordsMap_` in order.
  // This would have better locality, but the sorting takes non-linear time plus
  // the sorting has to handle pairs of `LocalVocabIndex` and `std::string`. So
  // for very large local vocabularies (and only then is this operation
  // performance-criticial at all), the simpler approach below is probably
  // better.
  const size_t localVocabSize = this->size();
  localVocabClone.indexesToWordsMap_.resize(localVocabSize);
  for (const auto& [wordInMap, index] : localVocabClone.wordsToIndexesMap_) {
    AD_CONTRACT_CHECK(index.get() < localVocabSize);
    localVocabClone.indexesToWordsMap_[index.get()] = std::addressof(wordInMap);
  }
  // Return the clone.
  return localVocabClone;
}

// _____________________________________________________________________________
template <typename WordT>
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContainedImpl(WordT&& word) {
  // The following code contains two subtle, but important optimizations:
  //
  // 1. The variant of `insert` used covers the case that `word` was already
  // contained in the map as well as the case that it is newly inserted. This
  // avoids computing the hash for `word` twice in case we see it for the first
  // time (note that hashing a string is not cheap).
  //
  // 2. The fact that we have a member variable `nextFreeIndex_` avoids that we
  // tentatively have to compute the next free ID every time this function is
  // called (even when the ID is not actually needed because the word is already
  // contained in the map).
  //
  auto [wordInMapAndIndex, isNewWord] =
      wordsToIndexesMap_.insert({std::forward<WordT>(word), nextFreeIndex_});
  const auto& [wordInMap, index] = *wordInMapAndIndex;
  if (isNewWord) {
    AD_CONTRACT_CHECK(!readOnly_);
    indexesToWordsMap_.push_back(&wordInMap);
    nextFreeIndex_ = LocalVocabIndex::make(indexesToWordsMap_.size());
  }
  return index;
}

// _____________________________________________________________________________
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContained(
    const std::string& word) {
  return getIndexAndAddIfNotContainedImpl(word);
}

// _____________________________________________________________________________
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContained(std::string&& word) {
  return getIndexAndAddIfNotContainedImpl(std::move(word));
}

// _____________________________________________________________________________
const std::string& LocalVocab::getWord(LocalVocabIndex localVocabIndex) const {
  if (localVocabIndex.get() >= indexesToWordsMap_.size()) {
    throw std::runtime_error(absl::StrCat(
        "LocalVocab error: request for word with local vocab index ",
        localVocabIndex.get(), ", but size of local vocab is only ",
        indexesToWordsMap_.size(), ", please contact the developers"));
  }
  return *(indexesToWordsMap_.at(localVocabIndex.get()));
}
