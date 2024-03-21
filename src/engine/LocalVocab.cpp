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
  // TODO<joka921> Make this work with transparent hashing and use try_emplace.
  auto [wordInMapAndIndex, isNewWord] =
      wordsToIndexesMap_.insert(AlignedStr{std::forward<WordT>(word)});
  const auto& wordInMap = *wordInMapAndIndex;
  return &wordInMap;
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
std::optional<LocalVocabIndex> LocalVocab::getIndexOrNullopt(
    const std::string& word) const {
  // TODO<joka921> Maybe we can make this work with transparent hashing,
  // but if this is only a testing API this is probably not worth the hassle.
  auto localVocabIndex = wordsToIndexesMap_.find(AlignedStr{word});
  if (localVocabIndex != wordsToIndexesMap_.end()) {
    return localVocabIndex->second;
  } else {
    return std::nullopt;
  }
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
