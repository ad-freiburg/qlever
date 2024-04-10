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
  localVocabClone.otherWordSets_ = otherWordSets_;
  localVocabClone.otherWordSets_.push_back(primaryWordSet_);
  // Return the clone.
  return localVocabClone;
}

// _____________________________________________________________________________
LocalVocab LocalVocab::merge(const LocalVocab& a, const LocalVocab& b) {
  LocalVocab res;
  auto inserter = std::back_inserter(res.otherWordSets_);
  std::ranges::copy(a.otherWordSets_, inserter);
  std::ranges::copy(b.otherWordSets_, inserter);
  *inserter = a.primaryWordSet_;
  *inserter = b.primaryWordSet_;
  return res;
}

// _____________________________________________________________________________
template <typename WordT>
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContainedImpl(WordT&& word) {
  // The following code contains a subtle, but important optimizations:
  //
  // 1. The variant of `insert` used covers the case that `word` was already
  // contained in the map as well as the case that it is newly inserted. This
  // avoids computing the hash for `word` twice in case we see it for the first
  // time (note that hashing a string is not cheap).
  // TODO<joka921> Make this work with transparent hashing and use try_emplace.
  auto [wordIterator, isNewWord] =
      primaryWordSet().insert(StringAligned16{std::forward<WordT>(word)});
  return std::addressof(*wordIterator);
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
  auto localVocabIndex = primaryWordSet().find(StringAligned16{word});
  if (localVocabIndex != primaryWordSet().end()) {
    return &(*localVocabIndex);
  } else {
    return std::nullopt;
  }
}

// _____________________________________________________________________________
const std::string& LocalVocab::getWord(LocalVocabIndex localVocabIndex) const {
  return *localVocabIndex;
}

// _____________________________________________________________________________
std::vector<std::string> LocalVocab::getAllWordsForTesting() const {
  std::vector<std::string> result;
  std::ranges::copy(primaryWordSet(), std::back_inserter(result));
  for (const auto& previous : otherWordSets_) {
    std::ranges::copy(*previous, std::back_inserter(result));
  }
  return result;
}
