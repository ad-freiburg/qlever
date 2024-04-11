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
LocalVocab LocalVocab::merge(std::span<const LocalVocab*> vocabs) {
  LocalVocab res;
  auto inserter = std::back_inserter(res.otherWordSets_);
  for (const auto* vocab : vocabs) {
    std::ranges::copy(vocab->otherWordSets_, inserter);
    *inserter = vocab->primaryWordSet_;
  }
  return res;
}

// _____________________________________________________________________________
template <typename WordT>
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContainedImpl(WordT&& word) {
  // TODO<joka921> As soon as we store `IdOrString` in the local vocab, we
  // should definitely use `insert` instead of `emplace` here for some
  // transparency optimizations. We currently need `emplace` because of the
  // explicit conversion from `string` to `AlignedString16`.
  auto [wordIterator, isNewWord] =
      primaryWordSet().emplace(std::forward<WordT>(word));
  return std::to_address(wordIterator);
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
    return std::to_address(localVocabIndex);
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
