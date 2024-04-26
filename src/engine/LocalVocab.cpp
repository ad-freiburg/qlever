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
  // TODO<Libc++18> Use std::to_address (more idiomatic, but currently breaks
  // the MacOS build.
  return &(*wordIterator);
}

// _____________________________________________________________________________
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContained(
    const Entry& word) {
  return getIndexAndAddIfNotContainedImpl(word);
}

// _____________________________________________________________________________
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContained(Entry&& word) {
  return getIndexAndAddIfNotContainedImpl(std::move(word));
}

// _____________________________________________________________________________
std::optional<LocalVocabIndex> LocalVocab::getIndexOrNullopt(
    const Entry& word) const {
  auto localVocabIndex = primaryWordSet().find(word);
  if (localVocabIndex != primaryWordSet().end()) {
    // TODO<Libc++18> Use std::to_address (more idiomatic, but currently breaks
    // the MacOS build.
    return &(*localVocabIndex);
  } else {
    return std::nullopt;
  }
}

// _____________________________________________________________________________
const LocalVocab::Entry& LocalVocab::getWord(LocalVocabIndex localVocabIndex) const {
  return *localVocabIndex;
}


// _____________________________________________________________________________
std::vector<LocalVocab::Entry> LocalVocab::getAllWordsForTesting() const {
  std::vector<Entry> result;
  std::ranges::copy(primaryWordSet(), std::back_inserter(result));
  for (const auto& previous : otherWordSets_) {
    std::ranges::copy(*previous, std::back_inserter(result));
  }
  return result;
}
