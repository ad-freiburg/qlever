// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/LocalVocab.h"

#include "global/ValueId.h"
#include "util/TransparentFunctors.h"

// _____________________________________________________________________________
LocalVocab LocalVocab::clone() const {
  LocalVocab result;
  result.mergeWith(*this);
  AD_CORRECTNESS_CHECK(result.size_ == size_);
  return result;
}

// _____________________________________________________________________________
LocalVocab LocalVocab::merge(std::span<const LocalVocab*> vocabs) {
  LocalVocab result;
  result.mergeWith(vocabs | ql::views::transform(ad_utility::dereference));
  return result;
}

// _____________________________________________________________________________
void LocalVocab::mergeWith(const LocalVocab& other) {
  mergeWith(std::span<const LocalVocab, 1>{&other, 1});
}

// _____________________________________________________________________________
template <typename WordT>
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContainedImpl(WordT&& word) {
  // We can't modify the word set after it has been copied, otherwise we will
  // silently alter the size of other `LocalVocab`s leading to assertion errors
  // and data races. Please note that this check does not ensure thread safety!
  // If you're concurrently reading and writing to the same local vocab, you
  // still might end up with data races but it helps to find wrong
  // implementations.
  AD_CORRECTNESS_CHECK(!copied_->load());
  auto [wordIterator, isNewWord] = primaryWordSet().insert(AD_FWD(word));
  size_ += static_cast<size_t>(isNewWord);
  // TODO<Libc++18> Use std::to_address (more idiomatic, but currently breaks
  // the MacOS build.
  return &(*wordIterator);
}

// _____________________________________________________________________________
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContained(
    const LocalVocabEntry& word) {
  return getIndexAndAddIfNotContainedImpl(word);
}

// _____________________________________________________________________________
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContained(
    LocalVocabEntry&& word) {
  return getIndexAndAddIfNotContainedImpl(std::move(word));
}

// _____________________________________________________________________________
std::optional<LocalVocabIndex> LocalVocab::getIndexOrNullopt(
    const LocalVocabEntry& word) const {
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
const LocalVocabEntry& LocalVocab::getWord(
    LocalVocabIndex localVocabIndex) const {
  return *localVocabIndex;
}

// _____________________________________________________________________________
std::vector<LocalVocabEntry> LocalVocab::getAllWordsForTesting() const {
  std::vector<LocalVocabEntry> result;
  ql::ranges::copy(primaryWordSet(), std::back_inserter(result));
  for (const auto& previous : otherWordSets_) {
    ql::ranges::copy(*previous, std::back_inserter(result));
  }
  return result;
}

// _____________________________________________________________________________
BlankNodeIndex LocalVocab::getBlankNodeIndex(
    ad_utility::BlankNodeManager* blankNodeManager) {
  AD_CONTRACT_CHECK(blankNodeManager);
  // Initialize the `localBlankNodeManager_` if it doesn't exist yet.
  if (!localBlankNodeManager_) [[unlikely]] {
    localBlankNodeManager_ =
        std::make_shared<ad_utility::BlankNodeManager::LocalBlankNodeManager>(
            blankNodeManager);
  }
  return BlankNodeIndex::make(localBlankNodeManager_->getId());
}

// _____________________________________________________________________________
bool LocalVocab::isBlankNodeIndexContained(
    BlankNodeIndex blankNodeIndex) const {
  if (!localBlankNodeManager_) {
    return false;
  }
  return localBlankNodeManager_->containsBlankNodeIndex(blankNodeIndex.get());
}

// _____________________________________________________________________________
LocalVocab::LifetimeExtender LocalVocab::getLifetimeExtender() const {
  std::vector<std::shared_ptr<const Set>> wordSets;
  wordSets.reserve(otherWordSets_.size() + 1);
  if (!primaryWordSet_->empty()) {
    wordSets.emplace_back(primaryWordSet_);
  }
  ql::ranges::copy(otherWordSets_, std::back_inserter(wordSets));
  return LifetimeExtender{std::move(wordSets)};
}
