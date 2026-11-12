// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/LocalVocab.h"

#include "global/Id.h"
#include "util/TransparentFunctors.h"

// _____________________________________________________________________________
LocalVocab LocalVocab::clone() const {
  LocalVocab result;
  result.mergeWith(*this);
  AD_CORRECTNESS_CHECK(result.size_ == size_);
  return result;
}

// _____________________________________________________________________________
LocalVocab LocalVocab::merge(ql::span<const LocalVocab*> vocabs) {
  LocalVocab result;
  result.mergeWith(vocabs | ql::views::transform(ad_utility::dereference));
  return result;
}

// _____________________________________________________________________________
void LocalVocab::mergeWith(const LocalVocab& other) {
  mergeWith(ql::span<const LocalVocab, 1>{&other, 1});
}

// _____________________________________________________________________________
template <typename WordT>
std::pair<LocalVocabIndex, bool> LocalVocab::WordSet::insert(
    const WordT& word) {
  // Look up a view of `word`, which doesn't copy its characters.
  if (auto it = words_.find(LocalVocabEntryView{word}); it != words_.end()) {
    return {ql::to_address(it), false};
  }
  std::string_view representation = word.toStringRepresentation();
  // If the insertion below throws, the characters remain unreachable in the
  // arena, but the set is unchanged.
  auto* characters =
      static_cast<char*>(arena_.allocate(representation.size(), alignof(char)));
  // NOTE: Deliberately use `std::copy` instead of `ql::ranges::copy`, which is
  // not turned into a `std::memmove` by libstdc++ before version 13.4 (GCC bug
  // 116754) and by `range-v3`.
  std::copy(representation.begin(), representation.end(), characters);
  // TODO<RobinTF> The `find` above and the `insert` below both hash the word,
  // and the view is created before it is moved into the set. An
  // `emplace_from`-like operation (a heterogeneous `insert`/`try_emplace` that
  // only constructs the element from the key if it is not yet contained, like
  // `absl`'s `lazy_emplace`) would avoid both, and would also allow to
  // allocate and copy the characters only when actually needed.
  auto [it, isNew] = words_.insert(LocalVocabEntryView{
      word, std::string_view{characters, representation.size()}});
  AD_CORRECTNESS_CHECK(isNew);
  return {ql::to_address(it), true};
}
template std::pair<LocalVocabIndex, bool> LocalVocab::WordSet::insert(
    const LocalVocabEntry&);
template std::pair<LocalVocabIndex, bool> LocalVocab::WordSet::insert(
    const LocalVocabEntryView&);

// _____________________________________________________________________________
template <typename WordT>
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContainedImpl(
    const WordT& word) {
  // We can't modify the word set after it has been copied, otherwise we will
  // silently alter the size of other `LocalVocab`s leading to assertion errors
  // and data races. Please note that this check does not ensure thread safety!
  // If you're concurrently reading and writing to the same local vocab, you
  // still might end up with data races but it helps to find wrong
  // implementations.
  AD_CORRECTNESS_CHECK(!copied_->load());
  auto [index, isNewWord] = primaryWordSet().insert(word);
  size_ += static_cast<size_t>(isNewWord);
  return index;
}

// _____________________________________________________________________________
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContained(
    const LocalVocabEntry& word) {
  return getIndexAndAddIfNotContainedImpl(word);
}

// _____________________________________________________________________________
LocalVocabIndex LocalVocab::getIndexAndAddIfNotContained(
    const LocalVocabEntryView& word) {
  return getIndexAndAddIfNotContainedImpl(word);
}

// _____________________________________________________________________________
template <typename WordT>
Id LocalVocab::getIdAndAddIfNotContainedImpl(const WordT& word) {
  // `positionInVocab` returns a half-open range that is at most one wide, so
  // `lowerBound != upperBound` means that `word` does have a representation
  // outside of a local vocab, and the bits of `lowerBound` then are exactly the
  // corresponding `Id`, e.g. a `VocabIndex` if `word` is in the vocabulary of
  // the index. (A `LocalVocabEntry` never stores an IRI that is encoded
  // directly in an `Id`, see `toIdOrLocalVocabEntry`.)
  auto [lowerBound, upperBound] = word.positionInVocab();
  if (lowerBound != upperBound) {
    return Id::fromBits(lowerBound.get());
  }
  return Id::makeFromLocalVocabIndex(getIndexAndAddIfNotContained(word));
}

// _____________________________________________________________________________
Id LocalVocab::getIdAndAddIfNotContained(const LocalVocabEntry& word) {
  return getIdAndAddIfNotContainedImpl(word);
}

// _____________________________________________________________________________
Id LocalVocab::getIdAndAddIfNotContained(const LocalVocabEntryView& word) {
  return getIdAndAddIfNotContainedImpl(word);
}

// _____________________________________________________________________________
std::optional<LocalVocabIndex> LocalVocab::getIndexOrNullopt(
    const LocalVocabEntry& word) const {
  return getIndexOrNullopt(LocalVocabEntryView{word});
}

// _____________________________________________________________________________
std::optional<LocalVocabIndex> LocalVocab::getIndexOrNullopt(
    const LocalVocabEntryView& word) const {
  auto localVocabIndex = primaryWordSet().find(word);
  if (localVocabIndex != nullptr) {
    return localVocabIndex;
  } else {
    return std::nullopt;
  }
}

// _____________________________________________________________________________
const LocalVocabEntryView& LocalVocab::getWord(
    LocalVocabIndex localVocabIndex) const {
  return *localVocabIndex;
}

// _____________________________________________________________________________
std::vector<LocalVocabEntry> LocalVocab::getAllWordsForTesting() const {
  std::vector<LocalVocabEntry> result;
  auto copyWords = [&result](const Set& set) {
    for (const LocalVocabEntryView& word : set) {
      result.push_back(word.toOwned());
    }
  };
  copyWords(primaryWordSet());
  for (const auto& previous : otherWordSets_) {
    copyWords(*previous);
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

// _____________________________________________________________________________
auto LocalVocab::getOwnedLocalBlankNodeBlocks() const
    -> std::vector<LocalBlankNodeManager::OwnedBlocksEntry> {
  if (!localBlankNodeManager_) {
    return {};
  }
  return localBlankNodeManager_->getOwnedBlockIndices();
}

// _____________________________________________________________________________
void LocalVocab::reserveBlankNodeBlocksFromExplicitIndices(
    const std::vector<LocalBlankNodeManager::OwnedBlocksEntry>& indices,
    ad_utility::BlankNodeManager* blankNodeManager) {
  AD_CONTRACT_CHECK(!localBlankNodeManager_);
  if (indices.empty()) {
    return;
  }
  localBlankNodeManager_ =
      std::make_shared<ad_utility::BlankNodeManager::LocalBlankNodeManager>(
          blankNodeManager);
  localBlankNodeManager_->allocateBlocksFromExplicitIndices(indices);
}

// _____________________________________________________________________________
bool LocalVocab::isLocalVocabIndexContained(LocalVocabIndex lvi) const {
  auto c = [lvi](const auto& set) {
    AD_CONTRACT_CHECK(set != nullptr);
    return ql::ranges::any_of(
        *set, [lvi](const auto& entry) { return &entry == lvi; });
  };
  return c(primaryWordSet_) || ql::ranges::any_of(otherWordSets_, c);
}
