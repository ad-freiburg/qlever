// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <vector>

#include "absl/container/node_hash_set.h"
#include "global/Id.h"
#include "parser/LiteralOrIri.h"
#include "util/BlankNodeManager.h"

// A class for maintaining a local vocabulary with contiguous (local) IDs. This
// is meant for words that are not part of the normal vocabulary (constructed
// from the input data at indexing time).
//

class LocalVocab {
 private:
  using Entry = LocalVocabEntry;
  using LiteralOrIri = LocalVocabEntry;
  // A map of the words in the local vocabulary to their local IDs. This is a
  // node hash map because we need the addresses of the words (which are of type
  // `LiteralOrIri`) to remain stable over their lifetime in the hash map
  // because we hand out pointers to them.
  using Set = absl::node_hash_set<LiteralOrIri>;
  std::shared_ptr<Set> primaryWordSet_ = std::make_shared<Set>();

  // Local vocabularies from child operations that were merged into this
  // vocabulary s.t. the pointers are kept alive. They have to be `const`
  // because they are possibly shared concurrently (for example via the cache).
  std::vector<std::shared_ptr<const Set>> otherWordSets_;

  auto& primaryWordSet() { return *primaryWordSet_; }
  const auto& primaryWordSet() const { return *primaryWordSet_; }

  std::shared_ptr<ad_utility::BlankNodeManager::LocalBlankNodeManager>
      localBlankNodeManager_;

 public:
  // Create a new, empty local vocabulary.
  LocalVocab() = default;

  // Prevent accidental copying of a local vocabulary.
  LocalVocab(const LocalVocab&) = delete;
  LocalVocab& operator=(const LocalVocab&) = delete;

  // Make a logical copy. The clone will have an empty primary set so it can
  // safely be modified. The contents are copied as shared pointers to const, so
  // the function runs in linear time in the number of word sets.
  LocalVocab clone() const;

  // Moving a local vocabulary is not problematic (though the typical use case
  // in our code is to copy shared pointers to local vocabularies).
  LocalVocab(LocalVocab&&) = default;
  LocalVocab& operator=(LocalVocab&&) = default;

  // Get the index of a word in the local vocabulary. If the word was already
  // contained, return the already existing index. If the word was not yet
  // contained, add it, and return the new index.
  LocalVocabIndex getIndexAndAddIfNotContained(const LiteralOrIri& word);
  LocalVocabIndex getIndexAndAddIfNotContained(LiteralOrIri&& word);

  // Get the index of a word in the local vocabulary, or std::nullopt if it is
  // not contained. This is useful for testing.
  std::optional<LocalVocabIndex> getIndexOrNullopt(
      const LiteralOrIri& word) const;

  // The number of words in the vocabulary.
  // Note: This is not constant time, but linear in the number of word sets.
  size_t size() const {
    auto result = primaryWordSet().size();
    for (const auto& previous : otherWordSets_) {
      result += previous->size();
    }
    return result;
  }

  // Return true if and only if the local vocabulary is empty.
  bool empty() const { return size() == 0; }

  // Return a const reference to the word.
  const LiteralOrIri& getWord(LocalVocabIndex localVocabIndex) const;

  // Create a local vocab that contains and keeps alive all the words from each
  // of the `vocabs`. The primary word set of the newly created vocab is empty.
  static LocalVocab merge(std::span<const LocalVocab*> vocabs);

  // Merge all passed local vocabs to keep alive all the words from each of the
  // `vocabs`.
  template <std::ranges::range R>
  void mergeWith(const R& vocabs) {
    auto inserter = std::back_inserter(otherWordSets_);
    using std::views::filter;
    for (const auto& vocab : vocabs | filter(std::not_fn(&LocalVocab::empty))) {
      std::ranges::copy(vocab.otherWordSets_, inserter);
      *inserter = vocab.primaryWordSet_;
    }

    // Also merge the `vocabs` `LocalBlankNodeManager`s, if they exist.
    using LocalBlankNodeManager =
        ad_utility::BlankNodeManager::LocalBlankNodeManager;
    using sharedManager = std::shared_ptr<const LocalBlankNodeManager>;

    auto localManagersView =
        vocabs | std::views::transform(
                     [](const LocalVocab& vocab) -> const sharedManager {
                       return vocab.localBlankNodeManager_;
                     });

    auto it = std::ranges::find_if(
        localManagersView, [](const sharedManager& l) { return l != nullptr; });
    if (it == localManagersView.end()) {
      return;
    }
    if (!localBlankNodeManager_) {
      localBlankNodeManager_ =
          std::make_shared<LocalBlankNodeManager>((*it)->blankNodeManager());
    }
    localBlankNodeManager_->mergeWith(localManagersView);
  }

  // Return all the words from all the word sets as a vector.
  std::vector<LiteralOrIri> getAllWordsForTesting() const;

  // Get a new BlankNodeIndex using the LocalBlankNodeManager.
  [[nodiscard]] BlankNodeIndex getBlankNodeIndex(
      ad_utility::BlankNodeManager* blankNodeManager);

  // Return true iff the given `blankNodeIndex` is one that was previously
  // generated by the blank node manager of this local vocab.
  bool isBlankNodeIndexContained(BlankNodeIndex blankNodeIndex) const;

 private:
  // Common implementation for the two variants of
  // `getIndexAndAddIfNotContainedImpl` above.
  template <typename WordT>
  LocalVocabIndex getIndexAndAddIfNotContainedImpl(WordT&& word);
};
