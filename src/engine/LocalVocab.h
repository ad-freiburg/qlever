// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_LOCALVOCAB_H
#define QLEVER_SRC_ENGINE_LOCALVOCAB_H

#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_set.h>

#include <cstdlib>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "index/LocalVocabEntry.h"
#include "util/BlankNodeManager.h"
#include "util/Exception.h"

// A class for maintaining a local vocabulary, which conceptually is a set of
// `LiteralOrIri`s that are not part of the original vocabulary (which stems
// from the input data). The implementation is subtle and quite clever:
//
// The entries of the local vocabulary are `LocalVocabEntry`s, each of which
// holds a `LiteralOrIri` and remembers its position in the original vocabulary
// after it has been computed once.
//
// A `LocalVocab` has a primary set of `LocalVocabEntry`s, which can grow
// dynamically, and a collection of other sets of `LocalVocabEntry`s, which
// cannot be modified by this class. A `LocalVocabEntry` lives exactly as long
// as it is contained in at least one of the (primary or other) sets of a
// `LocalVocab`.
class LocalVocab {
 private:
  // The primary set of `LocalVocabEntry`s, which can grow dynamically.
  //
  // NOTE: This is a `absl::node_hash_set` because we hand out pointers to
  // the `LocalVocabEntry`s and it is hence essential that their addresses
  // remain stable over their lifetime in the hash set.
  using Set = absl::node_hash_set<LocalVocabEntry>;
  std::shared_ptr<Set> primaryWordSet_ = std::make_shared<Set>();

  // The other sets of `LocalVocabEntry`s, which are static.
  absl::flat_hash_set<std::shared_ptr<const Set>> otherWordSets_;

  // The number of words (so that we can compute `size()` in constant time).
  size_t size_ = 0;

  // Each `LocalVocab` has its own `LocalBlankNodeManager` to generate blank
  // nodes when needed (e.g., when parsing the result of a SERVICE query).
  std::shared_ptr<ad_utility::BlankNodeManager::LocalBlankNodeManager>
      localBlankNodeManager_;

  // Flag to prevent modification after copy.
  std::unique_ptr<std::atomic_bool> copied_ =
      std::make_unique<std::atomic_bool>(false);

 public:
  // Create a new, empty local vocabulary.
  LocalVocab() = default;

  // Prevent accidental copying of a local vocabulary.
  LocalVocab(const LocalVocab&) = delete;
  LocalVocab& operator=(const LocalVocab&) = delete;

  // Make a logical copy, where all sets of `LocalVocabEntry`s become "other"
  // sets, that is, they cannot be modified by the copy. The primary set becomes
  // empty. This only copies shared pointers and takes time linear in the number
  // of sets.
  LocalVocab clone() const;

  // Moving a local vocabulary is not problematic (though the typical use case
  // in our code is to copy shared pointers from one `LocalVocab` to another).
  LocalVocab(LocalVocab&&) = default;
  LocalVocab& operator=(LocalVocab&&) = default;

  // For a given `LocalVocabEntry`, return the corresponding `LocalVocabIndex`
  // (which is just the address of the `LocalVocabEntry`). If the
  // `LocalVocabEntry` is not contained in any of the sets, add it to the
  // primary.
  LocalVocabIndex getIndexAndAddIfNotContained(const LocalVocabEntry& word);
  LocalVocabIndex getIndexAndAddIfNotContained(LocalVocabEntry&& word);

  // Like `getIndexAndAddIfNotContained`, but if the `LocalVocabEntry` is not
  // contained in any of the sets, do not add it and return `std::nullopt`.
  std::optional<LocalVocabIndex> getIndexOrNullopt(
      const LocalVocabEntry& word) const;

  // The number of words in this local vocabulary.
  size_t size() const {
    if constexpr (ad_utility::areExpensiveChecksEnabled) {
      auto size = primaryWordSet().size();
      for (const auto& previous : otherWordSets_) {
        size += previous->size();
      }
      AD_CORRECTNESS_CHECK(size == size_);
    }
    return size_;
  }

  // Return true if and only if the local vocabulary is empty.
  bool empty() const { return size() == 0; }

  // The number of set stores (primary set and other sets).
  size_t numSets() const { return 1 + otherWordSets_.size(); }

  // Get the `LocalVocabEntry` corresponding to the given `LocalVocabIndex`.
  //
  // NOTE: This used to be a more complex function but is now a simple
  // dereference. It could be thrown out in the future.
  const LocalVocabEntry& getWord(LocalVocabIndex localVocabIndex) const;

  // Add all sets (primary and other) of the given local vocabs as other sets
  // to this local vocab. The purpose is to keep all the contained
  // `LocalVocabEntry`s alive as long as this `LocalVocab` is alive. The
  // primary set of this `LocalVocab` remains unchanged.
  CPP_template(typename R)(requires ql::ranges::range<R>) void mergeWith(
      const R& vocabs) {
    using ql::views::filter;
    auto addWordSet = [this](const std::shared_ptr<const Set>& set) {
      if (set == primaryWordSet_) {
        return;
      }
      bool added = otherWordSets_.insert(set).second;
      size_ += static_cast<size_t>(added) * set->size();
    };
    // Note: Even though the `otherWordsSet_`is a hash set that filters out
    // duplicates, we still manually filter out empty sets, because these
    // typically don't compare equal to each other because of the`shared_ptr`
    // semantics.
    for (const auto& vocab : vocabs | filter(std::not_fn(&LocalVocab::empty))) {
      // Mark vocab as copied
      vocab.copied_->store(true);
      ql::ranges::for_each(vocab.otherWordSets_, addWordSet);
      addWordSet(vocab.primaryWordSet_);
    }

    // Also merge the `vocabs` `LocalBlankNodeManager`s, if they exist.
    using LocalBlankNodeManager =
        ad_utility::BlankNodeManager::LocalBlankNodeManager;
    auto localManagersView =
        vocabs |
        ql::views::transform([](const LocalVocab& vocab) -> const auto& {
          return vocab.localBlankNodeManager_;
        });

    auto it = ql::ranges::find_if(localManagersView,
                                  [](const auto& l) { return l != nullptr; });
    if (it == localManagersView.end()) {
      return;
    }
    if (!localBlankNodeManager_) {
      localBlankNodeManager_ =
          std::make_shared<LocalBlankNodeManager>((*it)->blankNodeManager());
    }
    localBlankNodeManager_->mergeWith(localManagersView);
  }

  // Convenience function for a single `LocalVocab`.
  void mergeWith(const LocalVocab& other);

  // Create a new local vocab with empty set and other sets that are the union
  // of all sets (primary and other) of the given local vocabs.
  static LocalVocab merge(std::span<const LocalVocab*> vocabs);

  // Return all the words from all the word sets as a vector. This is useful
  // for testing.
  std::vector<LocalVocabEntry> getAllWordsForTesting() const;

  const Set& primaryWordSet() const { return *primaryWordSet_; }

  // Get a new BlankNodeIndex using the LocalBlankNodeManager.
  [[nodiscard]] BlankNodeIndex getBlankNodeIndex(
      ad_utility::BlankNodeManager* blankNodeManager);

  // Return true iff the given `blankNodeIndex` is one that was previously
  // generated by the blank node manager of this local vocab.
  bool isBlankNodeIndexContained(BlankNodeIndex blankNodeIndex) const;

  // Wrapper around a bunch of word sets without any access provided to them.
  // This is useful to extend the lifetime of this `LocalVocab` without making
  // this instance read-only by merging and/or cloning.
  class [[nodiscard(
      "The sole purpose of this object is to extend "
      "lifetimes.")]] LifetimeExtender {
    friend LocalVocab;
    std::vector<std::shared_ptr<const Set>> wordSets_;

    explicit LifetimeExtender(std::vector<std::shared_ptr<const Set>> wordSets)
        : wordSets_{std::move(wordSets)} {}
  };

  // Return a `LifetimeExtender` for all the words stored by this `LocalVocab`.
  // You can safely keep writing to this `LocalVocab` after acquiring it.
  LifetimeExtender getLifetimeExtender() const;

 private:
  // Accessors for the primary set.
  Set& primaryWordSet() { return *primaryWordSet_; }

  // Common implementation for the two methods `getIndexAndAddIfNotContained`
  // and `getIndexOrNullopt` above.
  template <typename WordT>
  LocalVocabIndex getIndexAndAddIfNotContainedImpl(WordT&& word);
};

#endif  // QLEVER_SRC_ENGINE_LOCALVOCAB_H
