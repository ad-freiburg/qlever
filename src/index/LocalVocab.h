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
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "index/LocalVocabEntry.h"
#include "util/AllocatorWithLimit.h"
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
  // The type of the sets that store the `LocalVocabEntry`s.
  //
  // NOTE: This derives from `absl::node_hash_set` because we hand out pointers
  // to the `LocalVocabEntry`s and it is hence essential that their addresses
  // remain stable over their lifetime in the hash set. It additionally carries
  // a `MemoryLimitReservation` that accounts for the memory used by its entries
  // against the memory limit (see `allocator_` and `memoryFootprint`). As the
  // sets are shared between `LocalVocab`s (see `mergeWith`), storing the
  // reservation inside the set ensures that the memory is accounted for exactly
  // as long as the set (and thus its entries) is alive.
  struct Set : absl::node_hash_set<LocalVocabEntry> {
    ad_utility::MemoryLimitReservation reservation_;
    explicit Set(ad_utility::MemoryLimitReservation reservation)
        : reservation_{std::move(reservation)} {}
  };

  // The allocator whose memory limit all the sets of this `LocalVocab` are
  // accounted against. By default it is unlimited (see the default
  // constructor), so that `LocalVocab`s that are not associated with a query
  // (e.g. in tests) don't need to know about a limit.
  ad_utility::AllocatorWithLimit<char> allocator_;

  // The primary set of `LocalVocabEntry`s, which can grow dynamically.
  std::shared_ptr<Set> primaryWordSet_;

  using LocalBlankNodeManager =
      ad_utility::BlankNodeManager::LocalBlankNodeManager;

  // The other sets of `LocalVocabEntry`s, which are static.
  absl::flat_hash_set<std::shared_ptr<const Set>> otherWordSets_;

  // The number of words (so that we can compute `size()` in constant time).
  size_t size_ = 0;

  // Each `LocalVocab` has its own `LocalBlankNodeManager` to generate blank
  // nodes when needed (e.g., when parsing the result of a SERVICE query).
  std::shared_ptr<LocalBlankNodeManager> localBlankNodeManager_;

  // Flag to prevent modification after copy.
  std::unique_ptr<std::atomic_bool> copied_ =
      std::make_unique<std::atomic_bool>(false);

 public:
  // Create a new, empty local vocabulary whose memory is not limited. This is
  // used for `LocalVocab`s that are not associated with a query (e.g. in
  // tests).
  LocalVocab() : LocalVocab(ad_utility::makeUnlimitedAllocator<char>()) {}

  // Create a new, empty local vocabulary whose memory is accounted against the
  // limit of the given `allocator`. Typically the allocator is obtained via
  // `QueryExecutionContext::getAllocator()`.
  template <typename T>
  explicit LocalVocab(const ad_utility::AllocatorWithLimit<T>& allocator)
      : allocator_{allocator.template as<char>()},
        primaryWordSet_{makeWordSet()} {}

  // Create a new, empty local vocabulary whose memory is deliberately NOT
  // accounted against any memory limit. For `LocalVocab`s that are part of a
  // query result, prefer the allocator-taking constructor above (or
  // `Operation::makeLocalVocab`). Use this named factory (instead of the
  // default constructor) at the call sites where an unlimited local vocab is
  // used on purpose (e.g. pass-through merges that never add new words, or code
  // outside of query execution), so that these sites can be easily spotted
  // during review.
  static LocalVocab unlimited() { return LocalVocab{}; }

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
    for (const LocalVocab& vocab :
         vocabs | filter(std::not_fn(&LocalVocab::empty))) {
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
  static LocalVocab merge(ql::span<const LocalVocab*> vocabs);

  // Return all the words from all the word sets as a vector. This is useful
  // for testing.
  std::vector<LocalVocabEntry> getAllWordsForTesting() const;

  const Set& primaryWordSet() const { return *primaryWordSet_; }
  const auto& otherSets() const { return otherWordSets_; }

  // This function returns the required data structure to restore the
  // `LocalBlankNodeManager`, e.g. when UPDATEs or cached queries are serialized
  // to disk and the engine is restarted.
  std::vector<LocalBlankNodeManager::OwnedBlocksEntry>
  getOwnedLocalBlankNodeBlocks() const;

  // Restore the state of the `LocalBlankNodeManager` from the serialized data
  // structure obtained from `getOwnedLocalBlankNodeBlocks` above. This must be
  // called at the engine start before any queries that might create new local
  // blank nodes are run (see `BlankNodeManager.h` for details).
  void reserveBlankNodeBlocksFromExplicitIndices(
      const std::vector<LocalBlankNodeManager::OwnedBlocksEntry>& indices,
      ad_utility::BlankNodeManager* blankNodeManager);

  // Get a new BlankNodeIndex using the LocalBlankNodeManager.
  [[nodiscard]] BlankNodeIndex getBlankNodeIndex(
      ad_utility::BlankNodeManager* blankNodeManager);

  // Return true iff the given `blankNodeIndex` is one that was previously
  // generated by the blank node manager of this local vocab.
  bool isBlankNodeIndexContained(BlankNodeIndex blankNodeIndex) const;

  // Return true iff the `lvi` is being kept alive by this `LocalVocab`.
  // NOTE: This function runs in O(size()) and can thereby be only used for
  // small unit tests.
  bool isLocalVocabIndexContained(LocalVocabIndex lvi) const;

  // Wrapper around a bunch of word sets without any access provided to them.
  // This is useful to extend the lifetime of this `LocalVocab` without making
  // this instance read-only by merging and/or cloning.
  class QL_NODISCARD(
      "The sole purpose of this object is to extend "
      "lifetimes.") LifetimeExtender {
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

  // Create a new, empty `Set` with a fresh `MemoryLimitReservation` that draws
  // from the pool of `allocator_`.
  std::shared_ptr<Set> makeWordSet() const {
    return std::make_shared<Set>(
        ad_utility::MemoryLimitReservation{allocator_});
  }

  // Return the amount of memory that the given `entry`, when stored in a `Set`,
  // occupies. This is the size of the node itself (which includes short strings
  // via SSO) plus a fixed overhead for the slot in the hash table, plus the
  // dynamically allocated part of the entry's string (empty for short strings).
  static ad_utility::MemorySize memoryFootprint(const LocalVocabEntry& entry);

  // Common implementation for the two methods `getIndexAndAddIfNotContained`
  // and `getIndexOrNullopt` above.
  template <typename WordT>
  LocalVocabIndex getIndexAndAddIfNotContainedImpl(WordT&& word);
};

#endif  // QLEVER_SRC_ENGINE_LOCALVOCAB_H
