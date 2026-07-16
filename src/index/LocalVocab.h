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
  // The primary set of `LocalVocabEntry`s, which can grow dynamically.
  //
  // NOTE: This is a `absl::node_hash_set` because we hand out pointers to
  // the `LocalVocabEntry`s and it is hence essential that their addresses
  // remain stable over their lifetime in the hash set.
  using Set = absl::node_hash_set<LocalVocabEntry>;

  // Accounting of the approximate memory used by one word set against the
  // memory pool registered via `setMemoryPoolForTracking` below (typically the
  // pool of the server's overall memory limit). Bytes are claimed when a word
  // is added to the set and are returned when the set is destroyed. Word sets
  // are shared between `LocalVocab`s via `shared_ptr`s and are freed when the
  // last owner goes away; the accounting piggybacks on exactly that lifetime
  // by storing a `shared_ptr` to this claim in the set's deleter (see
  // `makeWordSet`), so no central management of the entries is needed and the
  // memory of a shared set is counted exactly once. When no pool is registered
  // (index build, most unit tests), all operations are no-ops.
  class MemoryClaim {
   public:
    MemoryClaim() : pool_{memoryPoolForTracking()} {}
    MemoryClaim(const MemoryClaim&) = delete;
    MemoryClaim& operator=(const MemoryClaim&) = delete;
    ~MemoryClaim() {
      if (pool_.has_value() && claimed_ != ad_utility::MemorySize::bytes(0)) {
        pool_.value().ptr()->wlock()->increase(claimed_);
      }
    }
    // Claim `amount` additional bytes. Throws `AllocationExceedsLimitException`
    // if the pool does not have that much memory left.
    void add(ad_utility::MemorySize amount) {
      if (!pool_.has_value()) {
        return;
      }
      pool_.value().ptr()->wlock()->decrease_if_enough_left_or_throw(amount);
      claimed_ += amount;
    }
    // Return `amount` bytes to the pool (e.g. when an insert turned out to be
    // a duplicate).
    void refund(ad_utility::MemorySize amount) {
      if (!pool_.has_value()) {
        return;
      }
      pool_.value().ptr()->wlock()->increase(amount);
      claimed_ -= amount;
    }

   private:
    std::optional<ad_utility::detail::AllocationMemoryLeftThreadsafe> pool_;
    ad_utility::MemorySize claimed_ = ad_utility::MemorySize::bytes(0);
  };

  // Create a word set whose deleter keeps the given claim alive, such that the
  // claimed bytes are returned exactly when the set (and hence the memory of
  // its entries) is freed.
  static std::shared_ptr<Set> makeWordSet(std::shared_ptr<MemoryClaim> claim) {
    return std::shared_ptr<Set>{
        new Set{}, [claim = std::move(claim)](Set* set) { delete set; }};
  }

  // NOTE: The claim has to be declared before the set, because the set's
  // deleter holds a reference to it.
  std::shared_ptr<MemoryClaim> primaryClaim_ = std::make_shared<MemoryClaim>();
  std::shared_ptr<Set> primaryWordSet_ = makeWordSet(primaryClaim_);

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

  // Register the memory pool against which all `LocalVocab`s created from now
  // on account the (approximate) memory of their entries. This is called once
  // at server startup with the pool of the overall memory limit, so that
  // queries that create a lot of local vocabulary (e.g. via `GROUP_CONCAT`)
  // fail with a proper memory-limit error instead of invisibly exhausting the
  // machine's memory. When no pool is registered, no accounting takes place.
  static void setMemoryPoolForTracking(
      std::optional<ad_utility::detail::AllocationMemoryLeftThreadsafe> pool);

 private:
  // The pool registered via `setMemoryPoolForTracking`, or `std::nullopt`.
  static std::optional<ad_utility::detail::AllocationMemoryLeftThreadsafe>&
  memoryPoolForTracking();

  // The approximate number of bytes that the given entry occupies on the
  // heap when stored in the primary set: the entry itself, the heap
  // allocation of its string representation, and a constant for the per-node
  // overhead of the hash set.
  static ad_utility::MemorySize entrySizeEstimate(const LocalVocabEntry& word);

  // Accessors for the primary set.
  Set& primaryWordSet() { return *primaryWordSet_; }

  // Common implementation for the two methods `getIndexAndAddIfNotContained`
  // and `getIndexOrNullopt` above.
  template <typename WordT>
  LocalVocabIndex getIndexAndAddIfNotContainedImpl(WordT&& word);
};

#endif  // QLEVER_SRC_ENGINE_LOCALVOCAB_H
