// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_LOCALVOCAB_H
#define QLEVER_SRC_ENGINE_LOCALVOCAB_H

#include <absl/hash/hash.h>

#include <boost/version.hpp>
#if BOOST_VERSION >= 108100
#include <boost/unordered/unordered_flat_set.hpp>
#endif
#if BOOST_VERSION >= 108200
#include <boost/unordered/unordered_node_set.hpp>
#endif
#if BOOST_VERSION < 108200
#include <boost/unordered_set.hpp>
#endif
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "backports/algorithm.h"
#include "backports/memory.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "global/Id.h"
#include "index/LocalVocabEntry.h"
#include "util/AllocatorMemoryResource.h"
#include "util/BlankNodeManager.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"

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
//
// The words of a set are stored as `LocalVocabEntryView`s, which, together with
// their characters, are allocated from a monotonic arena that is owned by the
// set. Words are never removed from a set, so the arena never has to free
// memory before the set is destroyed. A `LocalVocabIndex` is the address of
// such a `LocalVocabEntryView`.
//
// The word sets of a `LocalVocab` (the words as well as the hash tables) are
// allocated with an `AllocatorWithLimit`, typically the one of the
// `QueryExecutionContext`, such that they count toward the memory limit of the
// query.
class LocalVocab {
 public:
  // The allocator for the word sets of a `LocalVocab`.
  using Allocator = ad_utility::AllocatorWithLimit<std::byte>;

  // A set of words, see above.
  class WordSet : ad_utility::NoCopyNoMove {
   private:
    // The resource from which the arena allocates its memory, via an
    // `Allocator`.
    ad_utility::AllocatorMemoryResource<Allocator> upstream_;

    // The arena holds the words, their characters, and the internal arrays of
    // `words_` (the arrays that `words_` replaces when it grows remain in the
    // arena until the set is destroyed). It requests its memory in blocks of
    // geometrically growing size from `upstream_`, so the cost of the memory
    // tracking is negligible.
    //
    // NOTE: The arena must be declared after `upstream_` and before `words_`,
    // because `words_` returns its memory to the arena when it is destroyed,
    // and the arena returns its memory to `upstream_`.
    ql::pmr::monotonic_buffer_resource arena_{&upstream_};

    // Both set types never move their elements, and leave the set unchanged
    // when the insertion of an element throws (in particular, when the memory
    // limit of the allocator is exceeded). `boost::unordered_node_set` is
    // faster, but only available since Boost 1.82.
#if BOOST_VERSION >= 108200
    template <typename... Ts>
    using NodeSet = boost::unordered_node_set<Ts...>;
#else
    template <typename... Ts>
    using NodeSet = boost::unordered_set<Ts...>;
#endif
    using Words = NodeSet<LocalVocabEntryView, absl::Hash<LocalVocabEntryView>,
                          std::equal_to<LocalVocabEntryView>,
                          ql::pmr::polymorphic_allocator<LocalVocabEntryView>>;
    Words words_{Words::allocator_type{&arena_}};

   public:
    // Create an empty set, the memory of which is allocated with `allocator`.
    explicit WordSet(Allocator allocator) : upstream_{std::move(allocator)} {}

    const Allocator& allocator() const { return upstream_.allocator(); }

    // If a word that is equal to `word` (a `LocalVocabEntry` or a
    // `LocalVocabEntryView`) is already contained, return its address and
    // `false`. Else add a copy of `word` and return its address and `true`.
    template <typename WordT>
    std::pair<LocalVocabIndex, bool> insert(const WordT& word);

    // Return the address of the word that is equal to `word`, or `nullptr` if
    // there is no such word.
    LocalVocabIndex find(const LocalVocabEntryView& word) const {
      auto it = words_.find(word);
      return it == words_.end() ? nullptr : ql::to_address(it);
    }

    size_t size() const { return words_.size(); }
    bool empty() const { return words_.empty(); }

    // Iterate over the words (as `const LocalVocabEntryView&`) in no
    // particular order.
    auto begin() const { return words_.begin(); }
    auto end() const { return words_.end(); }
  };

 private:
  using Set = WordSet;

  // A set that leaves itself unchanged when the insertion of an element throws
  // (in particular, when the memory limit of its allocator is exceeded), unlike
  // the hash sets of `absl`. `boost::unordered_flat_set` is faster, but only
  // available since Boost 1.81.
#if BOOST_VERSION >= 108100
  template <typename... Ts>
  using FlatSet = boost::unordered_flat_set<Ts...>;
#else
  template <typename... Ts>
  using FlatSet = boost::unordered_set<Ts...>;
#endif
  using SetPtr = std::shared_ptr<const Set>;
  using OtherWordSets =
      FlatSet<SetPtr, absl::Hash<SetPtr>, std::equal_to<SetPtr>,
              ad_utility::AllocatorWithLimit<SetPtr>>;

  using LocalBlankNodeManager =
      ad_utility::BlankNodeManager::LocalBlankNodeManager;

  // The allocator for the word sets of this `LocalVocab`, see the constructor.
  //
  // NOTE: Must be declared before the members that are allocated with it.
  Allocator allocator_;

  // The primary set of `LocalVocabEntry`s, which can grow dynamically.
  std::shared_ptr<Set> primaryWordSet_ = std::make_shared<Set>(allocator_);

  // The other sets of `LocalVocabEntry`s, which are static.
  //
  // NOTE: The set is stored behind a pointer, because the move assignment of a
  // `boost::unordered_set` (see `FlatSet`) is not `noexcept` if its allocator
  // is not always equal, which would make the move assignment of a
  // `LocalVocab` (and of the classes that contain one, like `ParsedQuery`) not
  // `noexcept` either. Only the (small and empty) set object itself is not
  // allocated with `allocator_`, such that creating a `LocalVocab` never
  // throws because of the memory limit.
  std::unique_ptr<OtherWordSets> otherWordSets_ =
      std::make_unique<OtherWordSets>(
          OtherWordSets::allocator_type{allocator_});

  // The number of words (so that we can compute `size()` in constant time).
  size_t size_ = 0;

  // Each `LocalVocab` has its own `LocalBlankNodeManager` to generate blank
  // nodes when needed (e.g., when parsing the result of a SERVICE query).
  std::shared_ptr<LocalBlankNodeManager> localBlankNodeManager_;

  // Flag to prevent modification after copy.
  std::unique_ptr<std::atomic_bool> copied_ =
      std::make_unique<std::atomic_bool>(false);

 public:
  // Create a new, empty local vocabulary. The word sets (the words as well as
  // the hash tables) are allocated with `allocator`, such that they count
  // toward its memory limit. Adding a word throws if the memory limit would be
  // exceeded (the local vocabulary then remains unchanged).
  //
  // NOTE: Pass the allocator of the query (e.g. the one of the
  // `QueryExecutionContext`) whenever there is one.
  explicit LocalVocab(Allocator allocator) : allocator_{std::move(allocator)} {}

  // Return the allocator of the word sets of this local vocabulary.
  const Allocator& allocator() const { return allocator_; }

  // Prevent accidental copying of a local vocabulary.
  LocalVocab(const LocalVocab&) = delete;
  LocalVocab& operator=(const LocalVocab&) = delete;

  // Make a logical copy, where all sets of `LocalVocabEntry`s become "other"
  // sets, that is, they cannot be modified by the copy. The primary set becomes
  // empty, and the copy uses the same allocator as `this`. This only copies
  // shared pointers and takes time linear in the number of sets.
  LocalVocab clone() const;

  // Moving a local vocabulary is not problematic (though the typical use case
  // in our code is to copy shared pointers from one `LocalVocab` to another).
  LocalVocab(LocalVocab&&) = default;
  LocalVocab& operator=(LocalVocab&&) = default;

  // For a given `LocalVocabEntry`, return the corresponding `LocalVocabIndex`
  // (which is just the address of the stored `LocalVocabEntryView`). If the
  // `LocalVocabEntry` is not contained in the primary set, add a copy of it
  // to the primary set.
  LocalVocabIndex getIndexAndAddIfNotContained(const LocalVocabEntry& word);
  // Overload for words that are already stored in some `LocalVocab`, see
  // above.
  LocalVocabIndex getIndexAndAddIfNotContained(const LocalVocabEntryView& word);

  // Return an `Id` for `word`. If `word` has a representation that doesn't
  // reference a local vocab at all, return that and leave this local vocab
  // unchanged, else behave like `getIndexAndAddIfNotContained`.
  //
  // NOTE: Prefer this whenever the resulting `Id`s are compared bitwise or
  // stored persistently. A word that is constructed at runtime (e.g. via
  // `CONCAT` or `IRI`) resides in a `LocalVocabEntry` even if it is also part
  // of the vocabulary of the index, and would then get a different `Id` than
  // the same word coming from the data.
  Id getIdAndAddIfNotContained(const LocalVocabEntry& word);
  Id getIdAndAddIfNotContained(const LocalVocabEntryView& word);

  // Like `getIndexAndAddIfNotContained`, but if the `LocalVocabEntry` is not
  // contained in any of the sets, do not add it and return `std::nullopt`.
  std::optional<LocalVocabIndex> getIndexOrNullopt(
      const LocalVocabEntry& word) const;
  std::optional<LocalVocabIndex> getIndexOrNullopt(
      const LocalVocabEntryView& word) const;

  // The number of words in this local vocabulary.
  size_t size() const {
    if constexpr (ad_utility::areExpensiveChecksEnabled) {
      auto size = primaryWordSet().size();
      for (const auto& previous : *otherWordSets_) {
        size += previous->size();
      }
      AD_CORRECTNESS_CHECK(size == size_);
    }
    return size_;
  }

  // Return true if and only if the local vocabulary is empty.
  bool empty() const { return size() == 0; }

  // The number of set stores (primary set and other sets).
  size_t numSets() const { return 1 + otherWordSets_->size(); }

  // Get the `LocalVocabEntry` corresponding to the given `LocalVocabIndex`.
  //
  // NOTE: This used to be a more complex function but is now a simple
  // dereference. It could be thrown out in the future.
  const LocalVocabEntryView& getWord(LocalVocabIndex localVocabIndex) const;

  // Add all sets (primary and other) of the given local vocabs as other sets
  // to this local vocab. The purpose is to keep all the contained
  // `LocalVocabEntry`s alive as long as this `LocalVocab` is alive. The
  // primary set of this `LocalVocab` remains unchanged.
  CPP_template(typename R)(requires ql::ranges::range<R>)
  void mergeWith(const R& vocabs) {
    using ql::views::filter;
    auto addWordSet = [this](const std::shared_ptr<const Set>& set) {
      if (set == primaryWordSet_) {
        return;
      }
      bool added = otherWordSets_->insert(set).second;
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
      ql::ranges::for_each(*vocab.otherWordSets_, addWordSet);
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
  // of all sets (primary and other) of the given local vocabs, which uses
  // `allocator`.
  static LocalVocab merge(ql::span<const LocalVocab*> vocabs,
                          Allocator allocator);

  // Return all the words from all the word sets as a vector. This is useful
  // for testing.
  std::vector<LocalVocabEntry> getAllWordsForTesting() const;

  const Set& primaryWordSet() const { return *primaryWordSet_; }
  const auto& otherSets() const { return *otherWordSets_; }

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

  // Common implementation of the overloads of `getIndexAndAddIfNotContained`
  // and `getIdAndAddIfNotContained` above.
  template <typename WordT>
  LocalVocabIndex getIndexAndAddIfNotContainedImpl(const WordT& word);
  template <typename WordT>
  Id getIdAndAddIfNotContainedImpl(const WordT& word);
};

#endif  // QLEVER_SRC_ENGINE_LOCALVOCAB_H
