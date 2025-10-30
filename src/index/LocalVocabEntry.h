//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_LOCALVOCABENTRY_H
#define QLEVER_SRC_INDEX_LOCALVOCABENTRY_H

#include <atomic>

#include "backports/algorithm.h"
#include "backports/keywords.h"
#include "backports/three_way_comparison.h"
#include "global/TypedIndex.h"
#include "global/VocabIndex.h"
#include "parser/LiteralOrIri.h"
#include "util/CompressedPointer.h"
#include "util/CopyableSynchronization.h"

// Forward declaration
class IndexImpl;

// This is the type we use to store literals and IRIs in the `LocalVocab`.
// It consists of a `LiteralOrIri` and a cache to store the position, where
// the entry would be in the global vocabulary of the Index. This position is
// used for efficient comparisons between entries in the local and global
// vocabulary because we only have to look up the position once per
// `LocalVocabEntry`, and all subsequent comparisons are cheap.
class alignas(16) LocalVocabEntry
    : public ad_utility::triple_component::LiteralOrIri {
 public:
  using Base = ad_utility::triple_component::LiteralOrIri;

  // Note: The values here actually are `Id`s, but we cannot store the `Id` type
  // directly because of cyclic dependencies.
  static constexpr std::string_view proxyTag = "LveIdProxy";
  using IdProxy = ad_utility::TypedIndex<uint64_t, proxyTag>;

 private:
  // The cache for the position in the vocabulary. As usual, the `lowerBound` is
  // inclusive, the `upperBound` is not, so if `lowerBound == upperBound`, then
  // the entry is not part of the globalVocabulary, and `lowerBound` points to
  // the first *larger* word in the vocabulary. Note: we store the cache as
  // three separate atomics to avoid mutexes. The downside is, that in parallel
  // code multiple threads might look up the position concurrently, which wastes
  // a bit of resources. However, we don't consider this case to be likely.
  mutable ad_utility::CopyableAtomic<IdProxy> lowerBoundInVocab_;
  mutable ad_utility::CopyableAtomic<IdProxy> upperBoundInVocab_;

  // Compressed pointer that packs the IndexImpl* (aligned to 64 bytes, so lower
  // 6 bits are zero) with the positionInVocabKnown_ flag in the LSB.
  // This saves 8 bytes compared to storing them separately.
  using CompressedIndexPtr =
      ad_utility::CompressedPointer<const IndexImpl, 64>;
  mutable ad_utility::CopyableAtomic<CompressedIndexPtr> indexAndKnownFlag_;

 public:
  // Constructors that require an IndexImpl pointer.
  // The index must not be nullptr.
  LocalVocabEntry(const Base& base, const IndexImpl* index)
      : Base{base}, indexAndKnownFlag_{CompressedIndexPtr{index, false}} {
    AD_CONTRACT_CHECK(index != nullptr,
                      "LocalVocabEntry requires a non-null IndexImpl pointer");
  }
  LocalVocabEntry(Base&& base, const IndexImpl* index) noexcept
      : Base{std::move(base)},
        indexAndKnownFlag_{CompressedIndexPtr{index, false}} {
    AD_CONTRACT_CHECK(index != nullptr,
                      "LocalVocabEntry requires a non-null IndexImpl pointer");
  }

  // Set the index pointer (used when adding to LocalVocab)
  void setIndex(const IndexImpl* index) const {
    auto current = indexAndKnownFlag_.load(std::memory_order_relaxed);
    indexAndKnownFlag_.store(CompressedIndexPtr{index, current.getFlag()},
                             std::memory_order_relaxed);
  }

  // Get the index pointer
  const IndexImpl* getIndex() const {
    return indexAndKnownFlag_.load(std::memory_order_relaxed).getPointer();
  }

  // Slice to base class `LiteralOrIri`.
  const ad_utility::triple_component::LiteralOrIri& asLiteralOrIri() const {
    return *this;
  }

  // Return the position in the vocabulary. If it is not already cached, then
  // the call to `positionInVocab()` first computes the position and then
  // caches it.
  // Note: We use `lowerBound` and `upperBound` because depending on the Local
  // settings there might be a range of words that are considered equal for the
  // purposes of comparing and sorting them.
  struct PositionInVocab {
    IdProxy lowerBound_;
    IdProxy upperBound_;
  };
  PositionInVocab positionInVocab() const {
    // Immediately return if we have previously computed and cached the
    // position.
    if (indexAndKnownFlag_.load(std::memory_order_acquire).getFlag()) {
      return {lowerBoundInVocab_.load(std::memory_order_relaxed),
              upperBoundInVocab_.load(std::memory_order_relaxed)};
    }
    return positionInVocabExpensiveCase();
  }

  // It suffices to hash the base class `LiteralOrIri` as the position in the
  // vocab is redundant for those purposes.
  template <typename H, typename V>
  friend auto AbslHashValue(H h, const V& entry)
      -> CPP_ret(H)(requires ranges::same_as<V, LocalVocabEntry>) {
    return H::combine(std::move(h), static_cast<const Base&>(entry));
  }

  // Comparison between two entries. We need to ensure the IndexImpl* from
  // LocalVocabEntry is used for the comparison.
  auto compareThreeWay(const LocalVocabEntry& rhs) const {
    // Temporarily set the index on the base LiteralOrIri for comparison
    const IndexImpl* index = getIndex() ? getIndex() : rhs.getIndex();
    // Create temporary copies with the index set
    Base lhsWithIndex = static_cast<const Base&>(*this);
    Base rhsWithIndex = static_cast<const Base&>(rhs);
    const_cast<Base&>(lhsWithIndex).setIndexImpl(index);
    const_cast<Base&>(rhsWithIndex).setIndexImpl(index);
    return ql::compareThreeWay(lhsWithIndex, rhsWithIndex);
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(LocalVocabEntry)

 private:
  // The expensive case of looking up the position in vocab.
  PositionInVocab positionInVocabExpensiveCase() const;
};

#endif  // QLEVER_SRC_INDEX_LOCALVOCABENTRY_H
