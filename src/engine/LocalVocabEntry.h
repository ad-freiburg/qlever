//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <atomic>

#include "global/VocabIndex.h"
#include "parser/LiteralOrIri.h"
#include "util/CopyableSynchronization.h"

// This is the type we use to store literals and IRIs in the `LocalVocab`.
// It consists of a `LiteralOrIri` and a cache to store the position, where
// the entry would be in the global vocabulary of the Index. This position is
// used for efficient comparisons between entries in the local and global
// vocabulary because we only have to lookup the position once per
// `LocalVocabEntry`, and all subsequent comparisons are cheap.
class alignas(16) LocalVocabEntry
    : public ad_utility::triple_component::LiteralOrIri {
 private:
  using Base = ad_utility::triple_component::LiteralOrIri;

  // The cache for the position in the vocabulary. As usual, the `lowerBound` is
  // inclusive, the `upperBound` is not, so if `lowerBound == upperBound`, then
  // the entry is not part of the globalVocabulary, an `lowerBound` points to
  // the first *larger* word in the vocabulary. Note: we store the cache as
  // three separate atomics to avoid mutexes. The downside is, that in parallel
  // code multiple threads might look up the position concurrently, which wastes
  // a bit of resources. We however don't consider this case to be likely.
  mutable ad_utility::CopyableAtomic<VocabIndex> lowerBoundInVocab_;
  mutable ad_utility::CopyableAtomic<VocabIndex> upperBoundInVocab_;
  mutable ad_utility::CopyableAtomic<bool> positionInVocabKnown_ = false;

 public:
  // Inherit the constructors from `LiteralOrIri`
  using Base::Base;

  // Deliberately allow implicit conversion from `LiteralOrIri`.
  explicit(false) LocalVocabEntry(const Base& base) : Base{base} {}
  explicit(false) LocalVocabEntry(Base&& base) noexcept
      : Base{std::move(base)} {}

  // Return the position in the vocabulary. If it is not already cached, then
  // the call to `positionInVocab()` first computes the position and then
  // caches it.
  // Note:: We use `lower` and `upperBound` because depending on the Local
  // settings there might be a range of words that are considered equal for the
  // purposes of comparing and sorting them.
  struct PositionInVocab {
    VocabIndex lowerBound_;
    VocabIndex upperBound_;
  };
  PositionInVocab positionInVocab() const;

  // It suffices to hash the base class `LiteralOrIri` as the position in the
  // vocab is redundant for those purposes.
  template <typename H>
  friend H AbslHashValue(H h, const std::same_as<LocalVocabEntry> auto& entry) {
    return AbslHashValue(std::move(h), static_cast<const Base&>(entry));
  }

  // Comparison between two entries could in theory also be sped up using the
  // cached `position` if it has previously been computed for both of the
  // entries, but it is currently questionable whether this gains much
  // performance.
  auto operator<=>(const LocalVocabEntry& rhs) const {
    return static_cast<const Base&>(*this) <=> static_cast<const Base&>(rhs);
  }
};
