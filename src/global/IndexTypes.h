//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_INDEXTYPES_H
#define QLEVER_INDEXTYPES_H

#include "./TypedIndex.h"
#include "parser/LiteralOrIri.h"

// Typedefs for several kinds of typed indices that are used across QLever.

// TODO<joka921> Rename `VocabIndex` to `RdfVocabIndex` at a point in time where
// this (very intrusive) renaming doesn't interfere with too many open pull
// requests.
using VocabIndex = ad_utility::TypedIndex<uint64_t, "VocabIndex">;

// TODO<joka921> Move to util library
template <typename T>
class CopyableAtomic : public std::atomic<T> {
  using Base = std::atomic<T>;

 public:
  using Base::Base;
  CopyableAtomic(const CopyableAtomic& rhs) : Base{rhs.load()} {}
  CopyableAtomic& operator=(const CopyableAtomic& rhs) {
    static_cast<Base&>(*this) = rhs.load();
    return *this;
  }
};

class alignas(16) LocalVocabEntry
    : public ad_utility::triple_component::LiteralOrIri {
 private:
  using Base = ad_utility::triple_component::LiteralOrIri;
  mutable CopyableAtomic<VocabIndex> lowerBoundInIndex_;
  mutable CopyableAtomic<VocabIndex> upperBoundInIndex_;
  mutable CopyableAtomic<bool> positionInVocabKnown = false;

 public:
  struct BoundsInIndex {
    VocabIndex lowerBound_;
    VocabIndex upperBound_;
  };
  using Base::Base;
  BoundsInIndex lowerBoundInIndex() const;

  LocalVocabEntry(const Base& base) : Base{base} {}
  LocalVocabEntry(Base&& base) noexcept : Base{std::move(base)} {}

  template <typename H>
  friend H AbslHashValue(H h, const std::same_as<LocalVocabEntry> auto& entry) {
    return AbslHashValue(std::move(h), static_cast<const Base&>(entry));
  }

  auto operator<=>(const LocalVocabEntry& rhs) const {
    return static_cast<const Base&>(*this) <=> static_cast<const Base&>(rhs);
  }
};
using LocalVocabIndex = const LocalVocabEntry*;
using TextRecordIndex = ad_utility::TypedIndex<uint64_t, "TextRecordIndex">;
using WordVocabIndex = ad_utility::TypedIndex<uint64_t, "WordVocabIndex">;
using BlankNodeIndex = ad_utility::TypedIndex<uint64_t, "BlankNodeIndex">;

#endif  // QLEVER_INDEXTYPES_H
