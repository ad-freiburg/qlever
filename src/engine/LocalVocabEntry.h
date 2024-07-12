//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <atomic>

#include "global/VocabIndex.h"
#include "parser/LiteralOrIri.h"

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
  mutable CopyableAtomic<VocabIndex> lowerBoundInVocab_;
  mutable CopyableAtomic<VocabIndex> upperBoundInVocab_;
  mutable CopyableAtomic<bool> positionInVocabKnown = false;

 public:
  struct PositionInVocab {
    VocabIndex lowerBound_;
    VocabIndex upperBound_;
  };
  using Base::Base;
  PositionInVocab positionInVocab() const;

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
