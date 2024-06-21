// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <ostream>

#include "global/Id.h"
#include "index/CompressedRelation.h"

// TODO<qup42> some matching comparison operators between
// idtriple/permutedtriple would be nice
// TODO<qup42>: add payload of arbitrary ids
struct IdTriple {
  std::array<Id, 3> ids_;

  explicit IdTriple() : ids_(){};
  explicit IdTriple(
      const CompressedBlockMetadata::PermutedTriple& permutedTriple)
      : ids_({permutedTriple.col0Id_, permutedTriple.col1Id_,
              permutedTriple.col2Id_}){};
  explicit IdTriple(const std::array<Id, 3> ids) : ids_(ids){};
  explicit IdTriple(const Id& col0Id, const Id& col1Id, const Id& col2Id)
      : ids_({col0Id, col1Id, col2Id}){};

  friend std::ostream& operator<<(std::ostream& os, const IdTriple& triple) {
    os << "IdTriple(";
    std::ranges::copy(triple.ids_, std::ostream_iterator<Id>(os, ", "));
    os << ")";
    return os;
  }

  auto operator<=>(const IdTriple&) const = default;

  template <typename H>
  friend H AbslHashValue(H h, const IdTriple& c) {
    return H::combine(std::move(h), c.ids_);
  }

  // TODO<qup42>: should this be a `PermutedTriple`?
  IdTriple permute(const std::array<size_t, 3>& keyOrder) const {
    return IdTriple{ids_[keyOrder[0]], ids_[keyOrder[1]], ids_[keyOrder[2]]};
  }
};
