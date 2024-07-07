// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <array>
#include <ostream>

#include "global/Id.h"
#include "index/CompressedRelation.h"

template <size_t N = 0>
struct IdTriple {
  // The three IDs that define the triple.
  std::array<Id, 3> ids_;
  // Some additional payload of the triple, e.g. which graph it belongs to.
  std::array<Id, N> payload_;

  IdTriple() = delete;

  explicit IdTriple(const std::array<Id, 3>& ids) requires(N == 0)
      : ids_(ids), payload_(){};

  explicit IdTriple(const std::array<Id, 3>& ids,
                    const std::array<Id, N>& payload) requires(N != 0)
      : ids_(ids), payload_(payload){};

  friend std::ostream& operator<<(std::ostream& os, const IdTriple& triple) {
    os << "IdTriple(";
    std::ranges::copy(triple.ids_, std::ostream_iterator<Id>(os, ", "));
    std::ranges::copy(triple.payload_, std::ostream_iterator<Id>(os, ", "));
    os << ")";
    return os;
  }

  auto operator<=>(const IdTriple&) const = default;

  template <typename H>
  friend H AbslHashValue(H h, const IdTriple& c) {
    return H::combine(std::move(h), c.ids_, c.payload_);
  }

  // Permutes the ID of this triple according to the given permutation given by
  // its keyOrder.
  IdTriple<N> permute(const std::array<size_t, 3>& keyOrder) const {
    std::array<Id, 3> newIds{ids_[keyOrder[0]], ids_[keyOrder[1]],
                             ids_[keyOrder[2]]};
    if constexpr (N == 0) {
      return IdTriple<N>(newIds);
    } else {
      return IdTriple<N>(newIds, payload_);
    }
  }

  CompressedBlockMetadata::PermutedTriple toPermutedTriple() const
      requires(N == 0) {
    return {ids_[0], ids_[1], ids_[2]};
  }
};
