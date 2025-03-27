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
  // A triple has four components: subject, predicate, object, and graph.
  static constexpr size_t NumCols = 4;
  // The three IDs that define the triple.
  std::array<Id, NumCols> ids_;
  // Some additional payload of the triple, e.g. which graph it belongs to.
  std::array<Id, N> payload_;

  explicit IdTriple(const std::array<Id, NumCols>& ids) requires(N == 0)
      : ids_(ids), payload_(){};

  explicit IdTriple(const std::array<Id, NumCols>& ids,
                    const std::array<Id, N>& payload) requires(N != 0)
      : ids_(ids), payload_(payload){};

  friend std::ostream& operator<<(std::ostream& os, const IdTriple& triple) {
    os << "IdTriple(";
    ql::ranges::copy(triple.ids_, std::ostream_iterator<Id>(os, ", "));
    ql::ranges::copy(triple.payload_, std::ostream_iterator<Id>(os, ", "));
    os << ")";
    return os;
  }

  // TODO: use `= default` once we drop Clang 16 with `libc++16`.
  std::strong_ordering operator<=>(const IdTriple& other) const {
    static_assert(NumCols == 4);
    return std::tie(ids_[0], ids_[1], ids_[2], ids_[3]) <=>
           std::tie(other.ids_[0], other.ids_[1], other.ids_[2], other.ids_[3]);
  }
  bool operator==(const IdTriple& other) const = default;

  template <typename H>
  friend H AbslHashValue(H h, const IdTriple& c) {
    return H::combine(std::move(h), c.ids_, c.payload_);
  }

  // Permutes the ID of this triple according to the given permutation given by
  // its keyOrder.
  IdTriple<N> permute(const std::array<size_t, 3>& keyOrder) const {
    std::array<Id, NumCols> newIds{ids_[keyOrder[0]], ids_[keyOrder[1]],
                                   ids_[keyOrder[2]], ids_[3]};
    if constexpr (N == 0) {
      return IdTriple<N>(newIds);
    } else {
      return IdTriple<N>(newIds, payload_);
    }
  }

  CompressedBlockMetadata::PermutedTriple toPermutedTriple() const
      requires(N == 0) {
    static_assert(NumCols == 4);
    return {ids_[0], ids_[1], ids_[2], ids_[3]};
  }
};
