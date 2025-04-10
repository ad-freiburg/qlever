// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_SRC_GLOBAL_IDTRIPLE_H
#define QLEVER_SRC_GLOBAL_IDTRIPLE_H

#include <array>
#include <ostream>

#include "backports/algorithm.h"
#include "global/Id.h"
#include "index/CompressedRelation.h"
#include "index/KeyOrder.h"

template <size_t N = 0>
struct IdTriple {
  // A triple has four components: subject, predicate, object, and graph.
  static constexpr size_t NumCols = 4;

  // For the case of no payload we use an empty struct which is stored at the
  // end of a tuple. That way it will consume no additional space in the struct.
  struct EmptyPayload : ql::ranges::empty_view<Id> {};
  static constexpr size_t PayloadSize = N;
  using Payload = std::conditional_t<(N > 0), std::array<Id, N>, EmptyPayload>;

  // The IDs that define the triple plus some optional payload.
  std::tuple<std::array<Id, NumCols>, Payload> data_;

  // Getters for the IDs and the payload.
  const auto& ids() const { return std::get<0>(data_); }
  auto& ids() { return std::get<0>(data_); }
  const auto& payload() const { return std::get<1>(data_); }
  auto& payload() { return std::get<1>(data_); }

  explicit IdTriple(const std::array<Id, NumCols>& idsIn) requires(N == 0)
      : data_{idsIn, {}} {};

  explicit IdTriple(const std::array<Id, NumCols>& idsIn,
                    const Payload& payload)
      : data_{idsIn, payload} {};

  friend std::ostream& operator<<(std::ostream& os, const IdTriple& triple) {
    os << "IdTriple(";
    ql::ranges::copy(triple.ids(), std::ostream_iterator<Id>(os, ", "));
    if constexpr (N > 0) {
      ql::ranges::copy(triple.payload(), std::ostream_iterator<Id>(os, ", "));
    }
    os << ")";
    return os;
  }

  // Note: The payload is not part of the value representation and therefore not
  // compared.
  std::strong_ordering operator<=>(const IdTriple& other) const {
    return std::lexicographical_compare_three_way(
        ids().begin(), ids().end(), other.ids().begin(), other.ids().end());
  }
  bool operator==(const IdTriple& other) const { return ids() == other.ids(); }

  template <typename H>
  friend H AbslHashValue(H h, const IdTriple& c) {
    return H::combine(std::move(h), c.ids());
  }

  // Permutes the ID of this triple according to the given permutation given by
  // its keyOrder.
  IdTriple<N> permute(const qlever::KeyOrder& keyOrder) const {
    return IdTriple{keyOrder.permute(ids()), payload()};
  }

  CompressedBlockMetadata::PermutedTriple toPermutedTriple() const
      requires(N == 0) {
    static_assert(NumCols == 4);
    return {ids()[0], ids()[1], ids()[2], ids()[3]};
  }
};

// Assert that empty payloads don't make the struct larger
static_assert(sizeof(IdTriple<0>) == IdTriple<0>::NumCols * sizeof(Id));

#endif  // QLEVER_SRC_GLOBAL_IDTRIPLE_H
