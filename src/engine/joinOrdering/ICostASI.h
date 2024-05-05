// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include "QueryGraph.h"

namespace JoinOrdering {

/**
 * adjacent sequence interchange
 *
 * let A, B two sequence and V, U two non-sequences
 * a cost function C is ASI if the following holds:
 *
 * C(AUVB) <= C(AVUB) <=> rank(U) <= rank(V)
 *
 * ref: 114/637
 */

template <typename N>
requires RelationAble<N> class ICostASI {
 public:
  /**
   * calculate rank ("benefit") for a relation
   *
   * if rank(R2) < rank(R3) then joining
   * (R1 x R2) x R3 is cheaper than
   * (R1 x R3) x R2
   *
   *
   * @param g precedence tree
   * @param n Relation (may be compound relation)
   * @return r(n)
   */
  virtual auto rank(const QueryGraph<N>& g, const N& n) -> float = 0;
};

}  // namespace JoinOrdering
