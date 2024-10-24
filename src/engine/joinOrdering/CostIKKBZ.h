// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include <span>

#include "ICostASI.h"
#include "util/HashMap.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> class CostIKKBZ : public ICostASI<N> {
 public:
  ad_utility::HashMap<N, float> rank_m;
  ad_utility::HashMap<N, float> C_m;
  ad_utility::HashMap<N, float> T_m;

  auto rank(const QueryGraph<N>& g, const N& n) -> float;

  /**
   *
   * calculate T for an uncompound relation s_i * n_i
   * (cardinality * selectivity)
   *
   *
   * @param g precedence tree
   * @param n Relation
   * @return T(n)
   */
  auto T(const QueryGraph<N>& g, const N& n) -> float;
  /**
   *
   * a join is called increasing if cost > 1
   * a join is called decreasing if cost < 1
   *
   * ref: 113/637
   *
   * @param g precedence tree
   * @param n Relation
   * @return C(n)
   */
  auto C(const QueryGraph<N>& g, const N& n) -> float;

  /**
   *
   * calculate cost for a sequence of relations
   *
   *
   * C(eps) = 0
   * C(R) = 0 (if R is root)
   * C(R) = h_i * (n_i)
   * C(S_1 S_2) = C(S1) + T(S1) * C(S2)
   *
   * ref: 113/637
   *
   * @param g precedence tree
   * @param seq sequence of relations (may include compound relations)
   * @return C(S_1 S_2)
   */
  auto C(const QueryGraph<N>& g, std::span<N> seq) -> float;
};

}  // namespace JoinOrdering
