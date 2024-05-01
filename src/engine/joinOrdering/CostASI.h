// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include <numeric>
#include <span>

#include "QueryGraph.h"

namespace JoinOrdering {  // NOLINT
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
namespace ASI {
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
template <typename N>
requires RelationAble<N> auto rank(QueryGraph<N>& g, const N& n) -> float;

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
template <typename N>
requires RelationAble<N> auto T(QueryGraph<N>& g, const N& n) -> float;

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
template <typename N>
requires RelationAble<N>
auto C(QueryGraph<N>& g, const std::vector<N>& seq) -> float;

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
template <typename N>
requires RelationAble<N> auto C(QueryGraph<N>& g, const N& n) -> float;

}  // namespace ASI
}  // namespace JoinOrdering
