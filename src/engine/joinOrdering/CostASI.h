// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include <numeric>
#include <span>

#include "QueryGraph.h"

namespace JoinOrdering::ASI {
/**
 *
 *
 * if rank(R2) < rank(R3) then joining
 * (R1 x R2) x R3 is cheaper than
 * (R1 x R3) x R2
 *
 *
 * @param g
 * @param n
 * @return
 */
template <typename N>
requires RelationAble<N> auto rank(QueryGraph<N>& g, const N& n) -> float;

template <typename N>
requires RelationAble<N> auto T(QueryGraph<N>& g, const N& n) -> float;

template <typename N>
requires RelationAble<N>
auto C(QueryGraph<N>& g, const std::vector<N>& seq) -> float;

template <typename N>
requires RelationAble<N> auto C(QueryGraph<N>& g, const N& n) -> float;

}  // namespace JoinOrdering::ASI
