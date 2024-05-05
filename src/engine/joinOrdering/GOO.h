// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include <boost/range/join.hpp>

#include "QueryGraph.h"

namespace JoinOrdering {

/**
 *
 * Greedy Operator Ordering
 *
 * Repeatedly combine the pair of relations with the minimal cost
 * until there is only one left
 *
 * ref: 101/637
 * @param g undirected QueryGraph
 * @return bushy join tree
 */
template <typename N>
requires RelationAble<N> auto GOO(QueryGraph<N>& g) -> N;

/**
 *
 * Remove Relation a and Relation b from the QueryGraph and add a new
 * Compound Relation ab with updated weight
 *
 * @param g undirected QueryGraph
 * @param a Relation a
 * @param b Relation b
 * @return newly created compound relation
 */
template <typename N>
requires RelationAble<N>
[[maybe_unused]] N GOO_combine(QueryGraph<N>& g, const N& a, const N& b);

}  // namespace JoinOrdering
