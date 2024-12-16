// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include "CostIKKBZ.h"
#include "IKKBZ.h"
#include "JoinTree.h"
#include "QueryGraph.h"
#include "RelationBasic.h"

namespace JoinOrdering {

/**
 *
 *  State Space Linearization in Combination with DP
 *
 * Given a medium-sized query(~100 relations) start with a reasonably good
 * (optimal left-deep) relative order for the relations using IKKBZ before
 * applying a Selinger's DP approach to construct optimal bushy join tree
 * (for the given relative order).
 *
 * ref: 5/16
 *
 * @tparam N type that satisfies RelationAble concept
 * @param g acyclic query graph
 * @return optimal bushy join tree for the query Q
 */
template <typename N>
requires RelationAble<N> JoinTree<N> linearizedDP(const QueryGraph<N>& g);

/**
 *
 * @tparam N type that satisfies RelationAble concept
 * @param g acyclic query graph
 * @param t1 non-empty JoinTree
 * @param t2 non-empty JoinTree
 * @return True if both t1 and t2 comply with at least 1 join predicate
 */
template <typename N>
requires RelationAble<N>
bool canJoin(const QueryGraph<N>& g, const JoinTree<N>& t1,
             const JoinTree<N>& t2);

/**
 *
 * for linear trees, assume t2 is a single relation
 *
 *
 * ref: 149/637
 * @tparam N type that satisfies RelationAble concept
 * @param t1 "optimal" join tree
 * @param t2 "optimal" join tree
 * @return optimal join tree (t1 ⋈ t2)
 */
template <typename N>
requires RelationAble<N>
JoinTree<N> createJoinTree(const JoinTree<N>& t1, const JoinTree<N>& t2);
}  // namespace JoinOrdering
