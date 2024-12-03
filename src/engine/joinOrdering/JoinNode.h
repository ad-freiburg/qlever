// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include "QueryGraph.h"
#include "memory"

namespace JoinOrdering {

/**
 * join operators at inner nodes
 * NATURAL_JOIN, CARTESIAN_JOIN
 */
enum JoinType { BOWTIE, CROSS };  // predicate?

/**
 *
 * JoinTree payload
 *
 * @tparam N type that satisfies RelationAble concept
 * @see JoinTree
 */
template <typename N>
requires RelationAble<N> class JoinNode {
 public:
  N relation;  // TODO: std::optional? inner nodes has no relations
  std::shared_ptr<JoinNode<N>> left, right;  // TODO: std::optional?
  JoinType joinType = BOWTIE;

  explicit JoinNode();
  explicit JoinNode(N relation);
  JoinNode(std::shared_ptr<JoinNode<N>> left,
           std::shared_ptr<JoinNode<N>> right, JoinType joinType_ = BOWTIE);
  /**
   * Leaf node hold relations
   * @return True if node has no children and contains JoinNode::relation info
   */
  bool isLeaf();
};

}  // namespace JoinOrdering
