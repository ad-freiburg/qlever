// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include <memory>
#include <set>

#include "JoinNode.h"
#include "QueryGraph.h"
#include "RelationBasic.h"

namespace JoinOrdering {

/**
 *
 * JoinTree as a direction-less binary tree with join operators (BOWTIE, CROSS)
 * as inner nodes and relations as leaf nodes.
 *
 *
 * z.B: let t be join tree (((R1xR2)⋈(R4⋈R5))x(R3))
 *
 *          x
 *         / \
 *        ⋈   R3
 *       / \
 *      /   \
 *     /     \
 *    x       ⋈
 *   / \     / \
 *  R1  R2  R4  R5
 *
 *
 * can be represented as:
 *
 * JoinTree(
 *          JoinTree(
 *                  JoinTree(R1, R2, JoinType::CROSS),
 *                  JoinTree(R4, R5, JoinType::BOWTIE)),
 *          JoinTree(R3), JoinType::CROSS
 *          )
 *
 *
 *
 * ref: 74/637
 * @tparam N N type that satisfies RelationAble concept
 * @see JoinNode
 */
template <typename N>
requires RelationAble<N> class JoinTree {
 public:
  std::shared_ptr<JoinNode<N>> root;
  JoinTree();
  explicit JoinTree(const N& a);
  explicit JoinTree(std::shared_ptr<JoinNode<N>> root);
  JoinTree(std::shared_ptr<JoinNode<N>> left,
           std::shared_ptr<JoinNode<N>> right, JoinType joinType = BOWTIE);
  JoinTree(const N& a, const N& b, JoinType = BOWTIE);

  // TODO: use createJoinTree as described in 149/637
  JoinTree(const JoinTree<N>& t1, const JoinTree<N>& t2, JoinType = BOWTIE);

  [[nodiscard]] std::vector<N> relations_iter() const;
  void relations_iter(std::shared_ptr<JoinNode<N>>, std::vector<N>&) const;

  [[nodiscard]] std::set<std::string> relations_iter_str() const;
  void relations_iter_str(std::shared_ptr<JoinNode<N>>,
                          std::set<std::string>&) const;

  std::string expr();
  std::string expr(std::shared_ptr<JoinNode<N>> r);

  bool isRightDeep();
  bool isRightDeep(std::shared_ptr<JoinNode<N>> r);
  bool isLeftDeep();
  bool isLeftDeep(std::shared_ptr<JoinNode<N>> r);
};

}  // namespace JoinOrdering
