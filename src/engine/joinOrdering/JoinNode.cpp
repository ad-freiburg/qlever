// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "JoinNode.h"

#include "RelationBasic.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> JoinNode<N>::JoinNode() {
  //  this->relation = nullptr;
  this->left = nullptr;
  this->right = nullptr;
  this->joinType = BOWTIE;
}

template <typename N>
requires RelationAble<N> JoinNode<N>::JoinNode(N relation) {
  this->relation = relation;
  this->left = nullptr;
  this->right = nullptr;
  this->joinType = BOWTIE;
}

template <typename N>
requires RelationAble<N>
JoinNode<N>::JoinNode(std::shared_ptr<JoinNode<N>> l_,
                      std::shared_ptr<JoinNode<N>> r_, JoinType joinType) {
  //  this->relation = NULL;
  this->left = l_;
  this->right = r_;
  this->joinType = joinType;
}

template <typename N>
requires RelationAble<N> bool JoinNode<N>::isLeaf() {
  // TODO: check for relation (this->relation != nullptr)
  return this->left == nullptr and this->right == nullptr;
}

// template <typename N>
// requires RelationAble<N> JoinNode<N>::JoinNode(N r) {
//   this->relation = r;
//   this->left = nullptr;
//   this->right = nullptr;
// }

// explicit template instantiation
template class JoinNode<RelationBasic>;
}  // namespace JoinOrdering
