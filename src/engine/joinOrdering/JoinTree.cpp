// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "JoinTree.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> JoinTree<N>::JoinTree() = default;

template <typename N>
requires RelationAble<N>
JoinTree<N>::JoinTree(std::shared_ptr<JoinNode<N>> r_) : root(r_) {}

template <typename N>
requires RelationAble<N>
JoinTree<N>::JoinTree(std::shared_ptr<JoinNode<N>> left_,
                      std::shared_ptr<JoinNode<N>> right_, JoinType joinType) {
  this->root =
      std::make_shared<JoinNode<N>>(JoinNode<N>(left_, right_, joinType));
}

template <typename N>
requires RelationAble<N>
JoinTree<N>::JoinTree(const N& a, const N& b, JoinType jt)
    : JoinTree(std::make_shared<JoinOrdering::JoinNode<N>>(
                   JoinOrdering::JoinNode(a)),
               std::make_shared<JoinOrdering::JoinNode<N>>(
                   JoinOrdering::JoinNode(b)),
               jt) {}

template <typename N>
requires RelationAble<N> JoinTree<N>::JoinTree(const N& a)
    : JoinTree(std::make_shared<JoinOrdering::JoinNode<N>>(
                   JoinOrdering::JoinNode(a)),
               nullptr) {}

template <typename N>
requires RelationAble<N>
JoinTree<N>::JoinTree(const JoinTree<N>& t1, const JoinTree<N>& t2, JoinType jt)
    : JoinTree<N>(
          std::make_shared<JoinNode<N>>(JoinNode<N>(t1.root, t2.root, jt))) {}

template <typename N>
requires RelationAble<N> std::vector<N> JoinTree<N>::relations_iter() const {
  std::vector<N> erg;
  relations_iter(this->root, erg);
  return erg;
}

template <typename N>
requires RelationAble<N>
void JoinTree<N>::relations_iter(std::shared_ptr<JoinNode<N>> r,
                                 std::vector<N>& s) const {
  if (r == nullptr) return;
  if (r->isLeaf()) s.emplace_back(r.get()->relation);

  relations_iter(r->left, s);
  relations_iter(r->right, s);
}

template <typename N>
requires RelationAble<N>
std::set<std::string> JoinTree<N>::relations_iter_str() const {
  std::set<std::string> erg;
  relations_iter_str(this->root, erg);
  return erg;
}

template <typename N>
requires RelationAble<N>
void JoinTree<N>::relations_iter_str(std::shared_ptr<JoinNode<N>> r,
                                     std::set<string>& s) const {
  if (r == nullptr) return;
  if (r->isLeaf()) s.insert(r.get()->relation.getLabel());

  relations_iter_str(r->left, s);
  relations_iter_str(r->right, s);
}

template <typename N>
requires RelationAble<N> std::string JoinTree<N>::expr() {
  return expr(root);
}

template <typename N>
requires RelationAble<N>
std::string JoinTree<N>::expr(std::shared_ptr<JoinNode<N>> r) {
  if (r == nullptr) return "";
  if (r->isLeaf()) return r.get()->relation.getLabel();

  if (!r->right) return "(" + expr(r->left) + ")";
  if (!r->left) return "(" + expr(r->right) + ")";

  std::string jsymbol = "⋈";
  switch (r->joinType) {
    case BOWTIE:
      jsymbol = "⋈";
      break;
    case CROSS:
      jsymbol = "x";
      break;
    default:
      jsymbol = "?";
      break;
  }
  return "(" + expr(r->left) + jsymbol + expr(r->right) + ")";
}

template <typename N>
requires RelationAble<N> bool JoinTree<N>::isRightDeep() {
  return isRightDeep(this->root);
}

template <typename N>
requires RelationAble<N>
bool JoinTree<N>::isRightDeep(std::shared_ptr<JoinNode<N>> r) {
  if (r == nullptr) return true;
  if (r->left) return false;
  if (!r->left && r->right) return isRightDeep(r->right);
}

template <typename N>
requires RelationAble<N> bool JoinTree<N>::isLeftDeep() {
  return isLeftDeep(this->root);
}

template <typename N>
requires RelationAble<N>
bool JoinTree<N>::isLeftDeep(std::shared_ptr<JoinNode<N>> r) {
  if (r == nullptr) return true;
  if (r->right) return false;
  if (!r->right && r->left) return isLeftDeep(r->left);
}

// explicit template instantiation
template class JoinTree<RelationBasic>;
}  // namespace JoinOrdering
