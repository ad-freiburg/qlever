// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "CostCout.h"

namespace JoinOrdering::Cost {

template <typename N>
requires RelationAble<N> float selectivity(
    std::shared_ptr<JoinNode<N>> x, std::shared_ptr<JoinNode<N>> y,
    const std::map<std::string, std::map<std::string, float>>& selectivities) {
  if (!x) return 1.0;
  if (!y) return 1.0;

  if (x->isLeaf() && y->isLeaf())
  //    return selectivities.at(x.get()->relation.getLabel())
  //        .at(y.get()->relation.getLabel());
  {
    auto z = selectivities.at(x.get()->relation.getLabel());
    auto kk = y.get()->relation.getLabel();
    if (!z.contains(kk)) return 1.0;
    auto zz = z.at(kk);  // TODO: get or default
    return zz;
  }

  if (x->isLeaf() && !y->isLeaf())
    return selectivity(x, y->left, selectivities) *
           selectivity(x, y->right, selectivities);

  if (!x->isLeaf() && y->isLeaf())
    return selectivity(y, x->left, selectivities) *
           selectivity(y, x->right, selectivities);

  return selectivity(x->left, y->left, selectivities) *
         selectivity(x->left, y->right, selectivities) *
         selectivity(x->right, y->left, selectivities) *
         selectivity(x->right, y->right, selectivities);
}

// assumes independence of the predicates
// ref: 77/637
template <typename N>
requires RelationAble<N> unsigned long long cardinality(
    std::shared_ptr<JoinNode<N>> n,
    const std::map<std::string, unsigned long long>& cardinalities,
    const std::map<std::string, std::map<std::string, float>>& selectivities) {
  if (n == nullptr) return 1;

  // TODO: log missing relation cardinality
  if (n->isLeaf()) return cardinalities.at(n->relation.getLabel());

  auto l = n->left;
  auto r = n->right;

  if (l && r)
    return cardinality(l, cardinalities, selectivities) *
           cardinality(r, cardinalities, selectivities) *
           selectivity(l, r, selectivities);

  if (l) return cardinality(n->left, cardinalities, selectivities);
  //  if (r) return cardinality(n->right, cardinalities, selectivities);
  return cardinality(n->right, cardinalities, selectivities);

  //  AD_CONTRACT_CHECK("How Did We Get Here?");
  //  return 0;
}

template <typename N>
requires RelationAble<N>
double Cout(const JoinTree<N>& t, const QueryGraph<N>& q) {
  //  q.selectivity
  std::map<std::string, std::map<std::string, float>> qselecm;
  std::map<std::string, unsigned long long> qcards;

  // FIXME: garbage!
  // std::map<N, std::map<N, EdgeInfo>>
  for (auto const& [k, xm] : q.edges_) {
    auto l = k;
    qcards[l.getLabel()] = l.getCardinality();
    for (auto const& [x, xe] : xm) {
      auto r = x;
      if (!xe.hidden) {
        auto s = xe.weight;
        qselecm[l.getLabel()][r.getLabel()] = s;
        qselecm[r.getLabel()][l.getLabel()] = s;
      }
    }
  }

  return Cout(t.root, qcards, qselecm);
}

template <typename N>
requires RelationAble<N> double Cout(
    const JoinTree<N>& t,
    const std::map<std::string, unsigned long long>& cardinalities,
    const std::map<std::string, std::map<std::string, float>>& selectivities) {
  return Cout(t.root, cardinalities, selectivities);
}

// ref: 79/637
template <typename N>
requires RelationAble<N> double Cout(
    std::shared_ptr<JoinNode<N>> n,
    const std::map<std::string, unsigned long long>& cardinalities,
    const std::map<std::string, std::map<std::string, float>>& selectivities) {
  if (n == nullptr) return 0;  // empty join tree, DP table
  if (n->isLeaf()) return 0;

  auto l = n->left;
  auto r = n->right;

  if (l && r)
    return cardinality(n, cardinalities, selectivities) +
           Cout(l, cardinalities, selectivities) +
           Cout(r, cardinalities, selectivities);

  if (l) return Cout(l, cardinalities, selectivities);
  //  if (r) return Cout(r, cardinalities, selectivities);
  return Cout(r, cardinalities, selectivities);
  //  AD_CONTRACT_CHECK("How Did We Get Here?");
  //  return 0;
}

template double Cout(const JoinTree<RelationBasic>& t,
                     const QueryGraph<RelationBasic>& q);

template double Cout(
    const JoinTree<RelationBasic>& t,
    const std::map<std::string, unsigned long long>& cardinalities,
    const std::map<std::string, std::map<std::string, float>>& selectivities);

template double Cout(
    std::shared_ptr<JoinNode<RelationBasic>> n,
    const std::map<std::string, unsigned long long>& cardinalities,
    const std::map<std::string, std::map<std::string, float>>& selectivities);

}  // namespace JoinOrdering::Cost
