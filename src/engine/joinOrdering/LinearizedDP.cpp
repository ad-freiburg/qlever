// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "LinearizedDP.h"

#include "CostCout.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> JoinTree<N> linearizedDP(const QueryGraph<N>& g) {
  // find a linearization using IKKBZ
  std::vector<N> O = IKKBZ<N>(g);
  size_t sz_v = O.size();

  // empty DP table of size |V|*|V|
  std::vector<std::vector<JoinTree<N>>> T(O.size(),
                                          std::vector<JoinTree<N>>(O.size()));

  for (size_t i = 0; i < sz_v; i++)
    for (size_t j = 0; j < sz_v; j++) T[i][j] = JoinTree(O[i]);

  // find the optimal plan for the linearization
  for (size_t s = 2; s <= sz_v; s++) {
    for (size_t i = 0; i <= sz_v - s; i++) {
      for (size_t j = 1; j <= s - 1; j++) {
        auto L = T[i][i + j - 1];
        // FIXME: is this a typo??
        auto R = T[i + j][i + s - 1];  // auto R = T[i + s][i + s - 1];
        if (canJoin(g, L, R)) {
          JoinTree<N> P = JoinTree(L, R);

          // TODO: how to argmin when the cost of single relation is zero?
          if (Cost::Cout(P, g) > Cost::Cout(T[i][i + s - 1], g))
            T[i][i + s - 1] = P;
        }
      }
    }
  }

  for (size_t i = 0; i < sz_v; i++) {
    for (size_t j = 0; j < sz_v; j++) std::cout << T[i][j].expr() << " ";
    std::cout << std::endl;
  }

  return T[0][sz_v - 1];
}

template <typename N>
requires RelationAble<N>
bool canJoin(const QueryGraph<N>& g, const JoinTree<N>& t1,
             const JoinTree<N>& t2) {
  // FIXME: this doesn't sound right...
  // TODO: all wrong!
  auto r1 = t1.relations_iter();
  auto r2 = t2.relations_iter();

  auto s1 = t1.relations_iter_str();
  auto s2 = t2.relations_iter_str();

  // empty join tree can be joined with anything
  // useful when initing the DP table
  // TODO: useless
  if (s1.empty() || s2.empty()) return true;

  // TODO: again not like this...
  for (auto const& x : t1.relations_iter())
    for (auto const& y : t2.relations_iter())
      if (g.has_rjoin(x, y)) return true;

  return false;
}

template <typename N>
requires RelationAble<N>
JoinTree<N> createJoinTree(const JoinTree<N>& t1, const JoinTree<N>& t2) {
  // ref: 149/637
  // TODO: consider cartisan, hash joins, etc...
  // applicable join implementations (from previously defined jointype enum?)
  // TODO: assert 2 tree don't share relations?

  return JoinTree(t1, t2);

  std::vector<JoinTree<N>> B;

  // checking whether a given tree is leftdeep is a O(n)
  bool t1_isLeftDeep = t1.isLeftDeep();
  bool t1_isRightDeep = t1.isRightDeep();
  bool t2_isLeftDeep = t1.isLeftDeep();
  bool t2_isRightDeep = t1.isRightDeep();

  t1.relations_iter();
  if (t1_isLeftDeep && t2_isLeftDeep) return JoinTree(t1, t2);
  if (t1_isRightDeep && t2_isRightDeep) return JoinTree(t2, t1);

  return B[0];  // TODO: argmin cost
}

// explicit template instantiation
template JoinTree<RelationBasic> linearizedDP<RelationBasic>(
    const QueryGraph<RelationBasic>&);
template bool canJoin(const QueryGraph<RelationBasic>&,
                      const JoinTree<RelationBasic>&,
                      const JoinTree<RelationBasic>&);

}  // namespace JoinOrdering
