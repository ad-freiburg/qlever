// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "CostASI.h"

namespace JoinOrdering::ASI {

template <typename N>
requires RelationAble<N> auto rank(QueryGraph<N>& g, const N& n) -> float {
  auto c = C(g, n);
  auto t = T(g, n);

  // TODO: what's the rank of root?
  if (c == 0) return 0;
  auto r = (t - 1) / c;
  // assert rank [0, 1]
  AD_CONTRACT_CHECK(r >= 0 && r <= 1);
  return r;
}

template <typename N>
requires RelationAble<N> auto T(QueryGraph<N>& g, const N& n) -> float {
  return g.selectivity.at(n) * static_cast<float>(n.getCardinality());
}

template <typename N>
requires RelationAble<N> auto C(QueryGraph<N>& g, const N& n) -> float {
  auto hxs = g.hist[n];
  // return 0 if Ri is root 113/637
  if (g.root == n) return 0;

  // i.e: regular relation
  if (hxs.empty()) return T(g, n);

  // otherwise compound relation
  return C(g, hxs);
}

template <typename N>
requires RelationAble<N> auto C(QueryGraph<N>& g, const std::vector<N>& seq)
    -> float {  // TODO: std::span
  if (seq.empty()) return 0.0f;
  auto s1 = seq.front();
  //  template instantiation depth exceeds maximum of 900
  //  auto s2 = seq | std::views::drop(1);
  auto s2 = std::vector(seq.begin() + 1, seq.end());
  return C(g, s1) + T(g, s1) * C(g, s2);
}
}  // namespace JoinOrdering::ASI
