// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "CostASI.h"

namespace JoinOrdering::ASI {

template <typename N>
requires RelationAble<N>
auto rank(const QueryGraph<N>& g, const N& n) -> float {
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
requires RelationAble<N> auto T(const QueryGraph<N>& g, const N& n) -> float {
  // return 0 if Ri is root 113/637
  if (g.root == n) return 1;
  return g.selectivity.at(n) * static_cast<float>(n.getCardinality());
}

template <typename N>
requires RelationAble<N> auto C(const QueryGraph<N>& g, const N& n) -> float {
  // return 0 if Ri is root 113/637
  if (g.root == n) return 0;

  // i.e: regular relation
  if (!g.is_compound_relation(n)) return T(g, n);

  auto seq = g.hist.at(n);
  return C(g, std::span<N>(seq));
}

template <typename N>
requires RelationAble<N>
auto C(const QueryGraph<N>& g, std::span<N> seq) -> float {
  if (seq.empty()) return 0.0f;
  auto s1 = seq.front();
  //  auto s2 = seq | std::views::drop(1);
  auto s2 = seq.subspan(1);
  return C(g, s1) + T(g, s1) * C(g, s2);  // TODO: might overflow
}

}  // namespace JoinOrdering::ASI
