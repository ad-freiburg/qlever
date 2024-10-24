// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "CostIKKBZ.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N>
auto CostIKKBZ<N>::C(const QueryGraph<N>& g, std::span<N> seq) -> float {
  if (seq.empty()) return 0.0f;
  auto s1 = seq.front();
  //  auto s2 = seq | std::views::drop(1);
  auto s2 = seq.subspan(1);
  return C(g, s1) + T(g, s1) * C(g, s2);  // TODO: might overflow
}

template <typename N>
requires RelationAble<N>
auto CostIKKBZ<N>::C(const QueryGraph<N>& g, const N& n) -> float {
  // return 0 if Ri is root 113/637
  if (g.root == n) return 0;

  // i.e: regular relation
  if (!g.is_compound_relation(n)) return T(g, n);

  auto const& [s1, s2] = g.hist.at(n).value();
  return C(g, s1) + T(g, s1) * C(g, s2);  // TODO: might overflow
}

template <typename N>
requires RelationAble<N>
auto CostIKKBZ<N>::T(const QueryGraph<N>& g, const N& n) -> float {
  // return 0 if Ri is root 113/637
  if (g.root == n) return 1;
  return g.selectivity.at(n) * static_cast<float>(n.getCardinality());
}

template <typename N>
requires RelationAble<N>
auto CostIKKBZ<N>::rank(const QueryGraph<N>& g, const N& n) -> float {
  // memorize cost and rank
  // avoid recomputing for long sequences
  if (rank_m.contains(n)) return rank_m[n];     // important
  auto c = C_m.contains(n) ? C_m[n] : C(g, n);  // important
  auto t = T_m.contains(n) ? T_m[n] : T(g, n);  // maybe not important

  if (c == 0) return 0;
  auto r = (t - 1) / c;
  AD_CONTRACT_CHECK(r >= 0 && r <= 1);

  rank_m[n] = r;
  C_m[n] = c;
  T_m[n] = t;
  return r;
}
}  // namespace JoinOrdering
