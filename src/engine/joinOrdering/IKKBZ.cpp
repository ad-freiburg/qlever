// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "IKKBZ.h"

namespace JoinOrdering {

template <typename N>
auto IKKBZ(QueryGraph<N> g, const N& n) -> QueryGraph<N> {
  // TODO: argmin over all rooted relations
  auto new_g = toPrecedenceGraph(g, n);
  IKKBZ_Sub(new_g);
  return new_g;
}

template <typename N>
[[nodiscard]] auto toPrecedenceGraph(QueryGraph<N>& g, const N& root)
    -> QueryGraph<N> {
  // bfs-ing over g and assign direction to visited relation
  auto pg = QueryGraph<N>();
  auto v = std::set<N>();
  auto q = std::queue<N>();
  pg.root = root;
  v.insert(pg.root);
  q.push(pg.root);

  while (!q.empty()) {
    auto a = q.front();
    q.pop();
    for (auto const& [b, _] : g.r[a]) {  // std::views::keys(g.r[a]);
      if (v.contains(b)) continue;
      if (!pg.has_relation(a)) pg.add_relation(a);
      if (!pg.has_relation(b)) pg.add_relation(b);

      // we assign selectivity here
      pg.add_rjoin(a, b, g.selectivity[b], Direction::PARENT);
      q.push(b);
      v.insert(b);
    }
  }

  return pg;
}

template <typename N>
void IKKBZ_Sub(QueryGraph<N>& g) {
  while (!g.is_chain(g.root)) {
    auto subtree = g.get_chained_subtree(g.root);

    while (!IKKBZ_Normalized(g, subtree))
      ;
    g.merge(subtree);
  }
  IKKBZ_denormalize(g);
}

template <typename N>
bool IKKBZ_Normalized(QueryGraph<N>& g, const N& subtree_root) {
  for (auto const& d : g.get_descendents(subtree_root)) {
    auto pv = g.get_parent(d);
    if (pv.empty()) continue;
    auto p = pv.front();

    if (p == g.root) continue;  // TODO: check skip norm root
    if (d == subtree_root || p == subtree_root) continue;

    auto cxs = g.get_children(p);
    for (auto const& c : cxs)
      // "precedence graph demands A -> B but rank(A) > rank(B),
      // we speak of contradictory sequences."
      // 118/637
      if (g.rank(p) > g.rank(c)) {
        // a new node representing compound relation
        g.combine(p, c);
        return false;
      }
  }
  return true;  // ready to merge
}

template <typename N>
void IKKBZ_denormalize(QueryGraph<N>& g) {
  while (!std::ranges::all_of(g.get_descendents(g.root), [g](const N& n) {
    if (g.hist.contains(n)) return g.hist.at(n).empty();
    return true;
  }))
    for (auto const& x : g.get_descendents(g.root)) g.uncombine(x);
}
}  // namespace JoinOrdering
