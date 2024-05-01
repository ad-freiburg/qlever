// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "IKKBZ.h"

#include "CostASI.cpp"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> auto IKKBZ(QueryGraph<N> g) -> std::vector<N> {
  std::vector<N> rxs(g.relations_);
  typedef std::pair<std::vector<N>, float> vf;

  // TODO: execution::par_unseq
  auto qcxs = std::views::transform(rxs, [&](const N& n) {
    //    std::cout << "ROOTED AT " << n.getLabel() << "\n";
    auto pg_ikbbz = IKKBZ(g, n);
    auto q = pg_ikbbz.iter();
    auto qn = std::pair(q, ASI::C(pg_ikbbz, q));
    //        std::cout << n.getLabel() << " " << qn.second << "\n";
    return qn;
  });

  std::vector<N> erg;
  float min_cost = std::numeric_limits<float>::max();
  // TODO: std::transform_reduce, std::min_element or whatever
  for (const vf& x : qcxs)
    if (min_cost > x.second) {
      erg = x.first;
      min_cost = x.second;
    }

  return erg;
}

template <typename N>
requires RelationAble<N>
auto IKKBZ(QueryGraph<N> g, const N& n) -> QueryGraph<N> {
  auto new_g = toPrecedenceGraph(g, n);
  IKKBZ_Sub(new_g);
  return new_g;
}

template <typename N>
requires RelationAble<N>
[[nodiscard]] auto toPrecedenceGraph(QueryGraph<N> g, const N& root)
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
    for (auto const& [b, _] : g.edges_[a]) {  // std::views::keys(g.edges_[a]);
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
requires RelationAble<N> void IKKBZ_Sub(QueryGraph<N>& g) {
  while (!g.is_chain(g.root)) {
    auto subtree = g.get_chained_subtree(g.root);

    while (!IKKBZ_Normalized(g, subtree))
      ;
    IKKBZ_merge(g, subtree);
  }
  IKKBZ_denormalize(g);
}

template <typename N>
requires RelationAble<N>
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
      if (ASI::rank(g, p) > ASI::rank(g, c)) {
        // a new node representing compound relation
        g.combine(p, c);
        return false;
      }
  }
  return true;  // ready to merge
}

template <typename N>
requires RelationAble<N> void IKKBZ_merge(QueryGraph<N>& g, const N& n) {
  // we get here after we are already sure that descendents are in a chain
  auto dxs = g.get_descendents(n);

  // get_descendents includes n, exclude from sorting
  dxs.erase(n);
  std::vector<N> dv(dxs.begin(), dxs.end());
  if (dv.empty()) return;

  std::ranges::sort(dv, [&](const N& a, const N& b) {
    return ASI::rank(g, a) < ASI::rank(g, b);
  });

  // given a sequence post sort dv (a, b, c, d, ...)
  // we remove all connections they have and conform to the order
  // we got post the sorting process (a -> b -> c -> d)
  g.unlink(dv[0]);
  g.add_rjoin(n, dv[0], g.selectivity[dv[0]], Direction::PARENT);

  for (size_t i = 1; i < dv.size(); i++) {
    g.unlink(dv[i]);
    g.add_rjoin(dv[i - 1], dv[i], g.selectivity[dv[i]], Direction::PARENT);
  }
}

template <typename N>
requires RelationAble<N> void IKKBZ_denormalize(QueryGraph<N>& g) {
  while (!std::ranges::all_of(g.get_descendents(g.root), [g](const N& n) {
    if (g.hist.contains(n)) return g.hist.at(n).empty();
    return true;
  }))
    for (auto const& x : g.get_descendents(g.root)) g.uncombine(x);
}
}  // namespace JoinOrdering
