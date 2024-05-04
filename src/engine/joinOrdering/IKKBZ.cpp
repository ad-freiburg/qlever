// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "IKKBZ.h"

#include "CostASI.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> auto IKKBZ(QueryGraph<N> g) -> std::vector<N> {
  // execute the IKKBZ routine for EVERY relation on the graph
  // then take return the permutation with the minimum cost.
  auto rxs(g.relations_);
  AD_CONTRACT_CHECK(!rxs.empty());

  typedef std::pair<std::vector<N>, float> vf;
  auto [ldtree_opt, cost] = std::transform_reduce(
      // TODO: macos doesn't like it
      //      std::execution::par_unseq,  // (3) in parallel if hw allows it
      rxs.begin(), rxs.end(),  // (1) for every relation in query
      vf({}, std::numeric_limits<float>::max()),
      [&](const vf& l, const vf& r) {  // (4) return the tree with min cost
        return std::ranges::min(
            l, r, [](const vf& a, const vf& b) { return a.second < b.second; });
      },
      [&](const N& n) {  // (2) run IKKBZ routine
        auto ldtree = IKKBZ(g, n);
        auto seq = ldtree.iter();
        auto seqv = std::span<N>(seq);
        return vf(seq, ASI::C(ldtree, seqv));
      });

  return ldtree_opt;
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
  for (auto const& d : g.iter(subtree_root)) {
    auto pv = g.get_parent(d);
    if (pv.empty()) continue;
    auto p = pv.front();
    if (d == subtree_root || p == subtree_root) continue;

    for (auto const& c : g.get_children(p))
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
  auto dv = g.iter(n);  // iter includes "n" back.

  // exclude n from sorting. subchain root not considered during sorting.
  // n is always at the beginning of dv
  std::ranges::partial_sort(dv.begin() + 1, dv.end(), dv.end(),
                            [&](const N& a, const N& b) {
                              return ASI::rank(g, a) < ASI::rank(g, b);
                            });

  // given a sequence post sort dv (a, b, c, d, ...)
  // include subchain root at the beginning (n, a, b, c, d, ...)
  // we remove all connections they have (except n) and conform to the order
  // we get post the sorting process (n -> a -> b -> c -> d)

  for (size_t i = 1; i < dv.size(); i++) {
    g.unlink(dv[i]);
    g.add_rjoin(dv[i - 1], dv[i], g.selectivity[dv[i]], Direction::PARENT);
  }
}

template <typename N>
requires RelationAble<N> void IKKBZ_denormalize(QueryGraph<N>& g) {
  auto is_compound = [&](const N& n) { return g.is_compound_relation(n); };
  auto uncombine = [&](const N& n) { g.uncombine(n); };

  auto rxs = g.iter();
  // R1 -> R4R6R7 -> R5 -> R3 -> R2
  auto fv = std::views::filter(rxs, is_compound);  // R4R6R7

  // R1 -> R4 -> R6 -> R7 -> R5 -> R3 -> R2
  std::for_each(fv.begin(), fv.end(), uncombine);
}
}  // namespace JoinOrdering
