// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "IKKBZ.h"

#include "CostIKKBZ.h"
#include "RelationBasic.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> std::vector<N> IKKBZ(QueryGraph<N> g) {
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
        auto Ch = CostIKKBZ<N>();
        auto ldtree = IKKBZ(g, Ch, n);
        auto seq = ldtree.iter();
        auto seqv = std::span<N>(seq);
        return vf(seq, Ch.C(ldtree, seqv));
      });

  return ldtree_opt;
}

template <typename N>
requires RelationAble<N> QueryGraph<N> IKKBZ(QueryGraph<N> g, const N& n) {
  auto new_g = toPrecedenceGraph(g, n);
  auto Ch = CostIKKBZ<N>();
  IKKBZ_Sub(new_g, Ch);
  return new_g;
}

template <typename N>
requires RelationAble<N>
QueryGraph<N> IKKBZ(QueryGraph<N> g, ICostASI<N>& Ch, const N& n) {
  auto new_g = toPrecedenceGraph(g, n);
  IKKBZ_Sub(new_g, Ch);
  return new_g;
}

template <typename N>
requires RelationAble<N>
[[nodiscard]] QueryGraph<N> toPrecedenceGraph(QueryGraph<N>& g, const N& root) {
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
requires RelationAble<N> void IKKBZ_Sub(QueryGraph<N>& g, ICostASI<N>& Ch) {
  while (!g.is_chain(g.root)) {
    auto subtree_root = g.get_chained_subtree(g.root);
    auto normalized_subtree = IKKBZ_Normalized(g, Ch, subtree_root);
    IKKBZ_merge(g, Ch, normalized_subtree);
  }
  IKKBZ_Denormalize(g);
}

template <typename N>
requires RelationAble<N>
std::vector<N> IKKBZ_Normalized(QueryGraph<N>& g, ICostASI<N>& Ch,
                                const N& subtree_root) {
  for (bool normalized = true;; normalized = true) {
    auto subtree = g.iter(subtree_root);

    for (auto const& d : subtree) {
      // iter includes subtree_root back
      // skip subtree root
      if (d == subtree_root) continue;
      auto pv = g.get_parent(d);

      // absence of a parent means g.root
      // skip query graph root
      if (pv.empty()) continue;
      auto p = pv.front();

      // subtree_root is excluded from the ranking comparison
      if (p == subtree_root) continue;

      for (auto const& c : g.get_children(p))
        // "precedence graph demands A -> B but rank(A) > rank(B),
        // we speak of contradictory sequences."
        // 118/637
        if (Ch.rank(g, p) > Ch.rank(g, c)) {
          // a new node representing compound relation
          IKKBZ_combine(g, p, c);
          // mark as dirty
          // subtree_root might (or might not) need more normalization
          normalized = false;
        }
    }
    if (normalized) return subtree;
  }
}

template <typename N>
requires RelationAble<N>
void IKKBZ_merge(QueryGraph<N>& g, ICostASI<N>& Ch, std::vector<N>& dv) {
  // we get here after we are already sure that descendents
  // are going to be in a SINGLE chain

  // subchain root not considered during sorting
  // subchain root is always at the beginning regardless of it's rank
  // subchain is always at the beginning of dv
  std::ranges::partial_sort(
      dv.begin() + 1, dv.end(), dv.end(),
      [&](const N& a, const N& b) { return Ch.rank(g, a) < Ch.rank(g, b); });

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
requires RelationAble<N>
[[maybe_unused]] N IKKBZ_combine(QueryGraph<N>& g, const N& a, const N& b) {
  // 104/637
  // "if the ordering violates the query constraints, it constructs compounds"

  AD_CONTRACT_CHECK(g.has_rjoin(a, b));

  // 118/637
  // "its cardinality is computed by multiplying the cardinalities of
  // all relations in A and B"
  //  auto w = cardinality[a] * cardinality[b];
  auto w = a.getCardinality() * b.getCardinality();

  // "its selectivity is the product of all selectivities (s_i) of relations
  // R_i contained in A and B"
  auto s = g.selectivity[a] * g.selectivity[b];

  // add the newly computed cardinality to the
  // cardinality map of the query graph.
  auto n = N(a.getLabel() + "," + b.getLabel(), w);
  g.add_relation(n);

  // to be able to apply the inverse operation (IKKBZ_uncombine)
  // we keep track of the combined relation in the `hist` map

  g.hist[n] = {a, b};

  // TODO: use common neighbor?
  std::set<N> parents;
  for (auto const& x : g.get_parent(a)) parents.insert(x);
  for (auto const& x : g.get_parent(b)) parents.insert(x);

  // IN CASE merging bc
  // a -> b -> c
  // we don't want b to be the parent of bc
  parents.erase(a);
  parents.erase(b);

  //  for (auto const& x : parents) add_rjoin(x, n, s, Direction::PARENT);
  AD_CONTRACT_CHECK(parents.size() == 1);
  g.add_rjoin(*parents.begin(), n, s, Direction::PARENT);

  // filters out duplicate relation if the 2 relation have common descendants.
  // yes. it should never happen in an acyclic graph.
  // rationale behind using a std::set here
  std::set<N> children{};

  // collect all children of relation a
  // collect all children of relation b
  // connect relation n to each child of a and b

  auto ca = g.get_children(a);
  auto cb = g.get_children(b);
  children.insert(ca.begin(), ca.end());
  children.insert(cb.begin(), cb.end());

  // equiv. to add_rjoin(n, c, s, Direction::PARENT);
  for (auto const& c : children) g.add_rjoin(c, n, s, Direction::CHILD);

  // make these relations unreachable
  g.rm_relation(a);
  g.rm_relation(b);
  return n;
}

template <typename N>
requires RelationAble<N> void IKKBZ_uncombine(QueryGraph<N>& g, const N& n) {
  // ref: 121/637
  // don't attempt to uncombine regular relation
  if (!g.is_compound_relation(n)) return;

  auto pn = g.get_parent(n);
  auto cn = g.get_children(n);

  std::vector<N> rxs{};

  // breaks down a given compound relation (n)
  // to its basic components (r1, r2, ....)
  g.unpack(n, rxs);

  // put the parent of relation first (1)
  std::vector<N> v{pn.begin(), pn.end()};
  // assert single parent to the compound relation
  AD_CONTRACT_CHECK(v.size() == 1);

  // then the basic relation (r1, r2, ...) (2)
  v.insert(v.end(), rxs.begin(), rxs.end());
  // then the children of (n) (3)
  v.insert(v.end(), cn.begin(), cn.end());

  // also removes all incoming and outgoing connections
  g.rm_relation(n);

  // given {p, r1, r2, ..., c1, c2, ...}, connect them such that
  // p -> r1 -> r2 -> ... -> c1 -> c2 -> ...
  for (size_t i = 1; i < v.size(); i++)
    g.add_rjoin(v[i - 1], v[i], g.selectivity[v[i]], Direction::PARENT);
}

template <typename N>
requires RelationAble<N> void IKKBZ_Denormalize(QueryGraph<N>& g) {
  auto is_compound = [&](const N& n) { return g.is_compound_relation(n); };
  auto uncombine = [&](const N& n) { IKKBZ_uncombine(g, n); };

  auto rxs = g.iter();
  // R1 -> R4R6R7 -> R5 -> R3 -> R2
  auto fv = std::views::filter(rxs, is_compound);  // R4R6R7

  // R1 -> R4 -> R6 -> R7 -> R5 -> R3 -> R2
  std::for_each(fv.begin(), fv.end(), uncombine);
}

// explicit template instantiation

template std::vector<RelationBasic> IKKBZ(QueryGraph<RelationBasic>);
template QueryGraph<RelationBasic> IKKBZ(QueryGraph<RelationBasic>,
                                         const RelationBasic&);
template void IKKBZ_merge(QueryGraph<RelationBasic>&, ICostASI<RelationBasic>&,
                          std::vector<RelationBasic>&);

}  // namespace JoinOrdering
