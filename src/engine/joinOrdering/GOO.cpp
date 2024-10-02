// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "GOO.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> auto GOO(QueryGraph<N>& g) -> N {
  typedef std::pair<N, N> rr;
  auto costfn = [&](const rr& r) {
    auto& [a, b] = r;
    return g.edges_[a][b].weight * g.cardinality[a] * g.cardinality[b];
  };
  auto comp = [&](const rr& l, const rr& r) { return costfn(l) < costfn(r); };

  // TODO: assert decreasing size
  while (true) {
    std::vector<rr> zxs = g.iter_pairs();
    auto& [a, b] = *std::ranges::min_element(zxs, comp);
    auto ab = GOO_combine(g, a, b);
    if (zxs.size() == 1) return ab;
    //    for (auto const& [x, y] : zxs)
    //      std::cout << x.getLabel() << " " << y.getLabel() << " " <<
    //      std::fixed
    //                << costfn(rr(x, y)) << "\n";
  }
}

template <typename N>
requires RelationAble<N>
[[maybe_unused]] N GOO_combine(QueryGraph<N>& g, const N& a, const N& b) {
  auto w = a.getCardinality() * b.getCardinality() * g.edges_[a][b].weight;
  AD_CONTRACT_CHECK(w >= 0);

  // add the newly computed cardinality to the
  // cardinality map of the query graph.
  auto n = N("(" + a.getLabel() + "⋈" + b.getLabel() + ")", w);
  g.add_relation(n);

  // we keep track of the combined relation in the `hist` map
  g.hist[n] = {a, b};

  // TODO: STL chain iterators
  for (auto const& [x, e] : boost::join(g.edges_[a], g.edges_[b])) {
    if (e.hidden || x == a || x == b) continue;
    g.add_rjoin(n, x, e.weight, Direction::UNDIRECTED);

    if (!g.is_common_neighbour(a, b, x)) continue;
    // when the 2 relations to be combined have common neighbours
    // multiply edge weights of newly combined relation
    g.edges_[x][n].weight = g.edges_[a][x].weight * g.edges_[b][x].weight;
    g.edges_[n][x].weight = g.edges_[a][x].weight * g.edges_[b][x].weight;
  }

  // make these relations unreachable
  g.rm_relation(a);
  g.rm_relation(b);

  return n;
}

}  // namespace JoinOrdering
