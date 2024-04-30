// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "QueryGraph.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> auto QueryGraph<N>::add_relation(const N& n) -> N {
  // extract the cardinality and add to the cardinality map to make
  // the lookup process easy when using cost function
  //  cardinality[n] = n.getCardinality();
  return n;
}

template <typename N>
requires RelationAble<N> bool QueryGraph<N>::has_relation(const N& n) const {
  // TODO: doesn't work if the relation has no connection?
  return edges_.contains(n);
}

template <typename N>
requires RelationAble<N> void QueryGraph<N>::rm_relation(const N& n) {
  // Removing a relation by turning-off all it's neighbours
  // hiding the connections instead of erasing them, effectively deleting them.
  // the hidden property is used to filter out these relation in
  // JoinOrdering::get_parent and JoinOrdering::get_children.
  for (auto& [x, e] : edges_[n]) {
    edges_[x][n].hidden = true;
    edges_[n][x].hidden = true;
  }
}

template <typename N>
requires RelationAble<N>
void QueryGraph<N>::add_rjoin(const N& a, const N& b, float join_selectivity,
                              Direction dir) {
  // TODO: assert single parent here?

  // add connection between a -> b
  edges_[a][b] = EdgeInfo(dir);

  // add connection between b -> a
  edges_[b][a] = EdgeInfo(inv(dir));

  // TODO: avoid overwriting selectivity
  // selectivity is a relation property
  switch (dir) {
    case Direction::UNDIRECTED:
      if (!selectivity.contains(a)) selectivity[a] = join_selectivity;
      if (!selectivity.contains(b)) selectivity[b] = join_selectivity;
      break;
    case Direction::PARENT:
      if (!selectivity.contains(b)) selectivity[b] = join_selectivity;
      break;
    case Direction::CHILD:
      if (!selectivity.contains(a)) selectivity[a] = join_selectivity;
      break;
  }
}

template <typename N>
requires RelationAble<N> bool QueryGraph<N>::has_rjoin(const N& a, const N& b) {
  // does relation a exists
  // does relation b exists
  // is there a connection between a and b
  // is there a connection between b and a
  // is the connection between a and is NOT hidden
  return (edges_.contains(a) && edges_.contains(b) &&
          edges_.at(a).contains(b) && edges_.at(b).contains(a) &&
          !edges_[a][b].hidden);
}

template <typename N>
requires RelationAble<N> void QueryGraph<N>::rm_rjoin(const N& a, const N& b) {
  //    r[a].erase(b);
  //    r[b].erase(a);

  // hide the connection between a and b [dir]
  edges_[a][b].hidden = true;

  // hide the connection between b and a [inv(dir)]
  edges_[b][a].hidden = true;
}

template <typename N>
requires RelationAble<N>
N QueryGraph<N>::combine(const N& a,
                         const N& b) {  // -> Compound Relation (hist)

  // 104/637
  // if the ordering violates the query constraints, it constructs compounds
  // TODO: assert chain
  //    std::cout << "COMBINE " << a.label << "  " << b.label << "\n";

  // 118/637

  // "its cardinality is computed by multiplying the cardinalities of
  // all relations in A and B"
  //  auto w = cardinality[a] * cardinality[b];
  auto w = a.getCardinality() * b.getCardinality();

  // "its selectivity is the product of all selectivities (s_i) of relations
  // R_i contained in A and B"
  auto s = selectivity[a] * selectivity[b];

  // add the newly computed cardinality to the
  // cardinality map of the query graph.
  auto n = this->add_relation(N(a.getLabel() + "," + b.getLabel(), w));

  //    hist[n].push_back(a);
  //    hist[n].push_back(b);

  // to be able to apply the inverse operation (QueryGraph::uncombine)
  // we keep track of the combined relation in the `hist` map
  if (hist[a].empty()) hist[n].push_back(a);
  // it's already a compound relation, so we graph it's original relations
  else
    for (auto const& x : hist[a]) hist[n].push_back(x);

  // do the same of the relation b
  if (hist[b].empty())
    hist[n].push_back(b);
  else
    for (auto const& x : hist[b]) hist[n].push_back(x);

  std::set<N> parents;
  for (auto const& x : get_parent(a)) parents.insert(x);
  for (auto const& x : get_parent(b)) parents.insert(x);

  // IN CASE merging bc
  // a -> b -> c
  // we don't want b to be the parent of bc
  parents.erase(a);
  parents.erase(b);

  //  for (auto const& x : parents) add_rjoin(x, n, s, Direction::PARENT);
  AD_CONTRACT_CHECK(parents.size() == 1);
  add_rjoin(*parents.begin(), n, s, Direction::PARENT);

  // filters out duplicate relation if the 2 relation have common descendants.
  // yes. it should never happen.
  // rationale behind using a std::set here
  std::set<N> children{};

  // collect all children of relation a
  // collect all children of relation b
  // connect relation n to each child of a and b

  auto ca = get_children(a);
  auto cb = get_children(b);
  children.insert(ca.begin(), ca.end());
  children.insert(cb.begin(), cb.end());
  children.erase(a);  // redundant
  children.erase(b);  // redundant

  // equiv. to add_rjoin(n, c, s, Direction::PARENT);
  for (auto const& c : children) add_rjoin(c, n, s, Direction::CHILD);

  // make these relations unreachable
  rm_relation(a);
  rm_relation(b);

  return n;
}
template <typename N>
requires RelationAble<N> void QueryGraph<N>::uncombine(const N& n) {
  // ref: 121/637
  // don't attempt to uncombine what has never been combined before
  if (hist[n].empty()) return;

  auto pn = get_parent(n);
  auto cn = get_children(n);
  auto rxs = hist[n];

  std::vector<N> v{pn.begin(), pn.end()};
  // assert single parent to the compound relation
  AD_CONTRACT_CHECK(v.size() == 1);

  v.insert(v.end(), rxs.begin(), rxs.end());
  v.insert(v.end(), cn.begin(), cn.end());

  // also removes all incoming and outgoing connections
  rm_relation(n);

  // given {a, b, c, ...}, connect them such that
  // a -> b -> c -> ...
  for (size_t i = 1; i < v.size(); i++)
    add_rjoin(v[i - 1], v[i], selectivity[v[i]], Direction::PARENT);
}

template <typename N>
requires RelationAble<N> void QueryGraph<N>::unlink(const N& n) {
  //  auto cv = get_children(n);
  //  auto pv = get_parent(n);
  //  std::set<N> children(cv.begin(), cv.end());
  //  std::set<N> parent(pv.begin(), pv.end());
  //
  //  // cut all connections from n to it's children
  //  for (auto const& c : children) rm_rjoin(c, n);
  //  // cut all connections from n to it's parent(s)?
  //  for (auto const& p : parent) rm_rjoin(p, n);

  // TODO: redundant, remove from IKKBZ_merge
  rm_relation(n);
}

template <typename N>
requires RelationAble<N> bool QueryGraph<N>::is_chain(const N& n) {  // NOLINT
  auto cv = get_children(n);
  auto len = std::ranges::distance(cv);

  if (len == 0) return true;  // leaf
  if (len > 1) return false;  // another subtree

  // len == 1
  return is_chain(cv.front());
}

template <typename N>
requires RelationAble<N> bool QueryGraph<N>::is_subtree(const N& n) {
  return !is_chain(n) and std::ranges::all_of(get_children(n), [&](const N& x) {
    return is_chain(x);
  });
}

template <typename N>
requires RelationAble<N> auto QueryGraph<N>::get_parent(const N& n) {
  return std::views::filter(edges_[n],
                            [](std::pair<const N&, const EdgeInfo&> t) {
                              auto const& [x, e] = t;
                              return e.direction == Direction::CHILD &&
                                     !e.hidden;
                            }) |
         std::views::transform(
             [](std::pair<const N&, const EdgeInfo&> t) { return t.first; });
}

template <typename N>
requires RelationAble<N> auto QueryGraph<N>::get_children(const N& n) {
  return std::views::filter(edges_[n],
                            [](std::pair<const N&, const EdgeInfo&> t) {
                              // TODO: structural binding in args
                              auto const& [x, e] = t;
                              return e.direction == Direction::PARENT &&
                                     !e.hidden;
                            }) |
         std::views::transform(
             [](std::pair<const N&, const EdgeInfo&> t) { return t.first; });
}

template <typename N>
requires RelationAble<N>
void QueryGraph<N>::get_descendents(const N& n, std::set<N>& acc) {
  if (acc.contains(n)) return;
  for (auto const& x : get_children(n)) {
    get_descendents(x, acc);
    acc.insert(x);
  }
}

template <typename N>
requires RelationAble<N>
auto QueryGraph<N>::get_descendents(const N& n) -> std::set<N> {
  // TODO: join views?
  std::set<N> acc{};
  get_descendents(n, acc);
  acc.insert(n);  // including frequently used self
  return acc;
}

template <typename N>
requires RelationAble<N>
auto QueryGraph<N>::get_chained_subtree(const N& n) -> N {
  //    for (auto const& x : get_descendents(n)) {
  //      if (is_subtree(x)) return x;
  //    }

  auto dxs = get_descendents(n);

  auto it =
      std::ranges::find_if(dxs, [&](const N& x) { return is_subtree(x); });

  if (it != dxs.end()) return *it;
  throw std::runtime_error("how did we get here?");
}

template <typename N>
requires RelationAble<N> auto QueryGraph<N>::iter() -> std::vector<N> {
  auto erg = std::vector<N>();
  auto q = std::queue<N>();
  auto v = std::set<N>();

  // TODO: switch to get_descendents(root); with unordered_set

  // TODO: ensure query graph has a root assigned with a constructor
  // QueryGraph(Relation)?
  AD_CONTRACT_CHECK(&root != NULL);
  auto n = root;
  v.insert(root);
  q.push(root);
  erg.push_back(root);

  while (!q.empty()) {
    auto f = q.front();
    q.pop();

    for (auto const& x : get_children(f)) {
      if (v.contains(x)) continue;
      q.push(x);
      v.insert(x);
      erg.push_back(x);
    }
  }

  return erg;
}

template <typename N>
requires RelationAble<N> void QueryGraph<N>::iter(const N& n) {
  std::set<N> visited{};
  iter(n, visited);
}

template <typename N>
requires RelationAble<N>
void QueryGraph<N>::iter(const N& n, std::set<N>& visited) {
  if (visited.contains(n)) return;

  for (auto const& [x, e] : edges_[n]) {
    if (e.hidden) continue;
    std::cout << n.getLabel() << " " << x.getLabel() << " "
              << static_cast<int>(e.direction) << " "
              << static_cast<int>(e.hidden) << "\n";
    visited.insert(n);

    iter(x, visited);
  }
}

template <typename N>
requires RelationAble<N> constexpr Direction QueryGraph<N>::inv(Direction dir) {
  //  const ad_utility::HashMap<Direction, Direction> m{
  //      {Direction::UNDIRECTED, Direction::UNDIRECTED},
  //      {Direction::PARENT, Direction::CHILD},
  //      {Direction::CHILD, Direction::PARENT},
  //  };

  switch (dir) {
    case Direction::UNDIRECTED:
      return Direction::UNDIRECTED;
    case Direction::PARENT:
      return Direction::CHILD;
    case Direction::CHILD:
      return Direction::PARENT;
  }

  // warning: control reaches end of non-void function
  return Direction::UNDIRECTED;
}

}  // namespace JoinOrdering
