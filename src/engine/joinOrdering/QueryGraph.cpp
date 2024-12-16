// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include "QueryGraph.h"

#include "RelationBasic.h"

namespace JoinOrdering {

template <typename N>
requires RelationAble<N> void QueryGraph<N>::add_relation(const N& n) {
  // extract the cardinality and add to the cardinality map to make
  // the lookup process easy when using cost function
  cardinality[n] = n.getCardinality();
  if (!has_relation(n)) relations_.push_back(n);
}

template <typename N>
requires RelationAble<N> bool QueryGraph<N>::has_relation(const N& n) const {
  // TODO: it's faster to lookup the edges_ map...
  //  return edges_.contains(n);
  return std::find(relations_.begin(), relations_.end(), n) != relations_.end();
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
bool QueryGraph<N>::is_compound_relation(const N& n) const {
  return hist.contains(n) && hist.at(n).has_value();
}

template <typename N>
requires RelationAble<N>
bool QueryGraph<N>::is_common_neighbour(const N& a, const N& b,
                                        const N& n) const {
  return has_rjoin(a, n) && has_rjoin(b, n);
}

template <typename N>
requires RelationAble<N>
void QueryGraph<N>::add_rjoin(const N& a, const N& b, float join_selectivity,
                              Direction dir) {
  // add connection between a -> b
  edges_[a][b] = EdgeInfo(dir, join_selectivity);

  // add connection between b -> a
  edges_[b][a] = EdgeInfo(inv(dir), join_selectivity);

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
requires RelationAble<N>
bool QueryGraph<N>::has_rjoin(const N& a, const N& b) const {
  // does relation a exists
  // does relation b exists
  // is there a connection between a and b
  // is there a connection between b and a
  // is the connection between a and is NOT hidden
  return (edges_.contains(a) && edges_.contains(b) &&
          edges_.at(a).contains(b) && edges_.at(b).contains(a) &&
          !edges_.at(a).at(b).hidden);  // !edges_[a][b].hidden;
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
void QueryGraph<N>::unpack(const N& n, std::vector<N>& acc) {  // NOLINT

  // cannot be broken down into small parts anymore
  // i.e. regular relation
  if (!is_compound_relation(n)) {
    acc.push_back(n);
    return;
  }

  // otherwise it consists of 2 relations s1 and s2
  // they may or may not be compound too, so we call unpack again
  auto const& [s1, s2] = hist[n].value();
  unpack(s1, acc);
  unpack(s2, acc);
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
requires RelationAble<N>
bool QueryGraph<N>::is_chain(const N& n) const {  // NOLINT
  auto cv = get_children(n);
  auto len = std::ranges::distance(cv);

  if (len == 0) return true;  // leaf
  if (len > 1) return false;  // another subtree

  // len == 1
  return is_chain(cv.front());
}

template <typename N>
requires RelationAble<N> bool QueryGraph<N>::is_subtree(const N& n) const {
  // TODO: mem subtrees?
  return !is_chain(n) and std::ranges::all_of(get_children(n), [&](const N& x) {
    return is_chain(x);
  });
}
//
// template <typename N>
// requires RelationAble<N> auto QueryGraph<N>::get_parent(const N& n) const {
//  return std::views::filter(edges_.at(n),  // edges_[n],
//                            [](std::pair<const N&, const EdgeInfo&> t) {
//                              auto const& [x, e] = t;
//                              return e.direction == Direction::CHILD &&
//                                     !e.hidden;
//                            }) |
//         std::views::transform(
//             [](std::pair<const N&, const EdgeInfo&> t) { return t.first; });
//}

// template <typename N>
// requires RelationAble<N> auto QueryGraph<N>::get_children(const N& n) const {
//   return std::views::filter(edges_.at(n),  // edges_[n]
//                             [](std::pair<const N&, const EdgeInfo&> t) {
//                               // TODO: structural binding in args
//                               auto const& [x, e] = t;
//                               return e.direction == Direction::PARENT &&
//                                      !e.hidden;
//                             }) |
//          std::views::transform(
//              [](std::pair<const N&, const EdgeInfo&> t) { return t.first; });
// }

template <typename N>
requires RelationAble<N>
auto QueryGraph<N>::get_chained_subtree(const N& n) -> N {
  auto dxs = iter(n);
  // lookup the first subtree
  auto it =
      std::ranges::find_if(dxs, [&](const N& x) { return is_subtree(x); });

  // since this is called from IKKBZ_Normalize
  // we have already checked the existence of a subtree
  //  if (it != dxs.end())
  return *it;

  //  AD_CONTRACT_CHECK(false);
  //  throw std::runtime_error("how did we get here?");
}

template <typename N>
requires RelationAble<N> auto QueryGraph<N>::iter() -> std::vector<N> {
  // QueryGraph(Relation)?
  //  AD_CONTRACT_CHECK(&root != nullptr); // always true
  return iter(root);
}

template <typename N>
requires RelationAble<N>
auto QueryGraph<N>::iter(const N& n) -> std::vector<N> {
  // bfs-ing over all relations starting from n

  auto erg = std::vector<N>();
  auto q = std::queue<N>();
  auto v = std::set<N>();

  v.insert(n);
  q.push(n);
  erg.push_back(n);

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

// template <typename N>
// requires RelationAble<N>
// auto QueryGraph<N>::iter_pairs() -> std::vector<std::pair<N, N>> {
//   auto v = std::set<std::pair<N, N>>();
//
//   for (auto const& [a, be] : edges_)
//     for (auto const& [b, e] : be) {
//       auto p = std::pair(b, a);
//       // skip implicitly removed relation (with hidden edges)
//       // skip already visited pairs
//       // skip duplicates. (R1, R2) is the same as (R2, R1)
//       if (e.hidden || v.contains(p) || v.contains(std::pair(a, b))) continue;
//       v.insert(p);
//     }
//
//   return std::vector(v.begin(), v.end());
// }
//
// template <typename N>
// requires RelationAble<N>
// auto QueryGraph<N>::iter_pairs(const N& n) -> std::vector<std::pair<N, N>> {
//   auto v = std::set<std::pair<N, N>>();
//
//   for (auto const& [b, e] : edges_[n]) {
//     auto p = std::pair(b, n);
//     // skip implicitly removed relation (with hidden edges)
//     // skip already visited pairs
//     // skip duplicates. (R1, R2) is the same as (R2, R1)
//     if (e.hidden || v.contains(p) || v.contains(std::pair(n, b))) continue;
//     v.insert(p);
//   }
//
//   return std::vector(v.begin(), v.end());
// }

template <typename N>
requires RelationAble<N> constexpr Direction QueryGraph<N>::inv(Direction dir) {
  //  const ad_utility::HashMap<Direction, Direction> m{
  //      {Direction::UNDIRECTED, Direction::UNDIRECTED},
  //      {Direction::PARENT, Direction::CHILD},
  //      {Direction::CHILD, Direction::PARENT},
  //  };

  switch (dir) {
      //    case Direction::UNDIRECTED:
      //      return Direction::UNDIRECTED;
    case Direction::PARENT:
      return Direction::CHILD;
    case Direction::CHILD:
      return Direction::PARENT;
    default:
      // suppress compiler warning
      return Direction::UNDIRECTED;
  }
}

template class QueryGraph<RelationBasic>;

}  // namespace JoinOrdering
