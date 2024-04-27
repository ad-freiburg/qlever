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
  cardinality[n] = n.getCardinality();
  return n;
}

template <typename N>
requires RelationAble<N> bool QueryGraph<N>::has_relation(const N& n) const {
  // TODO: doesn't work if the relation has no connection?
  return r.contains(n);
}
template <typename N>
requires RelationAble<N> void QueryGraph<N>::rm_relation(const N& n) {
  // removing a relation by turning-off all it's neighbours
  // hiding the connections instead of erasing them, effectively deleting them.
  for (auto& [x, e] : r[n]) {
    r[x][n].hidden = true;
    r[n][x].hidden = true;
  }
}
template <typename N>
requires RelationAble<N>
void QueryGraph<N>::add_rjoin(const N& a, const N& b, float s, Direction dir) {
  // TODO: assert single parent here?

  // add connection between a -> b
  r[a][b] = RJoin(dir);

  // add connection between b -> a
  r[b][a] = RJoin(inv(dir));

  // TODO: avoid overwriting selectivity
  // selectivity is a relation property
  switch (dir) {
    case Direction::UNDIRECTED:
      if (!selectivity.contains(a)) selectivity[a] = s;
      if (!selectivity.contains(b)) selectivity[b] = s;
      break;
    case Direction::PARENT:
      if (!selectivity.contains(b)) selectivity[b] = s;
      break;
    case Direction::CHILD:
      if (!selectivity.contains(a)) selectivity[a] = s;
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
  return (r.contains(a) && r.contains(b) && r.at(a).contains(b) &&
          r.at(b).contains(a) && !r[a][b].hidden);
}

template <typename N>
requires RelationAble<N> void QueryGraph<N>::rm_rjoin(const N& a, const N& b) {
  //    r[a].erase(b);
  //    r[b].erase(a);

  // hide the connection between a and b [dir]
  r[a][b].hidden = true;

  // hide the connection between b and a [inv(dir)]
  r[b][a].hidden = true;
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
  auto w = cardinality[a] * cardinality[b];

  // "its selectivity is the product of all selectivities (s_i) of relations
  // R_i contained in A and B"
  auto s = selectivity[a] * selectivity[b];

  // add the newly computed cardinality to the
  // cardinality map of the query graph.
  auto n = this->add_relation(N(a.getLabel() + "," + b.getLabel(), w));

  selectivity[n] = s;  // redundant
  cardinality[n] = w;  // redundant

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
  parents.erase(n);
  // IN CASE merging bc
  // a -> b -> c
  // we don't want b to be the parent of bc
  parents.erase(a);
  parents.erase(b);

  // TODO: assert a single parent
  for (auto const& x : parents) add_rjoin(x, n, s, Direction::PARENT);

  // filters out duplicate relation if the 2 relation have common descendants.
  std::set<N> children{};
  auto ca = get_children(a);
  auto cb = get_children(b);
  children.insert(ca.begin(), ca.end());
  children.insert(cb.begin(), cb.end());
  children.erase(a);
  children.erase(b);

  for (auto const& c : children) add_rjoin(c, n, s, Direction::CHILD);

  rm_relation(a);
  rm_relation(b);

  return n;
}
template <typename N>
requires RelationAble<N> void QueryGraph<N>::uncombine(const N& n) {
  // ref: 121/637
  // assert hist?

  // don't attempt to uncombine what has never been combined before
  if (hist[n].empty()) return;

  //    std::cout << "UNCOMBINE " << n.label << "\n";

  auto pn = get_parent(n);
  auto cn = get_children(n);

  // FIXME: there is an order when uncombining hist[n]
  // sort by rank?
  // is it the same as @ see merge?
  auto rxs = hist[n];

  std::vector<N> v{pn.begin(), pn.end()};
  v.insert(v.end(), rxs.begin(), rxs.end());
  v.insert(v.end(), cn.begin(), cn.end());

  for (auto const& x : v) rm_rjoin(x, n);

  // TODO: ??
  if (!v.empty())
    for (auto const& x : pn) rm_rjoin(x, v[1]);  // rm_rjoin(pn, v[1]);

  for (size_t i = 1; i < v.size(); i++) {
    add_rjoin(v[i - 1], v[i], selectivity[v[i]], Direction::PARENT);
    rm_rjoin(v[i], n);
  }
}
template <typename N>
requires RelationAble<N> void QueryGraph<N>::merge(const N& n) {
  // we get here after we are already sure that descendents are in a chain
  auto dxs = get_descendents(n);

  // get_descendents includes n, exclude from sorting
  dxs.erase(n);
  std::vector<N> dv(dxs.begin(), dxs.end());
  if (dv.empty()) return;

  std::ranges::sort(dv,
                    [&](const N& a, const N& b) { return rank(a) < rank(b); });

  // given a sequence post sort dv (a, b, c, d, ...)
  // we remove all connections they have and conform to the order
  // we got post the sorting process (a -> b -> c -> d)
  unlink(dv[0]);
  add_rjoin(n, dv[0], selectivity[dv[0]], Direction::PARENT);

  for (size_t i = 1; i < dv.size(); i++) {
    unlink(dv[i]);
    add_rjoin(dv[i - 1], dv[i], selectivity[dv[i]], Direction::PARENT);
  }
}
template <typename N>
requires RelationAble<N> void QueryGraph<N>::unlink(const N& n) {
  auto cv = get_children(n);
  auto pv = get_parent(n);
  std::set<N> children(cv.begin(), cv.end());
  std::set<N> parent(pv.begin(), pv.end());

  // cut all connections from n to it's children
  for (auto const& c : children) rm_rjoin(c, n);
  // cut all connections from n to it's parent(s)?
  for (auto const& p : parent) rm_rjoin(p, n);
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
  return std::views::filter(r[n],
                            [](std::pair<const N&, const RJoin&> t) {
                              auto const& [x, e] = t;
                              return e.direction == Direction::CHILD &&
                                     !e.hidden;
                            }) |
         std::views::transform(
             [](std::pair<const N&, const RJoin&> t) { return t.first; });
}

template <typename N>
requires RelationAble<N> auto QueryGraph<N>::get_children(const N& n) {
  return std::ranges::views::filter(r[n],
                                    [](std::pair<const N&, const RJoin&> t) {
                                      // TODO: structural binding in args
                                      auto const& [x, e] = t;
                                      return e.direction == Direction::PARENT &&
                                             !e.hidden;
                                    }) |
         std::ranges::views::transform(
             [](std::pair<const N&, const RJoin&> t) { return t.first; });
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
requires RelationAble<N> auto QueryGraph<N>::rank(N n) -> float {
  // TODO: unpack hist here?
  std::vector<N> seq{n};

  // assert rank [0, 1]
  return (T(seq) - 1) / C(seq);
}

template <typename N>
requires RelationAble<N> auto QueryGraph<N>::T(std::span<N> seq) -> float {
  return std::transform_reduce(
      seq.begin(), seq.end(), 1.0f, std::multiplies{}, [&](const N& n) {
        return selectivity.at(n) * static_cast<float>(cardinality.at(n));
      });
}
template <typename N>
requires RelationAble<N> auto QueryGraph<N>::C(std::vector<N>& seq) -> float {
  std::vector<N> v{};

  for (auto const& x : seq)
    //    if (hist.contains(x) && hist.at(x).empty())
    if (hist[x].empty())
      v.push_back(x);
    else
      for (auto const& h : hist.at(x)) v.push_back(h);
  // return 0 if Ri is root 113/637
  //    if (v.size() == 1 && v.front() == root) return 0;

  if (v.empty()) return 0;
  if (v.size() == 1)
    return selectivity.at(v.front()) *
           static_cast<float>(cardinality.at(v.front()));  // T(v)

  //    auto s1 = seq | std::views::take(1);
  //    auto s2 = seq | std::views::drop(1);

  auto s1 = std::vector<N>{v.front()};
  auto s2 = std::vector<N>(v.begin() + 1, v.end());

  // std::span(v.begin()+1, v.end())
  return C(s1) + T(s1) * C(s2);
}
template <typename N>
requires RelationAble<N> auto QueryGraph<N>::C(std::set<N>& seq) -> float {
  std::vector<N> t(seq.begin(), seq.end());
  return C(t);
}

template <typename N>
requires RelationAble<N> auto QueryGraph<N>::iter() -> std::vector<N> {
  auto erg = std::vector<N>();
  auto q = std::queue<N>();
  auto v = std::set<N>();

  // TODO: switch to get_descendents(root); with unordered_set
  // TODO: assert root absence
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

  for (auto const& [x, e] : r[n]) {
    if (e.hidden) continue;
    std::cout << n.getLabel() << " " << x.getLabel() << " "
              << static_cast<int>(e.direction) << " "
              << static_cast<int>(e.hidden) << "\n";
    visited.insert(n);

    iter(x, visited);
  }
}

template <typename N>
requires RelationAble<N> Direction QueryGraph<N>::inv(Direction dir) {
  const std::map<Direction, Direction> m{
      {Direction::UNDIRECTED, Direction::UNDIRECTED},
      {Direction::PARENT, Direction::CHILD},
      {Direction::CHILD, Direction::PARENT},
  };

  return m.at(dir);
}

}  // namespace JoinOrdering
