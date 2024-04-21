// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <iterator>
#include <map>
#include <numeric>
#include <ranges>
#include <set>
#include <span>
#include <vector>

namespace JoinOrdering {

enum class Direction {
  UNDIRECTED,
  PARENT,
  CHILD,

};

class Relation {
 public:
  int cardinality{-1};
  std::string label{"R?"};

  Relation() = default;
  Relation(const std::string& label, int cardinality) : Relation() {
    this->label = label;
    this->cardinality = cardinality;
  }

  auto operator<=>(const Relation& other) const = default;
  bool operator==(const Relation& other) const {
    return this->cardinality == other.cardinality && this->label == other.label;
  };
};

class RJoin {  // predicate?
 public:
  float selectivity{-1};  // TODO: DEPRECATED
  Direction direction{Direction::UNDIRECTED};
  bool hidden{false};  // instead of erasing

  RJoin() = default;

  // read from left to right
  // Ra is a dir of Rb
  RJoin(float s, Direction dir) : RJoin() {
    this->selectivity = s;
    this->direction = dir;
  }
};

// typedef std::pair<const Relation&, const RJoin&> RJ;

class JoinTree {
 public:
  JoinTree() = default;

  std::map<Relation, std::map<Relation, RJoin>> r;
  std::map<Relation, std::vector<Relation>> hist;
  std::map<Relation, int> cardinality;
  std::map<Relation, float> selectivity;
  Relation root;

  auto add_relation(const Relation& n) {
    cardinality[n] = n.cardinality;
    return n;
  }

  /**
   *
   *
   * disable any edge between a relation and all of it's neighbours
   * (parent and children) effectively removing it.
   *
   * the hidden property is used to filter out these relation in
   * JoinOrdering::get_parent and JoinOrdering::get_children
   *
   * @param n make relation unreachable.
   */
  void rm_relation(const Relation& n) {
    for (auto& [x, e] : r[n]) {
      r[x][n].hidden = true;
      r[n][x].hidden = true;
    }
  }

  /**
   *
   *
   * ref: 77/637
   * TODO: 91/637 do not add single relations, but subchains
   * @param label
   * @param jcardinality
   * @return Standalone Relation. pending joining.
   */
  [[nodiscard("add with rjoin")]] auto add_relation(const std::string& label,
                                                    int jcardinality) {
    return add_relation(Relation(label, jcardinality));
  }

  /**
   *
   * Connect 2 relations and assign the selectivity for the path.
   *
   * JoinOrdering::toPrecedenceTree will mutated the dir
   * and create parent, children relationships.
   *
   * ref: 76/637
   * @param a Relation A
   * @param b Relation B
   * @param s Join selectivity
   * @param dir Relation A is a (dir) to Relation B
   */
  void add_rjoin(const Relation& a, const Relation& b, float s,
                 Direction dir = Direction::UNDIRECTED) {
    // TODO: assert single parent here?
    r[a][b] = RJoin(s, dir);
    r[b][a] = RJoin(s, inv(dir));

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

  // FIXME: SIGSEGV magnet
  void rm_rjoin(const Relation& a, const Relation& b) {
    //    r[a].erase(b);
    //    r[b].erase(a);

    r[a][b].hidden = true;
    r[b][a].hidden = true;
  }

  [[nodiscard("no side effects")]] bool has_relation(const Relation& n) const {
    return r.contains(n);
  }

  auto get_children(const Relation& n) {
    return std::ranges::views::filter(
               r[n],
               [](std::pair<const Relation&, const RJoin&> t) {
                 // TODO: structural binding in args
                 auto const& [x, e] = t;
                 return e.direction == Direction::PARENT && !e.hidden;
               }) |
           std::ranges::views::transform(
               [](std::pair<const Relation&, const RJoin&> t) {
                 return t.first;
               });
  }

  // TODO: return optional?
  auto get_parent(const Relation& n) {
    // FIXME: SIGSEGV in some stupid corner cases for some stupid reason
    return std::views::filter(r[n],
                              [](std::pair<const Relation&, const RJoin&> t) {
                                auto const& [x, e] = t;
                                return e.direction == Direction::CHILD &&
                                       !e.hidden;
                              }) |
           std::views::transform(
               [](std::pair<const Relation&, const RJoin&> t) {
                 return t.first;
               });
    //        .front() // FIXME: empty .front() undefined behaviour
    //        .first;  // | std::views::take(1);
  }

  auto get_descendents(const Relation& n) {
    // TODO: join views?
    std::set<Relation> acc{};
    get_descendents(n, acc);
    acc.insert(n);  // including frequently used self
    return acc;
  }

  // TODO: std::iterator or std::iterator_traits
  void iter(const Relation& n) {
    std::set<Relation> visited{};
    iter(n, visited);
  }

  // real join
  void ppjoin() {
    // TODO: assert root absence
    auto n = root;
    while (true) {
      auto cxs = get_children(n);
      std::cout << n.label;
      if (cxs.empty()) {
        auto dxs = get_descendents(root);
        std::cout << std::fixed << " (COST w. ROOT " << root.label << ": "
                  << C(dxs) << ")\n";
        return;
      }
      n = cxs.front();
      std::cout << " -> ";
    }
  }

  // TODO: std::iterator or std::iterator_traits
  auto iter() {
    std::vector<Relation> erg{};
    // TODO: assert root absence
    auto n = root;
    while (true) {
      erg.push_back(n);
      auto cxs = get_children(n);
      //      std::cout << n.label;
      if (cxs.empty()) {
        auto dxs = get_descendents(root);
        //        std::cout << std::fixed << " (COST w. ROOT " << root.label <<
        //        ": "
        //                  << C(dxs) << ")\n";
        return erg;
      }
      n = cxs.front();
    }
  }

  // 104/637
  // if the ordering violates the query constraints, it constructs compounds
  auto combine(const Relation& a,
               const Relation& b) {  // -> Compound Relation (hist)

    // TODO: assert chain
    //    std::cout << "COMBINE " << a.label << "  " << b.label << "\n";

    // 118/637
    auto w = cardinality[a] * cardinality[b];
    auto s = selectivity[a] * selectivity[b];
    auto n = add_relation(a.label + "," + b.label, w);
    selectivity[n] = s;
    cardinality[n] = w;

    //    hist[n].push_back(a);
    //    hist[n].push_back(b);

    if (hist[a].empty())
      hist[n].push_back(a);
    else
      for (auto const& x : hist[a]) hist[n].push_back(x);
    if (hist[b].empty())
      hist[n].push_back(b);
    else
      for (auto const& x : hist[b]) hist[n].push_back(x);

    std::set<Relation> parents;
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

    std::set<Relation> children{};
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

  void uncombine(const Relation& n) {
    // assert hist?

    // has never been combined before
    if (hist[n].empty()) return;

    //    std::cout << "UNCOMBINE " << n.label << "\n";

    auto pn = get_parent(n);
    auto cn = get_children(n);

    // FIXME: there is an order when uncombining hist[n]
    // sort by rank?
    // is it the same as @ see merge?
    auto rxs = hist[n];

    std::vector<Relation> v{pn.begin(), pn.end()};
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

  /**
   * ref: 121/637
   * @param n Relation to merge chains under according to rank function
   */
  void merge(const Relation& n) {
    auto dxs = get_descendents(n);
    dxs.erase(n);

    std::vector<Relation> dv(dxs.begin(), dxs.end());

    std::ranges::sort(dv, [this](const Relation& a, const Relation& b) {
      return rank(a) < rank(b);
    });

    if (dv.empty()) return;
    unlink(dv[0]);
    add_rjoin(n, dv[0], selectivity[dv[0]], Direction::PARENT);

    for (size_t i = 1; i < dv.size(); i++) {
      unlink(dv[i]);
      add_rjoin(dv[i - 1], dv[i], selectivity[dv[i]], Direction::PARENT);
    }
  }

  /**
   *
   * Remove all connections between a relation and it's neighbours
   *
   * @param n non-root Relation
   */
  void unlink(const Relation& n) {
    auto cv = get_children(n);
    auto pv = get_parent(n);
    std::set<Relation> children(cv.begin(), cv.end());
    std::set<Relation> parent(pv.begin(), pv.end());

    for (auto const& c : children) rm_rjoin(c, n);
    for (auto const& p : parent) rm_rjoin(p, n);
  }

  /**
   *
   * the factor s_i * n_i determine how much the input relation (to be joined
   * with R_i) changes it's cardinality after join has been performed
   *
   * ref: 112,113/637
   * @param seq
   * @return
   */
  auto T(std::span<Relation> seq) -> float {  // TODO: potential overflow?

    return std::transform_reduce(seq.begin(), seq.end(), 1.0f,
                                 std::multiplies{}, [this](const Relation& n) {
                                   return selectivity.at(n) *
                                          (float)cardinality.at(n);
                                 });
  }

  /**
   *
   * @param seq
   * @return
   */
  // FIXME: DOUBLE CHECK COST FN (113/637)
  // TODO: rewrite with std::span
  float C(std::vector<Relation>& seq) {  // NOLINT
    std::vector<Relation> v{};

    for (auto const& x : seq)
      if (hist[x].empty())
        v.push_back(x);
      else
        for (auto const& h : hist[x]) v.push_back(h);
    // return 0 if Ri is root 113/637
    //    if (v.size() == 1 && v.front() == root) return 0;

    if (v.empty()) return 0;
    if (v.size() == 1)
      return selectivity.at(v.front()) *
             (float)cardinality.at(v.front());  // T(v)

    //    auto s1 = seq | std::views::take(1);
    //    auto s2 = seq | std::views::drop(1);

    auto s1 = std::vector<Relation>{v.front()};
    auto s2 = std::vector<Relation>(v.begin() + 1, v.end());

    // std::span(v.begin()+1, v.end())
    return C(s1) + T(s1) * C(s2);
  }
  // TODO: C should (and can) accept any iterable STL container
  // std:span<Relation>
  float C(std::set<Relation>& seq) {
    std::vector<Relation> t(seq.begin(), seq.end());
    return C(t);
  }

  auto rank(const Relation& n) noexcept -> float {
    // TODO: unpack hist here?
    std::vector<Relation> seq{n};

    // assert rank [0, 1]
    return (T(seq) - 1) / C(seq);
  }

  /**
   *
   * @param n
   * @return True if Relation n is part of a subchain
   */
  bool is_chain(const Relation& n) {  // NOLINT
    auto cv = get_children(n);
    auto len = std::ranges::distance(cv);

    if (len == 0) return true;  // leaf
    if (len > 1) return false;  // another subtree

    // len == 1
    return is_chain(cv.front());
  }

  /**
   *
   * The generalization to bushy trees is not as obvious
   * each subtree must contain a subchain to avoid cross products
   * thus do not add single relations but subchains
   * whole chain must be R1 − . . . − Rn, cut anywhere
   *
   * ref: 91/637
   *
   * @param n
   * @return True if n is NOT a chain a chain and all children ARE chains.
   */
  bool is_subtree(const Relation& n) {
    return !is_chain(n) and
           std::ranges::all_of(get_children(n), [this](const Relation& x) {
             return is_chain(x);
           });
  }

  auto get_chained_subtree(const Relation& n) {
    // TODO: rewrite with std::ranges::find_if
    for (auto const& x : get_descendents(n)) {
      if (is_subtree(x)) return x;
    }

    throw std::runtime_error("how did we get here?");
  }

 private:
  void get_descendents(const Relation& n,  // NOLINT
                       std::set<Relation>& acc) {
    if (acc.contains(n)) return;
    for (auto const& x : get_children(n)) {
      get_descendents(x, acc);
      acc.insert(x);
    }
  }
  void iter(const Relation& n,  // NOLINT
            std::set<Relation>& visited) {
    if (visited.contains(n)) return;

    for (auto const& [x, e] : r[n]) {
      if (e.hidden) continue;
      std::cout << n.label << " " << x.label << " "
                << static_cast<int>(e.direction) << "\n";
      visited.insert(n);

      iter(x, visited);
    }
  }

  static Direction inv(Direction dir) {
    const std::map<Direction, Direction> m{
        {Direction::UNDIRECTED, Direction::UNDIRECTED},
        {Direction::PARENT, Direction::CHILD},
        {Direction::CHILD, Direction::PARENT},
    };

    return m.at(dir);
  }
};

}  // namespace JoinOrdering
