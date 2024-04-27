// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <concepts>
#include <iostream>
#include <map>
#include <numeric>
#include <queue>
#include <ranges>
#include <set>
#include <span>
#include <string>
#include <vector>

namespace JoinOrdering {

enum class Direction {
  UNDIRECTED,
  PARENT,
  CHILD,

};

class RJoin {  // predicate?
 public:
  Direction direction{Direction::UNDIRECTED};
  bool hidden{false};  // instead of erasing
  RJoin() = default;

  // read from left to right
  // Ra is a dir of Rb
  explicit RJoin(Direction dir) : direction(dir) {}
};

template <typename N>
concept RelationAble = requires(N n) {
  // using the relation as a key for some maps
  // using std::sets all over the place
  // FIXME: constrain hashable
  //  { std::hash<N>{}(n) } -> std::convertible_to<std::size_t>;
  // TODO: static assert with a meaningful diagnostics message
  { n.getCardinality() } -> std::integral;
  { n.getLabel() } -> std::same_as<std::string>;  // std::assignable_from?

  // TODO: check for constructor?
  //  { std::constructible_from<N, std::string, std::integral> };
};

// FIXME: circular dependency from ICostFn.h
// template <typename N>
// requires RelationAble<N> class ICostFn;

template <typename N>
requires RelationAble<N> class QueryGraph {
 public:
  QueryGraph() = default;

  std::map<N, std::map<N, RJoin>> r;
  std::map<N, std::vector<N>> hist;
  std::map<N, int> cardinality;
  std::map<N, float> selectivity;
  N root;

  /**
   * Add a relation to the query graph and and append it's cardinality
   * to the graph's cardinality lookup table (std::map<N, int> cardinality)
   *
   * ref: 77/637
   * TODO: 91/637 do not add single relations, but subchains
   * @param n Relation with a cardinality property (getCardinality)
   * @return the same relation back (TODO: used to make sense, now it doesn't)
   */
  [[nodiscard]] auto add_relation(const N& n) -> N;

  /**
   * Check whether a give relation has been added to the query graph or not.
   *
   * @param n Relation to check
   * @return True if it has been added before with QueryGraph::add_relation
   */
  [[nodiscard]] bool has_relation(const N& n) const;

  /**
   *
   * Disable any edge between a relation and all of it's neighbours
   * (parent and children) effectively removing it.
   *
   * the hidden property is used to filter out these relation in
   * JoinOrdering::get_parent and JoinOrdering::get_children
   *
   * @param n Relation to set as unreachable.
   */
  void rm_relation(const N& n);

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
  void add_rjoin(const N& a, const N& b, float s,
                 Direction dir = Direction::UNDIRECTED);

  /**
   * Check whether there is a connection between given 2 relations
   *
   * @param a Relation
   * @param b Relation
   * @return True if a connection has been created with QueryGraph::add_rjoin
   */
  [[nodiscard]] bool has_rjoin(const N& a, const N& b);

  /**
   * Remove connection between 2 given relations by setting `hidden` attribute
   * to true, effectively removing the connection from the query graph
   * @param a Relation
   * @param b Relation
   */
  void rm_rjoin(const N& a, const N& b);

  /**
   * Gets all the direct neighbours of a given relation where relation n is set
   * as a Direction::PARENT to the neighbour relation.
   *
   * Ignores any connections where hidden is set to true.
   * @see QueryGraph::get_descendents
   * @param n Relation
   * @return A view to the children of Relation n
   */
  auto get_children(const N& n);

  /**
   * Gets the direct parent of a given relation where relation n is set as a
   * Direction::CHILD to the neighbour relation.
   *
   * Ignores any connections where hidden is set to true.
   *
   * Similar to QueryGraph::get_children
   * @param n
   * @return
   */
  auto get_parent(const N& n);

  /**
   * Gets ALL relations where given relation n is an ancestor
   * (parent, grandparent, ...).
   *
   * Relation n itself is ALSO include in the
   * resultant set (for convenience).
   *
   *
   * @see QueryGraph::get_children
   * @param n Relation
   * @return set of lineage relations to give Relation N including n itself
   */
  auto get_descendents(const N& n) -> std::set<N>;

  /**
   * Given 2 Relations (already exist on the QueryGraph),
   * combine there 2 relation into a new compound relation.
   *
   * All descendents of Relation a and Relation b
   * become descendents of the newly created relation ab.
   *
   * Relation a and Relation b are expected to be neighbours.
   *
   *
   * @param a Relation a
   * @param b Relation b
   * @return Relation ab
   */
  N combine(const N& a, const N& b);

  /**
   * Inverse operation of QueryGraph::combine.
   *
   * Spread a compound relation back into it's original components.
   * @param n Compound Relation
   */
  void uncombine(const N& n);

  /**
   * Merge the chains under relation n according the rank function
   *
   * ref: 121/637
   * @param n Relation
   */
  void merge(const N& n);

  /**
   * Remove all connections between a relation and it's neighbours
   *
   * @param n non-root Relation
   */
  void unlink(const N& n);

  /**
   * Give Relation n is said to be part of a chain if all it's descendants
   * have no more than one child each.
   *
   * @param n Relation
   * @return True if Relation n is part of a subchain
   */
  bool is_chain(const N& n);

  /**
   *
   * "The generalization to bushy trees is not as obvious
   * each subtree must contain a subchain to avoid cross products
   * thus do not add single relations but subchains
   * whole chain must be R1 − . . . − Rn, cut anywhere."
   *
   * ref: 91/637
   *
   * @param n Relation
   * @return True if n is NOT a chain a chain and all children ARE chains.
   */
  bool is_subtree(const N& n);

  /**
   *
   * Looks for the first subtree that exists as a descendant to Relation n.
   *
   * @param n Relation
   * @return the root of the subtree whose subtrees are chains
   */
  auto get_chained_subtree(const N& n) -> N;

  /// START Cost function with ASI Property
  /**
   * if rank(R2) < rank(R3) then joining
   * (R1 x R2) x R3 is cheaper than
   * (R1 x R3) x R2
   * @param n Relation
   * @return
   */
  auto rank(N n) -> float;
  auto T(std::span<N> seq) -> float;
  auto C(std::vector<N>& seq) -> float;
  auto C(std::set<N>& seq) -> float;
  //  auto C(N n) -> float;
  /// END Cost function with ASI Property

  // TODO: std::iterator or std::iterator_traits
  void iter(const N& n);

  // TODO: std::iterator or std::iterator_traits
  auto iter() -> std::vector<N>;

 private:
  void get_descendents(const N&, std::set<N>&);
  void iter(const N&, std::set<N>&);

  static Direction inv(Direction);
};

}  // namespace JoinOrdering
