// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <concepts>
#include <map>
#include <queue>
#include <ranges>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "EdgeInfo.h"
#include "util/Exception.h"
#include "util/HashMap.h"

namespace JoinOrdering {

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

template <typename N>
requires RelationAble<N> class QueryGraph {
 public:
  QueryGraph() = default;

  std::vector<N> relations_;
  //  ad_utility::HashMap<N, ad_utility::HashMap<N, EdgeInfo>> edges_;
  //  ad_utility::HashMap<N, std::vector<N>> hist;
  //  ad_utility::HashMap<N, int> cardinality;
  //  ad_utility::HashMap<N, float> selectivity;

  std::map<N, std::map<N, EdgeInfo>> edges_;
  std::map<N, std::optional<std::pair<N, N>>> hist;
  std::map<N, int> cardinality;
  std::map<N, float> selectivity;  // FIXME: directed unordered pair
  N root;

  /**
   * Add a relation to the query graph and and append it's cardinality
   * to the graph's cardinality lookup table
   *
   * ref: 77/637
   * TODO: 91/637 do not add single relations, but subchains
   * @param n Relation with a cardinality property (getCardinality)
   */
  void add_relation(const N& n);

  /**
   * Check whether a give relation has been added to the query graph or not.
   *
   * @param n Relation to check
   * @return True if it has been added before with QueryGraph::add_relation
   */
  bool has_relation(const N& n) const;

  /**
   *
   * Disable any edge between a relation and all of it's neighbours
   * (parent and children) effectively removing it.
   *
   * @param n Relation to set as unreachable.
   */
  void rm_relation(const N& n);

  /**
   *
   * Checks whether a give relation is regular or compound.
   *
   * regular relations are ones added during construction of the QueryGraph
   * compound relation are the result of the QueryGraph::combine
   *
   * @param n Relation
   * @return True if Relation n is a compound relation
   */
  bool is_compound_relation(const N& n) const;

  /**
   *
   * Checks whether Relation n is a common neighbour between
   * Relation a and Relation b.
   *
   * Relation n is a common neighbour of Relation a and Relation b if
   * there exists a connection between Relation n and Relation a
   * AND
   * there exists a connection between Relation n and Relation b
   *
   * @param a Relation a
   * @param b Relation b
   * @param n Relation n
   * @return True if Relation n is a common neighbour between a and b.
   */
  bool is_common_neighbour(const N& a, const N& b, const N& n) const;

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
   * @param join_selectivity selectivity of the join with Relation B
   * @param dir Relation A is a (dir) to Relation B
   */
  void add_rjoin(const N& a, const N& b, float join_selectivity,
                 Direction dir = Direction::UNDIRECTED);

  /**
   * Check whether there is a connection between given 2 relations
   *
   * @param a Relation
   * @param b Relation
   * @return True if a connection has been created with QueryGraph::add_rjoin
   */
  [[nodiscard]] bool has_rjoin(const N& a, const N& b) const;

  /**
   * Remove connection between 2 given relations by setting `hidden` attribute
   * to true, effectively removing the connection from the query graph
   * @param a Relation
   * @param b Relation
   */
  void rm_rjoin(const N& a, const N& b);

  /**
   * Gets all **direct** neighbours (1-level) of a given relation where
   * relation n is set as a Direction::PARENT to the neighbour relation.
   *
   * Ignores any connections where hidden is set to true.
   * @see QueryGraph::iter
   * @param n Relation
   * @return A view to the children of Relation n
   */

  auto get_children(const N& n) const {
    return std::views::filter(edges_.at(n),  // edges_[n]
                              [](std::pair<const N&, const EdgeInfo&> t) {
                                // TODO: structural binding in args
                                auto const& [x, e] = t;
                                return e.direction == Direction::PARENT &&
                                       !e.hidden;
                              }) |
           std::views::transform(
               [](std::pair<const N&, const EdgeInfo&> t) { return t.first; });
  }

  /**
   * Gets the direct parent of a given relation where relation n is set as a
   * Direction::CHILD to the neighbour relation.
   *
   * Ignores any connections where hidden is set to true.
   *
   * Similar to QueryGraph::get_children
   * @param n Relation
   * @return A view to the parent of Relation n
   */
  auto get_parent(const N& n) const {
    return std::views::filter(edges_.at(n),  // edges_[n],
                              [](std::pair<const N&, const EdgeInfo&> t) {
                                auto const& [x, e] = t;
                                return e.direction == Direction::CHILD &&
                                       !e.hidden;
                              }) |
           std::views::transform(
               [](std::pair<const N&, const EdgeInfo&> t) { return t.first; });
  }

  /**
   * Recursively breaks down a compound relation till into basic relations.\
   *
   * @param n Compound Relation
   * @param erg Vector of n's basic relations
   * @see JoinOrdering::IKKBZ_combine
   * @see JoinOrdering::IKKBZ_uncombine
   */
  void unpack(const N& n, std::vector<N>& erg);
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
  bool is_chain(const N& n) const;

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
  bool is_subtree(const N& n) const;

  /**
   *
   * Looks for the first subtree that exists as a descendant to Relation n.
   *
   * @param n Relation
   * @return the root of the subtree whose subtrees are chains
   * @see IKKBZ_Normalized
   */
  auto get_chained_subtree(const N& n) -> N;

  // TODO: std::iterator or std::iterator_traits

  /**
   * Get all relations in a query graph starting from it's root
   *
   * @return vector of all relation in the QueryGraph
   */
  auto iter() -> std::vector<N>;

  /**
   * Gets ALL relations where given relation n is an ancestor
   * (parent, grandparent, ...).
   *
   * Relation n itself is ALSO include in the resultant set (for convenience).
   *
   * @see QueryGraph::get_children
   * @param n Relation
   * @return vector of lineage relations to give Relation N including n itself
   */
  auto iter(const N& n) -> std::vector<N>;

  /**
   * Gets ALL relations pairs on a the QueryGraph.
   *
   * @return set of pairs of connected relations
   */
  auto iter_pairs() -> std::vector<std::pair<N, N>>;

  /**
   * Gets relation pairs that involve a particular relation.
   * i.e the direct connected neighbours of give relation n.
   *
   * @param n Relation n
   * @return set of pairs of connected relations that involve n
   * @see iter_pairs()
   */
  auto iter_pairs(const N& n) -> std::vector<std::pair<N, N>>;

  /**
   *
   * used to assign bidirectional connections when populating the QueryGraph
   *
   *
   * inverse of a DIRECTION::PARENT is DIRECTION::CHILD
   * inverse of a DIRECTION::CHILD is DIRECTION::PARENT
   * inverse of a DIRECTION::UNDIRECTED is DIRECTION::UNDIRECTED
   *
   * @see QueryGraph::add_rjoin
   */
  constexpr static Direction inv(Direction);
};

}  // namespace JoinOrdering
