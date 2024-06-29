// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once

#include <execution>
#include <numeric>

#include "ICostASI.h"
#include "QueryGraph.h"

namespace JoinOrdering {

/**
 *
 * Polynomial algorithm for join ordering
 *
 * produces optimal left-deep trees without cross products
 * requires acyclic join graphs
 * cost function must have ASI property
 *
 * Can be used as heuristic if the requirements are violated
 *
 * ref: 103,120/637
 *
 * @param g acyclic query graph
 * @tparam N type that satisfies IKKBZ::RelationAble concept
 * @return optimal left-deep tree
 */
template <typename N>
requires RelationAble<N> auto IKKBZ(QueryGraph<N> g) -> std::vector<N>;

/**
 *
 * Generate a precedence graph out of an undirected graph and trigger
 * the main subroutine.
 *
 * @param g acyclic query graph
 * @param n relation used as root for the JoinOrdering::toPrecedenceGraph
 * @return left-deep tree rooted at n
 */
template <typename N>
requires RelationAble<N>
auto IKKBZ(QueryGraph<N> g, const N& n) -> QueryGraph<N>;

/**
 *
 * Generate a precedence graph out of an undirected graph and trigger
 * the main subroutine.
 *
 * @param g acyclic query graph
 * @param Ch cost function that has ASI property
 * @param n relation used as root for the JoinOrdering::toPrecedenceGraph
 * @return left-deep tree rooted at n
 */
template <typename N>
requires RelationAble<N>
auto IKKBZ(QueryGraph<N> g, ICostASI<N>& Ch, const N& n) -> QueryGraph<N>;

/**
 * The precedence graph describes the (partial) ordering of joins
 * implied by the query graph.
 *
 * z.B:
 *

 R1  -+         +-  R5
      |         |

     R3   ---  R4

      |         |
 R2  -+         +-  R6

     query graph



  R1

   |
   |
   v

  R3   -->  R2

   |
   |
   v

  R4   -->  R6

   |
   |
   v

  R5


 precedence graph rooted in R1
 *
 * ref: 106/637
 *
 * @param g acyclic query graph
 * @param root starting relation
 * @return new query graph (precedence tree)
 */
template <typename N>
requires RelationAble<N>
[[nodiscard]] auto toPrecedenceGraph(QueryGraph<N>& g, const N& root)
    -> QueryGraph<N>;

template <typename N>
requires RelationAble<N> void IKKBZ_Sub(QueryGraph<N>& g);

/**
 * continued process of building compound relations until
 * no contradictory sequences exist.
 *
 * "merges relations that would have been reorder if only considering the rank
 * guarantees that rank is ascending in each subchain"
 *
 *
 * ref: 119,122/637
 * @param g precedence tree
 * @param subtree_root subtree of g
 * @param Ch cost function that has ASI property
 * @return normalized relations under given subtree
 * @see IKKBZ_combine
 * @see IKKBZ_merge
 */
template <typename N>
requires RelationAble<N>
std::vector<N> IKKBZ_Normalized(QueryGraph<N>& g, ICostASI<N>& Ch,
                                const N& subtree_root);

/**
 * Merge the chains under relation n according the rank function.
 *
 * post IKKBZ_Normalized,
 * if rank(b) < rank(cd) and a -> b, a -> cd
 * then we merge them into a single chain where a is
 * the subtree_root
 *
 * ref: 121,126/637
 * @param g precedence tree with subchains ready to merge
 * @param Ch cost function that has ASI property
 * @param normalized_subtree vector of the relations in all the chains
 * @see IKKBZ_Normalized
 */
template <typename N>
requires RelationAble<N> void IKKBZ_merge(QueryGraph<N>& g, ICostASI<N>& Ch,
                                          std::vector<N>& normalized_subtree);

/**
 * Given 2 Relations (already exist on the QueryGraph),
 * combine there 2 relation into a new compound relation.
 *
 * All descendents of Relation a and Relation b become descendents of the newly
 * created relation ab. Relation a and Relation b are expected to be neighbours.
 *
 * Does NOT work with undirected graph, in such case use GOO_combine instead.
 *
 * @param g precedence tree
 * @param a Relation a
 * @param b Relation b
 * @return Relation ab
 * @see IKKBZ_uncombine
 */
template <typename N>
requires RelationAble<N>
[[maybe_unused]] N IKKBZ_combine(QueryGraph<N>& g, const N& a, const N& b);

/**
 * Inverse operation of IKKBZ_combine.
 *
 * Spread a compound relation back into it's direct components.
 * @param n Compound Relation
 * @see QueryGraph::unpack
 * @see IKKBZ_denormalize
 */
template <typename N>
requires RelationAble<N> void IKKBZ_uncombine(QueryGraph<N>& g, const N& n);

/**
 * the opposite step of JoinOrdering::IKKBZ_Normalized.
 *
 * transform precedence tree into a single chain
 *
 * replacing every compound relation by the sequence of relations
 * it was derived from
 *
 * ref: 119,121/637
 * @param g precedence tree
 * @see IKKBZ_uncombine
 */
template <typename N>
requires RelationAble<N> void IKKBZ_Denormalize(QueryGraph<N>& g);

}  // namespace JoinOrdering
