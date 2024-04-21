// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#pragma once
#include <iostream>
#include "JoinTree.hpp"

namespace JoinOrdering {

void _toPrecedenceGraph(JoinTree& g, const Relation& n) {  // NOLINT
  for (auto& [x, e] : g.r[n]) {
    if (g.r[n][x].direction != Direction::UNDIRECTED) continue;
    g.rm_rjoin(n, x);
    g.add_rjoin(n, x, g.selectivity[x], Direction::PARENT);
    _toPrecedenceGraph(g, x);
  }
}

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
 * 106/637
 *
 * @param g query graph
 * @param root starting relation
 * @return directed query graph
 */
[[nodiscard("use mutated graph")]] auto toPrecedenceGraph(JoinTree& g,
                                                          const Relation& root)
    -> JoinTree {
  g.root = root;
  _toPrecedenceGraph(g, root);
  // TODO: std::move?
  return g;  // graph copy
}

/**
 * continued process of building compound relations until
 * no contradictory sequences exist.
 *
 * merges relations that would have been reorder if only considering the rank
 * guarantees that rank is ascending in each subchain
 *
 *
 * ref: 119,122/637
 * @param g
 * @param subtree_root
 * @return
 * @see JoinOrdering::combine
 */
// FIXME: unbelievably stupid
[[nodiscard("check pre-merge")]] bool IKKBZ_Normalized(
    JoinTree& g, const Relation& subtree_root) {
  for (auto const& d : g.get_descendents(subtree_root)) {
    auto pv = g.get_parent(d);
    if (pv.empty()) continue;
    auto p = pv.front();

    if (p == g.root) continue;  // TODO: check skip norm root
    if (d == subtree_root || p == subtree_root) continue;

    auto cxs = g.get_children(p);
    for (auto const& c : cxs)
      // 118/637
      // precedence graph demands A -> B but rank(A) > rank(B),
      // we speak of contradictory sequences.
      if (g.rank(p) > g.rank(c)) {
        // a new node representing compound relation
        g.combine(p, c);
        return false;
      }
  }
  return true;  // ready to merge
}

/**
 * the opposite step of JoinOrdering::IKKBZ_Normalized.
 *
 * replacing every compound relation by the sequence of relations
 * it was derived from
 *
 * ref: 119/637
 * @param g
 * @see JoinOrdering::uncombine
 */
void IKKBZ_denormalize(JoinTree& g) {
  // TODO: garbage
  // TODO: check against rooted at R3 before refactor
  while (
      !std::ranges::all_of(g.get_descendents(g.root), [g](const Relation& n) {
        if (g.hist.contains(n)) return g.hist.at(n).empty();
        return true;
      }))
    //  std::ranges::for_each(g.get_descendents(g.root),
    //                        [g](const Relation& x) { return g.uncombine(x);
    //                        });

    for (auto const& x : g.get_descendents(g.root)) g.uncombine(x);
}

/**
 * transform precedence graph into chain
 *
 * ref: 121/637
 * @param g acyclic query graph
 */
void IKKBZ_Sub(JoinTree& g) {
  while (!g.is_chain(g.root)) {
    auto subtree = g.get_chained_subtree(g.root);

    while (!IKKBZ_Normalized(g, subtree))
      ;
    g.merge(subtree);
  }
  IKKBZ_denormalize(g);
}

/**
 * Polynomial algorithm for join ordering
 *
 * produces optimal left-deep trees without cross products
 * requires acyclic join graphs
 *
 * Can be used as heuristic if the requirements are violated
 *
 * ref: 103,120/637
 *
 * @param g acyclic query graph
 * @param n relation used as root for the JoiningOrder::toPrecedenceGraph
 * @return optimal left-deep tree
 */
auto IKKBZ(JoinTree g, const Relation& n) -> JoinTree {
  // TODO: argmin over all rooted relations
  auto new_g = toPrecedenceGraph(g, n);
  IKKBZ_Sub(new_g);
  return new_g;
}

}  // namespace JoinOrdering
