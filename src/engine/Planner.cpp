// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "Planner.h"

// _____________________________________________________________________________
QueryExecutionTree Planner::createQueryExecutionTree(const ParsedQuery& query) {
  // Each triple leads to a ResultTable
  // Each variable repetition is a join
  // Planned joins determine the ordering required from ResultTables.
  // TODO: Cyclic queries are delayed for now
  // TODO: Variables for predicates are delayed for now.
  // TODO: Text is delayed for now.

  // Given the above, there are two kinds of triple:
  // 1. One variable - the ResultTable has 1 column, the ordering required will
  // always be by the variable column and it can be read directly from the
  // P - otherFixed - var permutation
  // 2. Two variables - The Result Table has two columns, the ordering
  // is determined by the join it is needed in and can be read directly
  // from the according permutation

  // The simple join ordering can use exact numbers for triples of kind 1
  // from the actual ResultTables (because it is sure how they have to be read)
  // and the size for all ResultTables for triples of kind 2 is the size
  // of the relation and feasible to pre-compute.
  // Result cardinality of joins is not know and for now no statistic are used.
  // We just predict the size of a full cartesian product as very simple
  // heuristic.

  // All joins for a variable always have to be done at the same level in
  // the tree, other variables will require re-ordering.
  // Therefore the QueryGraph will have nodes for variables.
  // Variable with more than one occurrence have degree > 1,
  // Leaves will be triples of type 1 and triples of type 2 where
  // One of the variables only occurs once.
  // An internal node will have out degree according to the number of
  // occurrences of its variable and edges (labeled with a relation name)
  // represent joins done at the level.

  // In conclusion, I want to build a query graph where S and O are nodes,
  // and predicates are edge labels. Literals, URIs, etc and
  // variables with only one occurrence will lead to nodes with degree 1.
  // At nodes with degree > 1, a number of joins will happen.
  // The order can be determined by the heuristic described above.
  // In order to find an execution plan, take the node with degree 1,
  // that has the lowest expected cardinality, describe its result by
  // operations (with is either a trivial scan or involves a number of joins)
  // and collapse it's result into the neighbor which will get a lower degree.
  // Continue until only one node is left.
  // If there is no node with degree 1, there is a cycle which can be
  // resolved by temporarily removing an edge and using it later to filter
  // the result. TODO: delay this for now.

  // Ordering joins inside a node can be done smallest-first, again.

  // Columns always correspond to variables and this has to be remembered
  // throughout join operations.
  // Intermediate results will feature only a limited set of variables.
  // Projections can be done in the end (leads to lower
  // availability of optimized joins) or in between if columns are not needed
  // in the select clause.

  return QueryExecutionTree();
}