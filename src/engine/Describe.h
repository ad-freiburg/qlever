// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "parser/GraphPatternOperation.h"

// An `Operation` which implements SPARQL DESCRIBE queries according to the
// Concise Bounded Description (CBD, see
// https://www.w3.org/submissions/2005/SUBM-CBD-20050603/)
// Note: This implementation currently only recursively expands blank nodes, but
// not other custom reification schemes that might be used.
class Describe : public Operation {
 private:
  // The subtree that computes the WHERE clause of the DESCRIBE query.
  std::shared_ptr<QueryExecutionTree> subtree_;

  // The specification of the DESCRIBE clause.
  parsedQuery::Describe describe_;

 public:
  // Constructor. For details see the documentation of the member variables
  // above.
  Describe(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           parsedQuery::Describe describe);

  // Getter for testing.
  const auto& getDescribe() const { return describe_; }

  // The following functions are all overridden from the base class `Operation`,
  // see there for documentation.
  std::vector<QueryExecutionTree*> getChildren() override;
  string getCacheKeyImpl() const override;

  string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

 private:
  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;
  ProtoResult computeResult(bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;

  // Add all triples, where the subject is one of the `blankNodes` to the
  // `finalResult`. `blankNodes` is a table with one column. Recursively
  // continue for all newly found blank nodes (objects of the newly found
  // triples, which are not contained in `alreadySeen`). This is a recursive
  // implementation of breadth-first-search (BFS) where `blankNodes` is the set
  // of start nodes, and `alreadySeen` is the set of nodes which have already
  // been explored, which is needed to handle cycles in the graph. The explored
  // graph is `all triples currently stored by QLever`.
  void recursivelyAddBlankNodes(
      IdTable& finalResult, LocalVocab& localVocab,
      ad_utility::HashSetWithMemoryLimit<Id>& alreadySeen, IdTable blankNodes);

  // Join the `input` (a single column) with the full index on the subject
  // column. The result has three columns: the subject, predicate, and object of
  // the matching triples. The `localVocab` is updated to contain all local
  // vocab IDs that are possibly contained in the result.
  IdTable makeAndExecuteJoinWithFullIndex(IdTable input,
                                          LocalVocab& localVocab) const;

  // Get the set of (unique) IDs, that match the DESCRIBE clause and the
  // `result` of the WHERE clause. For example, in `DESCRIBE <x> ?y WHERE { ?y
  // <p> <o>}` The result consists of `<x>` and of all IRIs that match `?y` in
  // the WHERE clause, with all duplicates removed.
  IdTable getIdsToDescribe(const Result& result, LocalVocab& localVocab) const;
};
