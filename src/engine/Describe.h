// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "parser/GraphPatternOperation.h"

// Operation for DESCRIBE queries according to the Concise Bounded Description
// (CBD) specification: https://www.w3.org/submissions/2005/SUBM-CBD-20050603 .
//
// NOTE: The current implementation recursively expands blank nodes. This can
// be expanded to other reification schemes relatively easily.
class Describe : public Operation {
 private:
  // The query execution tree for computing the WHERE clause of the DESCRIBE.
  std::shared_ptr<QueryExecutionTree> subtree_;

  // The specification of the DESCRIBE clause.
  parsedQuery::Describe describe_;

 public:
  // Create a new DESCRIBE operation.
  Describe(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           parsedQuery::Describe describe);

  // Getter for testing.
  const auto& getDescribe() const { return describe_; }

  // The following functions are overridden from the base class `Operation`.
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

  // Add all triples where the subject is one of the `blankNodes` (an `IdTable`
  // with one column) to the `finalResult`. Recursively continue for all newly
  // found blank nodes (objects of the newly found triples, which are not
  // contained in `alreadySeen`). This is a recursive implementation of
  // breadth-first-search (BFS) where `blankNodes` is the set of start nodes,
  // and `alreadySeen` is the set of nodes which have already been explored,
  // which is needed to handle cycles in the graph. The explored graph is `all
  // triples currently stored by QLever`.
  void recursivelyAddBlankNodes(
      IdTable& finalResult, LocalVocab& localVocab,
      ad_utility::HashSetWithMemoryLimit<Id>& alreadySeen, IdTable blankNodes);

  // Join the `input` (an `IdTable` with one column) with the full index on the
  // subject column. The result has three columns: the subject, predicate, and
  // object of the matching triples. The `localVocab` is updated to contain all
  // local vocab IDs that are possibly contained in the result.
  IdTable makeAndExecuteJoinWithFullIndex(IdTable input,
                                          LocalVocab& localVocab) const;

  // Get the set of (unique) IDs that match one of the variables or IRIs in
  // the DESCRIBE clause and the `result` of the WHERE clause. For example, in
  // `DESCRIBE <x> ?y WHERE { ?y <p> <o>}` The result consists of `<x>` and of
  // all IRIs that match `?y` in the WHERE clause, with all duplicates removed.
  IdTable getIdsToDescribe(const Result& result, LocalVocab& localVocab) const;
};
