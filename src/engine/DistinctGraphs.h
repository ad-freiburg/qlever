// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Mete Tolga Gonultas <mg885@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_DISTINCTGRAPHS_H
#define QLEVER_SRC_ENGINE_DISTINCTGRAPHS_H

#include <cstdint>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "index/ConstantsIndexBuilding.h"
#include "rdfTypes/Variable.h"

// Operation that produces a single-column result containing all distinct
// named graph IRIs present in the index. It is used to evaluate SPARQL
// patterns of the form `GRAPH ?g { }` where ?g is a variable that isn't
// guaranteed to be an actual graph in the index.
//
// The implementation reads graph IDs directly from the block metadata of the
// SPO permutation, falling back to a full block decompression only when a
// block may contain a previously unseen graph ID. For most datasets this
// avoids a full table scan
// Example SPARQL query that uses this class:
//
//  SELECT ?g WHERE { GRAPH ?g { } } -> gives all distinct graphs in the dataset
//  SELECT ?g WHERE { GRAPH ?g { VALUES ?x { <something> } } } -> cartesian
//  product of all distinct graphs with all values of ?x
//  TODO: SELECT ?g WHERE { GRAPH ?g { VALUES ?g { <something> } } } -> Not
//  covered yet
//
// The query planner injects a `DistinctGraphs` operation whenever it detects
// a `GRAPH ?g { <inner> }` pattern where `?g` is not already bound by
// `<inner>`.
class DistinctGraphs : public Operation {
 public:
  explicit DistinctGraphs(QueryExecutionContext* qec, Variable graphVariable);

  std::vector<QueryExecutionTree*> getChildren() override { return {}; }

  [[nodiscard]] std::string getDescriptor() const override {
    return "Distinct Graphs";
  }

  // The result table has only 1 column.
  [[nodiscard]] size_t getResultWidth() const override { return 1; }

  // In the worst case every block introduces a new graph ID, forcing
  // decompression of all blocks. The cost is then proportional to the
  // total number of triples in the index.
  size_t getCostEstimate() override;

  // All values are distinct by design. There are no duplicates.
  float getMultiplicity(size_t) override { return 1.0f; }

  bool knownEmptyResult() override { return false; }

  std::unique_ptr<Operation> cloneImpl() const override;

 protected:
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override {
    return {};
  }

 private:
  [[nodiscard]] std::string getCacheKeyImpl() const override {
    return "DistinctGraphs";
  }

  // return last saved #distinctgraphs if valid, otherwise return an optimistic
  // average (MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA = 20)
  uint64_t getSizeEstimateBeforeLimit() override {
    return numOfDistinctGraphs_;
  }

  Result computeResult([[maybe_unused]] bool requestLaziness) override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;

  // The graph variable of queries of the form: `SELECT * { GRAPH ?g { ... }}`
  Variable graphVariable_;

  // Last saved number of distinct graphs, default to
  // MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA
  inline static std::atomic<uint64_t> numOfDistinctGraphs_{
      MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA};
};

#endif
