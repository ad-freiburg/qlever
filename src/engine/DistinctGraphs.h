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
#include "util/Algorithm.h"

// Operation that produces a single-column result containing all distinct
// graph IRIs present in the index. It is used to evaluate SPARQL patterns of
// the form `GRAPH ?g { ... }` where `?g` is not bound by the inner pattern.
// The default graph is only part of the result if `includeDefaultGraph` is
// set, which the query planner derives from the runtime parameter
// `treat-default-graph-as-named-graph`.
//
// The implementation reads graph IDs directly from the block metadata of the
// SPO permutation, falling back to a full block decompression only when a
// block may contain a previously unseen graph ID. For most datasets this
// avoids a full table scan.
//
// Example SPARQL queries that use this class:
//
//  SELECT ?g WHERE { GRAPH ?g { } } -> gives all distinct graphs in the dataset
//  SELECT ?g WHERE { GRAPH ?g { VALUES ?x { <something> } } } -> cartesian
//  product of all distinct graphs with all values of ?x
//
// TODO<metetolga> `SELECT ?g WHERE { GRAPH ?g { VALUES ?g { <something> } } }`
// is not covered yet.
//
// The query planner injects a `DistinctGraphs` operation whenever it detects
// a `GRAPH ?g { <inner> }` pattern where `?g` is not already bound by
// `<inner>`.
class DistinctGraphs : public Operation {
 public:
  DistinctGraphs(QueryExecutionContext* qec, Variable graphVariable,
                 bool includeDefaultGraph);

  [[nodiscard]] std::string getDescriptor() const override {
    return "Distinct Graphs";
  }

  // The result table has only 1 column.
  [[nodiscard]] size_t getResultWidth() const override { return 1; }

  // The metadata of every block is inspected, but typically only very few
  // blocks are decompressed, so the number of blocks is a reasonable estimate.
  size_t getCostEstimate() override;

  // All values are distinct by design. There are no duplicates.
  float getMultiplicity(size_t) override { return 1.0f; }

  bool knownEmptyResult() override { return false; }

  // Each graph ID appears once, so the result is distinct iff column 0 is kept.
  bool isDistinctByImpl(
      const std::vector<ColumnIndex>& distinctIndices) const override {
    return ad_utility::contains(distinctIndices, ColumnIndex{0});
  }

  std::unique_ptr<Operation> cloneImpl() const override;

 protected:
  // No column is sorted in result.
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override {
    return {};
  }

 private:
  // `DistinctGraphs` reads directly from the index and has no children.
  std::vector<QueryExecutionTree*> getChildrenImpl() const override {
    return {};
  }

  // The variable name is not part of the cache key, the result is the same
  // for every variable. Whether the default graph is included has to be part
  // of it, because the runtime parameter that decides this can change
  // between two queries.
  [[nodiscard]] std::string getCacheKeyImpl() const override;

  // Return the last saved number of distinct graphs.
  uint64_t getSizeEstimateBeforeLimit() override {
    return numOfDistinctGraphs_;
  }

  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  Result computeResult([[maybe_unused]] bool requestLaziness) override;

  // Only one column in the result and it is always defined.
  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;

  // The graph variable of queries of the form: `SELECT * { GRAPH ?g { ... }}`.
  Variable graphVariable_;

  // If false, the default graph is removed from the result.
  bool includeDefaultGraph_;

  // Last computed number of distinct graphs, initially
  // `MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA`. Deliberately shared by all
  // instances, so that the size estimate of a query benefits from the result
  // of a previous query (the result is the same for all instances on the same
  // index anyway).
  inline static std::atomic<uint64_t> numOfDistinctGraphs_{
      MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA};
};

#endif
