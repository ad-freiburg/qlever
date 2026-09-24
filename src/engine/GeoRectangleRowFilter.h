// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_GEORECTANGLEROWFILTER_H
#define QLEVER_SRC_ENGINE_GEORECTANGLEROWFILTER_H

#include "engine/Operation.h"
#include "rdfTypes/GeoRectangle.h"

// An operation that returns the rows of its only child whose geometry in a
// given column is not certainly outside a query rectangle, decided per row
// from the `ValueId` alone (the coordinates of a `GeoPoint`, see
// `GeoRectangleIdPrefilter`). It complements the block prefilter of an
// `IndexScan`: the block prefilter can only drop whole blocks, and for
// `GeoPoint`s it can only restrict the latitude, so the kept blocks may still
// hold many rows outside the rectangle. Dropping them here, before any `Sort`
// or `Join` above the scan, keeps those operations small. Everything else
// (result order, variables, multiplicities) is passed through from the child.
//
// The size estimate is the child's, unless an explicit estimate is given:
// the query planner puts this filter on every scan that binds the geometry
// variable, and gives all of them the estimate of the scan whose blocks
// could be pruned, so that the estimates do not depend on the permutation.
class GeoRectangleRowFilter : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> child_;
  ColumnIndex geometryColumn_;
  ad_utility::GeoRectangle rectangle_;
  ad_utility::GeoRectangleIdPrefilter prefilter_;
  std::optional<uint64_t> sizeEstimate_;

 public:
  GeoRectangleRowFilter(QueryExecutionContext* ctx,
                        std::shared_ptr<QueryExecutionTree> child,
                        ColumnIndex geometryColumn,
                        const ad_utility::GeoRectangle& rectangle,
                        std::optional<uint64_t> sizeEstimate = std::nullopt);

  std::string getCacheKeyImpl() const override;
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

 private:
  std::vector<QueryExecutionTree*> getChildrenImpl() const override;
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }
  std::unique_ptr<Operation> cloneImpl() const override;
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override;
  Result computeResult(bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;

  // Remove the rows of `table` that can be skipped.
  void filterTable(IdTable& table) const;
};

#endif  // QLEVER_SRC_ENGINE_GEORECTANGLEROWFILTER_H
