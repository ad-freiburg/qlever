// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_RASTERLOOKUP_H
#define QLEVER_SRC_ENGINE_RASTERLOOKUP_H

#include <memory>
#include <string>

#include "engine/Operation.h"
#include "engine/RasterLookupConfig.h"

// For each row of the child, looks up the pixels under its geometry and
// returns one row per pixel: the child's columns plus value (and center).
class RasterLookup : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> subtree_;
  RasterLookupConfiguration config_;
  // The file of `config_.dataset_`, determined once by the constructor.
  std::string path_;

 public:
  RasterLookup(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> subtree,
               RasterLookupConfiguration config);

  size_t getResultWidth() const override;
  std::string getDescriptor() const override;
  size_t getCostEstimate() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

 protected:
  std::vector<ColumnIndex> resultSortedOn() const override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;
  VariableToColumnMap computeVariableToColumnMap() const override;
  qlm::vector<QueryExecutionTree*> getChildrenImpl() const override;
  std::string getCacheKeyImpl() const override;
  std::unique_ptr<Operation> cloneImpl() const override;
  bool isDeterministicImpl() const override;
  Result computeResult(bool requestLaziness) override;
};

#endif  // QLEVER_SRC_ENGINE_RASTERLOOKUP_H
