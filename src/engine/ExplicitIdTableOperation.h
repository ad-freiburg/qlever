// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_EXPLICITIDTABLEOPERATION_H
#define QLEVER_SRC_ENGINE_EXPLICITIDTABLEOPERATION_H

#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "engine/Result.h"

// An operation that owns its explicit `Result` via `shared_ptr`s and just
// returns this result when `computeResult` is called.
class ExplicitIdTableOperation : public Operation {
 private:
  std::shared_ptr<const IdTable> idTable_;
  VariableToColumnMap variables_;
  std::vector<ColumnIndex> sortedColumns_;
  LocalVocab localVocab_;
  std::string cacheKey_;

 public:
  ExplicitIdTableOperation(QueryExecutionContext* ctx,
                           std::shared_ptr<const IdTable> table,
                           VariableToColumnMap variables,
                           std::vector<ColumnIndex> sortedColumns,
                           LocalVocab localVocab, std::string cacheKey);

  // Const and public getter for testing.
  size_t sizeEstimate() const { return idTable_->numRows(); }

  // Overridden methods from the `Operation` base class.
  std::vector<QueryExecutionTree*> getChildren() override;
  std::string getCacheKeyImpl() const override;
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;
  uint64_t getSizeEstimateBeforeLimit() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;
  std::unique_ptr<Operation> cloneImpl() const override;
  std::vector<ColumnIndex> resultSortedOn() const override;
  VariableToColumnMap computeVariableToColumnMap() const override;
  Result computeResult(bool requestLaziness) override;
};

#endif  // QLEVER_SRC_ENGINE_EXPLICITIDTABLEOPERATION_H
