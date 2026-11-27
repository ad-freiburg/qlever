// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_OPERATION_NEUTRALELEMENTOPERATION_H
#define QLEVER_SRC_ENGINE_OPERATION_NEUTRALELEMENTOPERATION_H

#include "engine/operation/Operation.h"
#include "util/CompilerWarnings.h"
#include "util/ContainersWithAllocator.h"

/// The neutral element wrt `JOIN`. It contains one element, but binds no
/// variables (which means it has 0 columns).
class NeutralElementOperation : public Operation {
 public:
  explicit NeutralElementOperation(QueryExecutionContext* qec)
      : Operation{qec} {}

 private:
  qlm::vector<QueryExecutionTree*> getChildrenImpl() const override {
    return qlm::vector<QueryExecutionTree*>{allocator()};
  }

  // The individual implementation of `getCacheKey` (see above) that has to be
  // customized by every child class.
  [[nodiscard]] qlm::string getCacheKeyImpl() const override {
    return qlm::string{"Neutral Element", allocator()};
  }

 public:
  [[nodiscard]] std::string getDescriptor() const override {
    return "NeutralElement";
  }
  [[nodiscard]] size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }

 private:
  uint64_t getSizeEstimateBeforeLimit() override { return 1; }

 public:
  float getMultiplicity(size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }

  std::unique_ptr<Operation> cloneImpl() const override {
    return std::make_unique<NeutralElementOperation>(_executionContext);
  }

 protected:
  [[nodiscard]] qlm::vector<ColumnIndex> resultSortedOn() const override {
    return qlm::vector<ColumnIndex>{allocator()};
  }

 private:
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  // NOTE: With `USE_SPLIT_LAYOUT`, GCC 13 reports false `-Wstringop-overflow`
  // positives for the `resize` below. The region in `IdColumnVector.h` does
  // not help here, because that header is part of the precompiled header of
  // the engine, where GCC 13 does not keep such regions.
  DISABLE_STRINGOP_OVERFLOW_WARNINGS
  Result computeResult([[maybe_unused]] bool requestLaziness) override {
    IdTable idTable{getExecutionContext()->getAllocator()};
    idTable.setNumColumns(0);
    idTable.resize(1);
    return {std::move(idTable), resultSortedOn(), LocalVocab{allocator()}};
  }
  GCC_REENABLE_WARNINGS

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap()
      const override {
    return {};
  }
};

#endif  // QLEVER_SRC_ENGINE_OPERATION_NEUTRALELEMENTOPERATION_H
