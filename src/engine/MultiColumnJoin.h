// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_MULTICOLUMNJOIN_H
#define QLEVER_SRC_ENGINE_MULTICOLUMNJOIN_H

#include <array>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

class MultiColumnJoin : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  std::vector<std::array<ColumnIndex, 2>> _joinColumns;

  std::vector<float> _multiplicities;
  size_t _sizeEstimate;
  bool _multiplicitiesComputed = false;

 public:
  // `allowSwappingChildrenOnlyForTesting` should only ever be changed by tests.
  MultiColumnJoin(QueryExecutionContext* qec,
                  std::shared_ptr<QueryExecutionTree> t1,
                  std::shared_ptr<QueryExecutionTree> t2,
                  bool allowSwappingChildrenOnlyForTesting = true);

 protected:
  std::string getCacheKeyImpl() const override;

 public:
  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  std::vector<ColumnIndex> resultSortedOn() const override;

  bool knownEmptyResult() override {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

  std::vector<QueryExecutionTree*> getChildren() override {
    return {_left.get(), _right.get()};
  }

  bool columnOriginatesFromGraphOrUndef(
      const Variable& variable) const override;

  /**
   * @brief Joins left and right using the column defined int joinColumns,
   *storing the resultMightBeUnsorted in resultMightBeUnsorted. R should have
   *width resultWidth (or be left vector that should have resultWidth entries).
   *This method is made public here for unit testing purposes.
   **/
  void computeMultiColumnJoin(
      const IdTable& left, const IdTable& right,
      const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
      IdTable* resultMightBeUnsorted);

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  Result computeResult([[maybe_unused]] bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  void computeSizeEstimateAndMultiplicities();
};

#endif  // QLEVER_SRC_ENGINE_MULTICOLUMNJOIN_H
