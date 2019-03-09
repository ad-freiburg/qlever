// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

class MultiColumnJoin : public Operation {
 public:
  MultiColumnJoin(QueryExecutionContext* qec,
                  std::shared_ptr<QueryExecutionTree> t1,
                  std::shared_ptr<QueryExecutionTree> t2,
                  const std::vector<std::array<Id, 2>>& joinCols);

  virtual string asString(size_t indent = 0) const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit) override {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  virtual bool knownEmptyResult() override {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

  /**
   * @brief Joins a and b using the column defined int joinColumns, storing the
   *        result in result. R should have width resultWidth (or be a vector
   *        that should have resultWidth entries).
   *        This method is made public here for unit testing purposes.
   **/
  template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
  static void computeMultiColumnJoin(const IdTable& a, const IdTable& b,
                                     const vector<array<Id, 2>>& joinColumns,
                                     IdTable* result);

 private:
  void computeSizeEstimateAndMultiplicities();

  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  std::vector<std::array<Id, 2>> _joinColumns;

  vector<float> _multiplicities;
  size_t _sizeEstimate;
  bool _multiplicitiesComputed;

  virtual void computeResult(ResultTable* result) override;
};
