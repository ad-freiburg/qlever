// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include <list>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;

class OptionalJoin : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  std::vector<std::array<ColumnIndex, 2>> _joinColumns;

  std::vector<float> _multiplicities;
  size_t _sizeEstimate;
  bool _multiplicitiesComputed;

 public:
  OptionalJoin(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> t1,
               std::shared_ptr<QueryExecutionTree> t2,
               const std::vector<std::array<ColumnIndex, 2>>& joinCols);

 private:
  string asStringImpl(size_t indent = 0) const override;

 public:
  string getDescriptor() const override;

  size_t getResultWidth() const override;

  vector<size_t> resultSortedOn() const override;

  void setTextLimit(size_t limit) override {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  bool knownEmptyResult() override { return _left->knownEmptyResult(); }

  float getMultiplicity(size_t col) override;

 private:
  size_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

  vector<QueryExecutionTree*> getChildren() override {
    return {_left.get(), _right.get()};
  }

  /**
   * @brief Joins two result tables on any number of columns, inserting the
   *        special value ID_NO_VALUE for any entries marked as optional.
   * @param a
   * @param b
   * @param joinColumns
   * @param result
   */
  static void optionalJoin(
      const IdTable& dynA, const IdTable& dynB,
      const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
      IdTable* dynResult);
  static void specialOptionalJoin(
      const IdTable& dynA, const IdTable& dynB,
      const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
      IdTable* dynResult);

 private:
  void computeSizeEstimateAndMultiplicities();

  /**
   * @brief Takes a row from each of the input tables and creates a result row
   * @param a A row from table a.
   * @param b A row from table b.
   * @param sizeA The size of a row in table a.
   * @param joinColumnBitmap_a A bitmap in which a bit is 1 if the corresponding
   *                           column is a join column
   * @param joinColumnBitmap_b A bitmap in which a bit is 1 if the corresponding
   *                           column is a join column
   * @param joinColumnAToB Maps join columns in a to their counterparts in b
   * @param res the result row
   */
  template <int OUT_WIDTH>
  static void createOptionalResult(const auto& row,
                                   IdTableStatic<OUT_WIDTH>* res);

  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
