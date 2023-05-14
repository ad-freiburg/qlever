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

  // This `enum` keeps track of which columns in the input contain UNDEF values.
  // This is then used to choose the cheapest possible implementation.
  enum struct Implementation {
    GeneralCase,  // No special implementation possible
    NoUndef,      // None of the join columns contains UNDEF
    OnlyUndefInLastJoinColumnOfLeft
  };

  Implementation implementation_ = Implementation::GeneralCase;

  std::vector<std::array<ColumnIndex, 2>> _joinColumns;

  std::vector<float> _multiplicities;
  size_t _sizeEstimate;
  std::optional<size_t> _costEstimate;
  bool _multiplicitiesComputed = false;

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
      const IdTable& left, const IdTable& right,
      const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
      IdTable* dynResult,
      Implementation implementation = Implementation::GeneralCase);

 private:
  void computeSizeEstimateAndMultiplicities();

  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  // Check which of the join columns in `left` and `right` contain UNDEF values
  // and return the appropriate `Implementation`.
  static Implementation computeImplementationFromIdTables(
      const IdTable& left, const IdTable& right,
      const std::vector<std::array<ColumnIndex, 2>>&);
};
