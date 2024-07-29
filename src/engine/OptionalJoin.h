// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

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
               std::shared_ptr<QueryExecutionTree> t2);

 private:
  string getCacheKeyImpl() const override;

 public:
  string getDescriptor() const override;

  size_t getResultWidth() const override;

  vector<ColumnIndex> resultSortedOn() const override;

  bool knownEmptyResult() override { return _left->knownEmptyResult(); }

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

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
  void optionalJoin(
      const IdTable& left, const IdTable& right,
      const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
      IdTable* dynResult,
      Implementation implementation = Implementation::GeneralCase);

 private:
  void computeSizeEstimateAndMultiplicities();

  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  // Check which of the join columns in `left` and `right` contain UNDEF values
  // and return the appropriate `Implementation`.
  static Implementation computeImplementationFromIdTables(
      const IdTable& left, const IdTable& right,
      const std::vector<std::array<ColumnIndex, 2>>&);
};
