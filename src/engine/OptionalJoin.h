// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_OPTIONALJOIN_H
#define QLEVER_SRC_ENGINE_OPTIONALJOIN_H

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

// Forward declaration
class IndexScan;
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

  // Specify whether the join columns should be part of the result.
  bool keepJoinColumns_ = true;

 public:
  OptionalJoin(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> t1,
               std::shared_ptr<QueryExecutionTree> t2,
               bool keepJoinColumns = true);

 private:
  std::string getCacheKeyImpl() const override;

 public:
  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  std::vector<ColumnIndex> resultSortedOn() const override;

  bool knownEmptyResult() override { return _left->knownEmptyResult(); }

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

  // Joins two result tables on any number of columns, inserting the special
  // value `Id::makeUndefined()` for any entries marked as optional.
  void optionalJoin(
      const IdTable& left, const IdTable& right,
      const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
      IdTable* dynResult,
      Implementation implementation = Implementation::GeneralCase);

  // Joins two results on a single join column lazily, inserting the special
  // value `Id::makeUndefined()` for any entries marked as optional.
  Result lazyOptionalJoin(std::shared_ptr<const Result> left,
                          std::shared_ptr<const Result> right,
                          bool requestLaziness);

  // Compute the result for the result from the `left` subtree
  // and the `rightScan`. This function applied block prefiltering for the
  // `rightScan`. This function currently only supports single-column OPTIONAL
  // joins, or OPTIONAL joins on two columns where UNDEF values are only in the
  // last (i.e. the second) join column. The `left` and `rightScan` have to be
  // obtained from the members `_left` and `_right` respectively, as those
  // members will be used to get additional required metadata for the arguments
  // `left` and `rightScan`.
  Result optionalJoinWithIndexScan(std::shared_ptr<const Result> left,
                                   std::shared_ptr<IndexScan> rightScan,
                                   bool requestLaziness);

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  // Helper function for `tryIndexNestedLoopJoinIfSuitable` which makes the
  // logic reusable.
  bool isIndexNestedLoopJoinSuitable() const;

  // Nested loop join optimization than can apply when a memory intensive sort
  // can be avoided this way.
  std::optional<Result> tryIndexNestedLoopJoinIfSuitable(bool requestLaziness);

  std::optional<std::shared_ptr<QueryExecutionTree>>
  makeTreeWithStrippedColumns(
      const std::set<Variable>& variables) const override;

  void computeSizeEstimateAndMultiplicities();

  Result computeResult(bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  // Check which of the join columns in `left` and `right` contain UNDEF values
  // and return the appropriate `Implementation`.
  static Implementation computeImplementationFromIdTables(
      const IdTable& left, const IdTable& right,
      const std::vector<std::array<ColumnIndex, 2>>&);
};

#endif  // QLEVER_SRC_ENGINE_OPTIONALJOIN_H
