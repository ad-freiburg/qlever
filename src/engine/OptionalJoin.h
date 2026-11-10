// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_OPTIONALJOIN_H
#define QLEVER_SRC_ENGINE_OPTIONALJOIN_H

#include "backports/span.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "util/ContainersWithAllocator.h"

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

  qlm::vector<std::array<ColumnIndex, 2>> _joinColumns;

  std::vector<float> _multiplicities;
  size_t _sizeEstimate = 0;
  std::optional<size_t> _costEstimate;
  bool _multiplicitiesComputed = false;

  // Specify whether the join columns should be part of the result.
  bool keepJoinColumns_ = true;

 public:
  // The outermost FILTERs of an OPTIONAL group that use variables from the
  // left side. By the SPARQL standard (section 18.2.2.6 and the definition of
  // `LeftJoin` in section 18.5) they are not a filter on the right side, but
  // the condition of the left join: a pair of a left and a right row is only
  // part of the result if all expressions are true for the merged row. A left
  // row for which no such pair exists is kept once, with UNDEF on the right.
  using JoinCondition = std::vector<sparqlExpression::SparqlExpressionPimpl>;

 private:
  JoinCondition condition_;

 public:
  OptionalJoin(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> t1,
               std::shared_ptr<QueryExecutionTree> t2,
               bool keepJoinColumns = true, JoinCondition condition = {});

 private:
  std::string getCacheKeyImpl() const override;

  void onLimitOffsetChanged(const LimitOffsetClause&,
                            bool childrenAreExclusivelyOwned) override;

 public:
  // We propagate part of the `LimitOffsetClause` to the child operation to
  // potentially speed it up and save memory, but `OptionalJoin` does not
  // actually apply its own `LimitOffsetClause` to itself, this still needs to
  // be done by the `Operation` base class.
  LimitOffsetHandling handlesLimitOffset() const override {
    return LimitOffsetHandling::PARTIAL;
  }

  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  qlm::vector<ColumnIndex> resultSortedOn() const override;

  bool knownEmptyResult() override { return _left->knownEmptyResult(); }

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

 private:
  qlm::vector<QueryExecutionTree*> getChildrenImpl() const override {
    return {{_left.get(), _right.get()}, allocator()};
  }

 public:
  bool columnOriginatesFromGraphOrUndef(
      const Variable& variable) const override;

  // Joins two result tables on any number of columns, inserting the special
  // value `Id::makeUndefined()` for any entries marked as optional.
  void optionalJoin(
      const IdTableView<0>& left, const IdTableView<0>& right,
      ql::span<const std::array<ColumnIndex, 2>> joinColumns,
      IdTable* dynResult,
      Implementation implementation = Implementation::GeneralCase);

  // Apply the `condition_` to the `result` of the join without condition.
  // `result` still has the internal column order of `AddCombinedRowToIdTable`
  // (join columns first). `leftRowOfResultRow[i]` is the index of the row in
  // `leftPermuted` from which row `i` of `result` was created, see
  // `AddCombinedRowToIdTable::trackLeftRowIndices`. Return true iff the
  // result is no longer sorted by the join columns afterwards.
  bool applyCondition(IdTable& result,
                      const std::vector<uint64_t>& leftRowOfResultRow,
                      const IdTableView<0>& leftPermuted) const;

  // Like `applyCondition`, but sort the result by the join columns if the
  // order was changed.
  void applyConditionAndRestoreOrder(
      IdTable& result, const std::vector<uint64_t>& leftRowOfResultRow,
      const IdTableView<0>& leftPermuted) const;

  // The variable to column map for the internal column order of the result
  // during the join, which is used to evaluate the condition.
  VariableToColumnMap getInternalVariableColumns() const;

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
  [[nodiscard]] bool isDeterministicImpl() const override {
    return ql::ranges::all_of(condition_, [](const auto& expression) {
      return expression.isDeterministic();
    });
  }

  std::unique_ptr<Operation> cloneImpl() const override;

  // Helper function for `tryIndexNestedLoopJoinIfSuitable` which makes the
  // logic reusable. Note that this depends on the size estimates and hence may
  // change when a `LIMIT` is pushed into `_left`, see `onLimitOffsetChanged`.
  bool isIndexNestedLoopJoinSuitable() const;

  // The join columns of the left input, in the order in which `_left` is
  // sorted.
  std::vector<ColumnIndex> leftJoinColumns() const;

  // Nested loop join optimization than can apply when a memory intensive sort
  // can be avoided this way.
  std::optional<Result> tryIndexNestedLoopJoinIfSuitable(bool requestLaziness);

  std::optional<std::shared_ptr<QueryExecutionTree>>
  makeTreeWithStrippedColumns(
      const std::set<Variable>& variables) const override;

  std::optional<std::shared_ptr<QueryExecutionTree>> makeTreeWithBindColumn(
      const parsedQuery::Bind& bind) const override;
  std::unique_ptr<Operation> cloneWithNewChildren(
      qlm::vector<std::shared_ptr<QueryExecutionTree>> children) const override;

  void computeSizeEstimateAndMultiplicities();

  Result computeResult(bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  // Check which of the join columns in `left` and `right` contain UNDEF values
  // and return the appropriate `Implementation`.
  static Implementation computeImplementationFromIdTables(
      const IdTableView<0>& left, const IdTableView<0>& right,
      ql::span<const std::array<ColumnIndex, 2>>);
};

#endif  // QLEVER_SRC_ENGINE_OPTIONALJOIN_H
