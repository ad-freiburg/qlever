// Copyright 2015 - 2026 The QLever Authors, in particular:

// 2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// 2018-2026 Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de), UFR
// 2026 Mark Veser (mark.veser87@gmail.com)

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_JOINIMPL_H
#define QLEVER_SRC_ENGINE_JOINIMPL_H

#include "backports/concepts.h"
#include "engine/AddCombinedRowToTable.h"
#include "engine/IndexScan.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "util/ContainersWithAllocator.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"
#include "util/TypeTraits.h"

class JoinImpl : public Operation {
 public:
  using JoinColumns = std::vector<std::array<ColumnIndex, 2>>;

  // The maximal number of join columns for which the specialized (lazy and
  // prefiltered) join algorithms are instantiated. Joins with more join
  // columns always fully materialize their inputs.
  static constexpr size_t MAX_NUM_JOIN_COLS_FOR_LAZY_JOIN = 3;

 private:
  std::shared_ptr<QueryExecutionTree> left_;
  std::shared_ptr<QueryExecutionTree> right_;

  // The pairs `(leftColumn, rightColumn)` of join columns. Both children are
  // sorted by their join columns in this order.
  JoinColumns joinColumns_;

  // The join variables, in the same order as `joinColumns_`.
  std::vector<Variable> joinVars_;

  bool sizeEstimateComputed_;
  size_t sizeEstimate_;

  std::vector<float> multiplicities_;

  // If set to false, the join column will not be part of the result. Can
  // only be set to false if there is exactly one join column.
  bool keepJoinColumn_ = true;

 public:
  // Join `t1` and `t2` on the given `joinColumns`, which are pairs of
  // `(columnOfT1, columnOfT2)`. If there are several join columns, their order
  // might be changed s.t. one of the children doesn't have to be sorted, see
  // `chooseJoinColumnOrder` below. `keepJoinColumn == false` is only supported
  // for a single join column. `allowSwappingChildrenOnlyForTesting` should only
  // ever be changed by tests.
  JoinImpl(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
           std::shared_ptr<QueryExecutionTree> t2, JoinColumns joinColumns,
           bool keepJoinColumn = true,
           bool allowSwappingChildrenOnlyForTesting = true);

  using OptionalPermutation = std::optional<std::vector<ColumnIndex>>;

  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  std::vector<ColumnIndex> resultSortedOn() const override;

  uint64_t getSizeEstimateBeforeLimit() override {
    if (!sizeEstimateComputed_) {
      computeSizeEstimateAndMultiplicities();
      sizeEstimateComputed_ = true;
    }
    return sizeEstimate_;
  }

  // Return the number of join columns.
  size_t getNumJoinColumns() const { return joinColumns_.size(); }

  // Return the join columns as pairs `(leftColumn, rightColumn)`.
  const JoinColumns& getJoinColumns() const { return joinColumns_; }

  // Reorder the `joinColumns` (pairs of `(colOfT1, colOfT2)`) s.t. one of the
  // children is already sorted by its join columns, if possible. Children that
  // are `IndexScan`s that support the prefiltered join are preferred.
  // Otherwise, the join columns are sorted by the column index of the child
  // with the smaller cache key. The result doesn't depend on the order of `t1`
  // and `t2` (apart from the order within the pairs).
  static JoinColumns chooseJoinColumnOrder(const QueryExecutionTree& t1,
                                           const QueryExecutionTree& t2,
                                           JoinColumns joinColumns);

  size_t getCostEstimate() override;

  bool knownEmptyResult() override {
    return left_->knownEmptyResult() || right_->knownEmptyResult();
  }

  void computeSizeEstimateAndMultiplicities();

  float getMultiplicity(size_t col) override;

 private:
  qlm::vector<QueryExecutionTree*> getChildrenImpl() const override {
    return {{left_.get(), right_.get()}, allocator()};
  }

 public:
  bool columnOriginatesFromGraphOrUndef(
      const Variable& variable) const override;

  /**
   * @brief Join the fully materialized inputs `a` and `b` (the results of the
   * left and right child) on the `joinColumns_` and store the result in
   * `result`. Creates a cross product for matching rows.
   *
   * This should be a switch, which should decide which algorithm to use for
   * joining two IdTables.
   * The possible algorithms should be:
   * - The normal merge join.
   * - The doGallopInnerJoin.
   * - The hashJoinImpl.
   * Currently it only decides between doGallopInnerJoin and the standard merge
   * join, with the merge join code directly written in the function.
   * TODO Move the merge join into it's own function and make this function
   * a proper switch.
   **/
  void join(const IdTableView<0>& a, const IdTableView<0>& b,
            IdTable* result) const;

  // Implementation of `join` (see above) for more than one join column. If
  // the join columns contain no UNDEF values, a galloping join or a simple
  // zipper join is used. Otherwise, the more expensive variant of
  // `zipperJoinWithUndef` that finds compatible rows with UNDEF values is used,
  // and the result is sorted afterwards if required.
  void multiColumnJoin(const IdTableView<0>& a, const IdTableView<0>& b,
                       IdTable* result) const;

 public:
  // Fallback implementation of a join that is used when at least one of the two
  // inputs is not fully materialized. This represents the general case where we
  // don't have any optimization left to try. Requires that
  // `lazyJoinAlgorithmsAreApplicable()` is true.
  Result lazyJoin(std::shared_ptr<const Result> a,
                  std::shared_ptr<const Result> b, bool requestLaziness) const;

  /**
   * @brief Joins IdTables dynA and dynB on join column jc2, returning
   * the result in dynRes. Creates a cross product for matching rows by putting
   * the smaller IdTable in a hash map and using that, to faster find the
   * matching rows.
   * Needed to be a separate function from the actual implementation, because
   * compiler optimization kept inlining it, which make testing impossible,
   * because you couldn't call the function after linking and just got
   * 'undefined reference' errors.
   *
   * @return The result is only sorted, if the bigger table is sorted.
   * Otherwise it is not sorted.
   **/
  static void hashJoin(const IdTable& dynA, ColumnIndex jc1,
                       const IdTable& dynB, ColumnIndex jc2, IdTable* dynRes);

  std::string getCacheKeyImpl() const override;
  std::unique_ptr<Operation> cloneImpl() const override;
  Result computeResult(bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;

  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  std::optional<std::shared_ptr<QueryExecutionTree>>
  makeTreeWithStrippedColumns(
      const std::set<Variable>& variables) const override;

  std::optional<std::shared_ptr<QueryExecutionTree>> makeTreeWithBindColumn(
      const parsedQuery::Bind& bind) const override;
  std::unique_ptr<Operation> cloneWithNewChildren(
      std::vector<std::shared_ptr<QueryExecutionTree>> children) const override;

 private:
  // Return true iff the lazy and prefiltered join algorithms can be used. This
  // is always the case for a single join column. For more join columns, it
  // requires that there are at most `MAX_NUM_JOIN_COLS_FOR_LAZY_JOIN` join
  // columns, none of which can contain UNDEF values, as those algorithms can
  // only handle UNDEF values in the first join column, and only if it is the
  // only join column.
  bool lazyJoinAlgorithmsAreApplicable() const;

  // Call `f` with a `std::integral_constant<size_t, numJoinColumns>`. Requires
  // that `lazyJoinAlgorithmsAreApplicable()` is true.
  template <typename F>
  decltype(auto) callWithNumJoinColumns(F&& f) const;

  // Compute the result by fully materializing both inputs. This is used when
  // `lazyJoinAlgorithmsAreApplicable()` is false.
  Result computeResultFullyMaterialized();

  // A special implementation that is called when both children are
  // `IndexScan`s. Uses the lazy scans to only retrieve the subset of the
  // `IndexScan`s that is actually needed without fully materializing them.
  template <size_t NumJoinCols>
  Result computeResultForTwoIndexScans(bool requestLaziness) const;

  // A special implementation that is called when exactly one of the children is
  // an `IndexScan` and the other one is a fully materialized result. The
  // argument `idTableIsRightInput` determines whether the `IndexScan` is the
  // left or the right child of this `Join`. This needs to be known to determine
  // the correct order of the columns in the result.
  template <size_t NumJoinCols, bool idTableIsRightInput>
  Result computeResultForIndexScanAndIdTable(
      bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
      std::shared_ptr<IndexScan> scan) const;

  // Special implementation that is called when the right child is an
  // `IndexScan` and the left child is a lazy result. (The constructor will
  // ensure the correct order if they are initially swapped). This allows the
  // `IndexScan` to skip rows that won't match in the join operation.
  template <size_t NumJoinCols>
  Result computeResultForIndexScanAndLazyOperation(
      bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
      std::shared_ptr<IndexScan> scan) const;

  // Default case where both inputs are fully materialized.
  Result computeResultForTwoMaterializedInputs(
      std::shared_ptr<const Result> leftRes,
      std::shared_ptr<const Result> rightRes) const;

  /*
   * @brief Combines 2 rows like in a join and inserts the result in the
   * given table.
   *
   *@tparam The amount of columns in the rows and the table.
   *
   * @param[in] The left row of the join.
   * @param[in] The right row of the join.
   * @param[in] The numerical position of the join column of row b.
   * @param[in] The table, in which the new combined row should be inserted.
   * Must be static.
   */
  template <typename ROW_A, typename ROW_B, int TABLE_WIDTH>
  static void addCombinedRowToIdTable(const ROW_A& rowA, const ROW_B& rowB,
                                      ColumnIndex jcRowB,
                                      IdTableStatic<TABLE_WIDTH>* table);

  /*
   * @brief The implementation of hashJoin.
   */
  template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  static void hashJoinImpl(const IdTable& dynA, ColumnIndex jc1,
                           const IdTable& dynB, ColumnIndex jc2,
                           IdTable* dynRes);

  // Commonly used code for the various known-to-be-empty cases.
  Result createEmptyResult() const;

  // Get permutation of input and output columns to apply before and after
  // joining. This is required because the join algorithms expect the join
  // columns to be the first columns of the input tables and the result to be in
  // the order of the input tables.
  ad_utility::JoinColumnMapping getJoinColumnMapping() const;

  // Helper function to create the commonly used instance of this class.
  ad_utility::AddCombinedRowToIdTable makeRowAdder(
      std::function<void(IdTable&, LocalVocab&)> callback) const;

  // Return true iff the join columns of the `side`-th child (0 for left, 1 for
  // right) are exactly the columns `0, ..., numJoinColumns - 1` in this order,
  // which is the case for all `IndexScan`s that are not sorted.
  bool joinColumnsArePrefix(size_t side) const;

  // Return a view of the join columns of the `side`-th child (0 for left, 1
  // for right).
  auto joinColumnsOfSide(size_t side) const {
    return joinColumns_ |
           ql::views::transform([side](const auto& jc) { return jc.at(side); });
  }

  // Compute the size estimate and multiplicities for more than one join
  // column.
  void computeSizeEstimateAndMultiplicitiesMultiColumn();

  // Compute the join columns (as pairs `(leftCol, rightCol)`) of `left` and
  // `right` for the given `joinVars`.
  static JoinColumns computeJoinColumns(const QueryExecutionTree& left,
                                        const QueryExecutionTree& right,
                                        const std::vector<Variable>& joinVars);
};

#endif  // QLEVER_SRC_ENGINE_JOINIMPL_H
