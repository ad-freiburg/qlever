// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_JOIN_H
#define QLEVER_SRC_ENGINE_JOIN_H

#include "backports/concepts.h"
#include "engine/AddCombinedRowToTable.h"
#include "engine/IndexScan.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"
#include "util/TypeTraits.h"

class Join : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  ColumnIndex _leftJoinCol;
  ColumnIndex _rightJoinCol;

  Variable _joinVar{"?notSet"};

  bool _sizeEstimateComputed;
  size_t _sizeEstimate;

  std::vector<float> _multiplicities;

  // If set to false, the join column will not be part of the result.
  bool keepJoinColumn_ = true;

 public:
  // `allowSwappingChildrenOnlyForTesting` should only ever be changed by tests.
  Join(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
       std::shared_ptr<QueryExecutionTree> t2, ColumnIndex t1JoinCol,
       ColumnIndex t2JoinCol, bool keepJoinColumn = true,
       bool allowSwappingChildrenOnlyForTesting = true);

  using OptionalPermutation = std::optional<std::vector<ColumnIndex>>;

  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  std::vector<ColumnIndex> resultSortedOn() const override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override {
    if (!_sizeEstimateComputed) {
      computeSizeEstimateAndMultiplicities();
      _sizeEstimateComputed = true;
    }
    return _sizeEstimate;
  }

 public:
  size_t getCostEstimate() override;

  bool knownEmptyResult() override {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  void computeSizeEstimateAndMultiplicities();

  float getMultiplicity(size_t col) override;

  std::vector<QueryExecutionTree*> getChildren() override {
    return {_left.get(), _right.get()};
  }

  bool columnOriginatesFromGraphOrUndef(
      const Variable& variable) const override;

  /**
   * @brief Joins IdTables a and b on join column jc2, returning
   * the result in dynRes. Creates a cross product for matching rows.
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
  void join(const IdTable& a, const IdTable& b, IdTable* result) const;

 public:
  // Helper function to compute the result of a join operation and conditionally
  // return a lazy or fully materialized result depending on `requestLaziness`.
  // This is achieved by running the `action` lambda in a separate thread and
  // returning a lazy result that reads from the queue of the thread. If
  // `requestLaziness` is false, the result is fully materialized and returned
  // directly.
  // `permutation` indicates a permutation to apply to the result columns before
  // yielding/returning them. An empty vector means no permutation is applied.
  // `action` is a lambda that can be used to send partial chunks to a consumer
  // in addition to returning the remaining result. If laziness is not required
  // it is a no-op.
  CPP_template(typename ActionT)(
      requires ad_utility::InvocableWithExactReturnType<
          ActionT, Result::IdTableVocabPair,
          std::function<void(IdTable&, LocalVocab&)>>) Result
      createResult(bool requestedLaziness, ActionT action,
                   OptionalPermutation permutation = {}) const;

  // Fallback implementation of a join that is used when at least one of the two
  // inputs is not fully materialized. This represents the general case where we
  // don't have any optimization left to try.
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

 protected:
  virtual std::string getCacheKeyImpl() const override;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  Result computeResult(bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  std::optional<std::shared_ptr<QueryExecutionTree>>
  makeTreeWithStrippedColumns(
      const std::set<Variable>& variables) const override;

  // A special implementation that is called when both children are
  // `IndexScan`s. Uses the lazy scans to only retrieve the subset of the
  // `IndexScan`s that is actually needed without fully materializing them.
  Result computeResultForTwoIndexScans(bool requestLaziness) const;

  // A special implementation that is called when exactly one of the children is
  // an `IndexScan` and the other one is a fully materialized result. The
  // argument `idTableIsRightInput` determines whether the `IndexScan` is the
  // left or the right child of this `Join`. This needs to be known to determine
  // the correct order of the columns in the result.
  template <bool idTableIsRightInput>
  Result computeResultForIndexScanAndIdTable(
      bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
      std::shared_ptr<IndexScan> scan) const;

  // Special implementation that is called when the right child is an
  // `IndexScan` and the left child is a lazy result. (The constructor will
  // ensure the correct order if they are initially swapped). This allows the
  // `IndexScan` to skip rows that won't match in the join operation.
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
};

#endif  // QLEVER_SRC_ENGINE_JOIN_H
