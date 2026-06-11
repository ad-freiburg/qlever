// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Mark Veser (veser@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_HASHJOIN_H
#define QLEVER_SRC_ENGINE_HASHJOIN_H

#include "backports/concepts.h"
#include "engine/AddCombinedRowToTable.h"
#include "engine/IndexScan.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"
#include "util/TypeTraits.h"

class HashJoin : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> left_;
  std::shared_ptr<QueryExecutionTree> right_;

  ColumnIndex leftJoinCol_;
  ColumnIndex rightJoinCol_;

  Variable joinVar_{"?notSet"};

  bool sizeEstimateComputed_;
  size_t sizeEstimate_;

  std::vector<float> multiplicities_;

  // If set to false, the join column will not be part of the result.
  bool keepJoinColumn_ = true;

  // Offset for cost estimation to ensure a greater value than the Join Class
  static constexpr size_t VETO_COST_OFFSET = 1;

 public:
  // `allowSwappingChildrenOnlyForTesting` should only ever be changed by tests.
  HashJoin(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
       std::shared_ptr<QueryExecutionTree> t2, ColumnIndex t1JoinCol,
       ColumnIndex t2JoinCol, bool keepJoinColumn = true,
       bool allowSwappingChildrenOnlyForTesting = true);

  using OptionalPermutation = std::optional<std::vector<ColumnIndex>>;

  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  std::vector<ColumnIndex> resultSortedOn() const override;

  size_t getCostEstimate() override;

  bool knownEmptyResult() override {
    return left_->knownEmptyResult() || right_->knownEmptyResult();
  }

  void computeSizeEstimateAndMultiplicities();

  float getMultiplicity(size_t col) override;

  std::vector<QueryExecutionTree*> getChildren() override {
    return {left_.get(), right_.get()};
  }

  bool columnOriginatesFromGraphOrUndef(
      const Variable& variable) const override;

  /** 
   * @brief Performs actual hashJoin operation.
   * leftIsSmaller is necessary to decide which side is used for building
   * the hash map. The left / right destinction is important for the correct
   * order of the columns in the result.
  **/
  Result hashJoin(std::shared_ptr<const Result> left,
                  std::shared_ptr<const Result> right,
                  bool leftIsSmaller);

 protected:
  virtual std::string getCacheKeyImpl() const override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override {
    if (!sizeEstimateComputed_) {
      computeSizeEstimateAndMultiplicities();
      sizeEstimateComputed_ = true;
    }
    return sizeEstimate_;
  }

  std::unique_ptr<Operation> cloneImpl() const override;

  Result computeResult(bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  std::optional<std::shared_ptr<QueryExecutionTree>>
  makeTreeWithStrippedColumns(
      const std::set<Variable>& variables) const override;

  std::optional<std::shared_ptr<QueryExecutionTree>> makeTreeWithBindColumn(
      const parsedQuery::Bind& bind) const override;

  /*
   * @brief Combines 2 rows like in a join and inserts the result in the
   * given table.
   *
   *@tparam The amount of columns in the rows and the table.
   *
   * @param[in] The left row of the join.
   * @param[in] The right row of the join.
   * @param[in] The numerical position of the join column of row a.
   * @param[in] The numerical position of the join column of row b.
   * @param[in] The table, in which the new combined row should be inserted.
   * Must be static.
   */
  template <typename ROW_A, typename ROW_B, int TABLE_WIDTH>
  static void addCombinedRowToIdTable(const ROW_A& rowA, const ROW_B& rowB,
                                      ColumnIndex jcRowA, ColumnIndex jcRowB,
                                      bool keepJoinColumn,
                                      IdTableStatic<TABLE_WIDTH>* table);

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

  // Value to return, if hash join is not applicable and merge join is to be
  // preferred by the Query Planner. This function returns a value that is
  // greater than the cost of the Join Class, but doesn't exceed the maximum
  // value of size_t, to avoid overflow issues.
  size_t vetoCostEstimate();

  // Uses hashMapFitsInMemory and biggerInputSortedOnJoinCol to check if hash
  // join is applicable for given inputs.
  bool hashJoinIsApplicable() const;

  // estimates if smaller side of join inputs still fits into memory when
  // building the hash map.
  bool hashMapFitsInMemory() const;

  // checks if the bigger input (probe side) is already sorted on the join
  // column.
  bool biggerInputSortedOnJoinCol() const;
  bool eitherSideIsIndexScan() const;
};

#endif  // QLEVER_SRC_ENGINE_HASHJOIN_H
