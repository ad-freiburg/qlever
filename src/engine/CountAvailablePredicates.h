// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "global/Pattern.h"

// This Operation takes a Result with at least one column containing ids,
// and a column index referring to such a column. It then creates a Result
// containing two columns, the first one filled with the ids of all predicates
// for which there is an entry in the index with one of the entities in the
// specified input column as its subject. The second output column contains a
// count of how many of the input entities fulfill that requirement for that
// predicate. This operation requires the use of the usePatterns option both
// when building and when loading the index.
class CountAvailablePredicates : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> subtree_;
  size_t subjectColumnIndex_;
  Variable predicateVariable_;
  Variable countVariable_;

 public:
  /**
   * @brief Creates a new CountAvailablePredicates operation that returns
   * predicates and their counts for the entities in column subjectColumnIndex
   * of the result of subtree.
   */
  CountAvailablePredicates(QueryExecutionContext* qec,
                           std::shared_ptr<QueryExecutionTree> subtree,
                           size_t subjectColumnIndex,
                           Variable predicateVariable, Variable countVariable);

 protected:
  [[nodiscard]] std::string getCacheKeyImpl() const override;

 public:
  [[nodiscard]] std::string getDescriptor() const override;

  [[nodiscard]] size_t getResultWidth() const override;

  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override;

  std::vector<QueryExecutionTree*> getChildren() override {
    using R = vector<QueryExecutionTree*>;
    return subtree_ != nullptr ? R{subtree_.get()} : R{};
  }

  bool knownEmptyResult() override {
    if (subtree_ != nullptr) {
      return subtree_->knownEmptyResult();
    }
    return false;
  }

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

  std::unique_ptr<Operation> cloneImpl() const override;

 public:
  size_t getCostEstimate() override;

  // Getters for testing.
  size_t subjectColumnIndex() const { return subjectColumnIndex_; }
  const Variable& predicateVariable() const { return predicateVariable_; }
  const Variable& countVariable() const { return countVariable_; }

 private:
  /**
   * @brief Computes all relations that have one of input[inputCol]'s entities
   *        as a subject and counts the number of their occurrences.
   * @param input The input table of entity ids
   * @param result A table with two columns, one for predicate ids,
   *               one for counts
   * @param patterns A mapping from pattern ids to patterns
   * @param subjectColumnIdx The column containing the entities for which the
   *                      relations should be counted.
   * @param patternColumnIdx The column containing the pattern IDs (previously
   * obtained via a scan of the `ql:has-pattern` predicate.
   */
  template <size_t I>
  static void computePatternTrick(const IdTable& input, IdTable* result,
                                  const CompactVectorOfStrings<Id>& patterns,
                                  size_t subjectColumnIdx,
                                  size_t patternColumnIdx,
                                  RuntimeInformation& runtimeInfo);

  // Special implementation for the full pattern trick.
  // Perform a lazy scan over the full `ql:has-pattern` relation,
  // and then count and expand the patterns.
  void computePatternTrickAllEntities(
      IdTable* result, const CompactVectorOfStrings<Id>& patterns) const;

  Result computeResult([[maybe_unused]] bool requestLaziness) override;
  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;
};
