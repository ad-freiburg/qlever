// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../global/Pattern.h"
#include "../parser/ParsedQuery.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::string;
using std::vector;

// This Operation takes a ResultTable with at least one column containing ids,
// and a column index referring to such a column. It then creates a ResultTable
// containing two columns, the first one filled with the ids of all predicates
// for which there is an entry in the index with one of the entities in the
// specified input column as its subject. The second output column contains a
// count of how many of the input entities fulfill that requirement for that
// predicate. This operation requires the use of the usePatterns option both
// when building as well as when loading the index.
class CountAvailablePredicates : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _subjectColumnIndex;
  Variable _predicateVariable;
  Variable _countVariable;

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
  [[nodiscard]] string getCacheKeyImpl() const override;

 public:
  [[nodiscard]] string getDescriptor() const override;

  [[nodiscard]] size_t getResultWidth() const override;

  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;

  vector<QueryExecutionTree*> getChildren() override {
    using R = vector<QueryExecutionTree*>;
    return _subtree != nullptr ? R{_subtree.get()} : R{};
  }

  void setTextLimit(size_t limit) override {
    if (_subtree != nullptr) {
      _subtree->setTextLimit(limit);
    }
  }

  bool knownEmptyResult() override {
    if (_subtree != nullptr) {
      return _subtree->knownEmptyResult();
    }
    return false;
  }

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

  // Getters for testing.
  size_t subjectColumnIndex() const { return _subjectColumnIndex; }
  const Variable& predicateVariable() const { return _predicateVariable; }
  const Variable& countVariable() const { return _countVariable; }

 private:
  // This method is declared here solely for unit testing purposes
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


 private:
  ResultTable computeResult() override;
  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;
};
