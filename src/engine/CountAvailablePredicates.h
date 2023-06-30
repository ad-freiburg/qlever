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
   * predicates and their counts for all entities.
   */
  explicit CountAvailablePredicates(QueryExecutionContext* qec,
                                    Variable predicateVariable,
                                    Variable countVariable);

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
  [[nodiscard]] string asStringImpl(size_t indent) const override;

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

  // This method is declared here solely for unit testing purposes
  /**
   * @brief Computes all relations that have one of input[inputCol]'s entities
   *        as a subject and counts the number of their occurrences.
   * @param input The input table of entity ids
   * @param result A table with two columns, one for predicate ids,
   *               one for counts
   * @param hasPattern A mapping from entity ids to pattern ids (or NO_PATTERN)
   * @param hasPredicate A mapping from entity ids to sets of relations
   * @param patterns A mapping from pattern ids to patterns
   * @param subjectColumn The column containing the entities for which the
   *                      relations should be counted.
   */
  template <size_t I>
  static void computePatternTrick(
      const IdTable& input, IdTable* result,
      const vector<PatternID>& hasPattern,
      const CompactVectorOfStrings<Id>& hasPredicate,
      const CompactVectorOfStrings<Id>& patterns, size_t subjectColumn,
      RuntimeInformation* runtimeInfo);

  static void computePatternTrickAllEntities(
      IdTable* result, const vector<PatternID>& hasPattern,
      const CompactVectorOfStrings<Id>& hasPredicate,
      const CompactVectorOfStrings<Id>& patterns);

 private:
  ResultTable computeResult() override;
  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;
};
