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

/**
 * @brief This operation takes a list of entities as an input and then, for
 * every entity in that list determines for how many predicates that entity is a
 * subject (or object). The returned table has two columns, one for entity ids,
 * and one for counts.
 */
class EntityCountPredicates : public Operation {
 public:
  enum class CountType { SUBJECT, OBJECT };

  /**
   * @brief Creates a new EntityCountPredicates operation that returns
   * predicate counts for all entities.
   */
  EntityCountPredicates(QueryExecutionContext* qec);

  /**
   * @brief Creates a new EntityCountPredicates operation that returns
   * predicate counts for the entities in column subjectColumnIndex
   * of the result of subtree.
   */
  EntityCountPredicates(QueryExecutionContext* qec,
                        std::shared_ptr<QueryExecutionTree> subtree,
                        size_t subjectColumnIndex);

  /**
   * @brief Creates a new EntityCountPredicates operation that returns
   * predicate counts for the entity given by the entityName.
   */
  EntityCountPredicates(QueryExecutionContext* qec, std::string entityName);

  virtual string asString(size_t indent = 0) const override;

  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  vector<QueryExecutionTree*> getChildren() override {
    using R = vector<QueryExecutionTree*>;
    return _subtree != nullptr ? R{_subtree.get()} : R{};
  }

  ad_utility::HashMap<string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit) override {
    if (_subtree != nullptr) {
      _subtree->setTextLimit(limit);
    }
  }

  virtual bool knownEmptyResult() override {
    if (_subtree != nullptr) {
      return _subtree->knownEmptyResult();
    }
    return false;
  }

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

  void setVarNames(const std::string& predicateVarName,
                   const std::string& countVarName);

  /**
   * @brief This operation can count predicates connected to subjects or
   * objects. This method switches between the two modes.
   */
  void setCountFor(CountType count_for);

  // This method is declared here solely for unit testing purposes
  template <int I, typename PredicateId>
  static void compute(const IdTable& input, IdTable* result,
                      const vector<PatternID>& hasPattern,
                      const CompactStringVector<Id, PredicateId>& hasPredicate,
                      const CompactStringVector<size_t, PredicateId>& patterns,
                      const size_t subjectColumn);

  template <typename PredicateId>
  static void computeAllEntities(
      IdTable* result, const vector<PatternID>& hasPattern,
      const CompactStringVector<Id, PredicateId>& hasPredicate,
      const CompactStringVector<size_t, PredicateId>& patterns);

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _subjectColumnIndex;
  // This can be used to aquire the predicates for a single entity
  std::optional<std::string> _subjectEntityName;
  std::string _predicateVarName;
  std::string _countVarName;

  CountType _count_for;

  virtual void computeResult(ResultTable* result) override;

  template <typename PredicateId>
  void computeResult(
      ResultTable* result,
      std::shared_ptr<const PatternContainerImpl<PredicateId>> pattern_data);
};
