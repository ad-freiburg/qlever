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

class HasPredicateScan : public Operation {
 public:
  enum class ScanType {
    // Given a constant predicate, return all subjects
    FREE_S,
    // Given a constant subject, return all predicates
    FREE_O,
    // For all subjects return their predicates
    FULL_SCAN,
    // For a given subset of subjects return their predicates
    SUBQUERY_S
  };

  HasPredicateScan(QueryExecutionContext* qec, ScanType type);

 private:
  virtual string asStringImpl(size_t indent = 0) const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<string, size_t> getVariableColumns() const override;

  virtual void setTextLimit(size_t limit) override;

  virtual bool knownEmptyResult() override;

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

  void setSubject(const std::string& subject);
  void setObject(const std::string& object);
  void setSubtree(std::shared_ptr<QueryExecutionTree> subtree);
  void setSubtreeSubjectColumn(size_t colIndex);
  ScanType getType() const;

  const std::string& getObject() const;

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

  // These are made static and public mainly for easier testing
  static void computeFreeS(ResultTable* result, size_t objectId,
                           const std::vector<PatternID>& hasPattern,
                           const CompactVectorOfStrings<Id>& hasPredicate,
                           const CompactVectorOfStrings<Id>& patterns);

  static void computeFreeO(ResultTable* result, size_t subjectId,
                           const std::vector<PatternID>& hasPattern,
                           const CompactVectorOfStrings<Id>& hasPredicate,
                           const CompactVectorOfStrings<Id>& patterns);

  static void computeFullScan(ResultTable* result,
                              const std::vector<PatternID>& hasPattern,
                              const CompactVectorOfStrings<Id>& hasPredicate,
                              const CompactVectorOfStrings<Id>& patterns,
                              size_t resultSize);

  template <int IN_WIDTH, int OUT_WIDTH>
  static void computeSubqueryS(IdTable* result, const IdTable& _subtree,
                               size_t subtreeColIndex,
                               const std::vector<PatternID>& hasPattern,
                               const CompactVectorOfStrings<Id>& hasPredicate,
                               const CompactVectorOfStrings<Id>& patterns);

 private:
  ScanType _type;
  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _subtreeColIndex;

  std::string _subject;
  std::string _object;

  void computeResult(ResultTable* result) override;
};
