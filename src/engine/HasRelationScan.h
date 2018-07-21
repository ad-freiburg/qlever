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

class HasRelationScan : public Operation {
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

  HasRelationScan(QueryExecutionContext* qec, ScanType type);

  virtual string asString(size_t indent = 0) const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const;

  std::unordered_map<string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit);

  virtual bool knownEmptyResult();

  virtual float getMultiplicity(size_t col);

  virtual size_t getSizeEstimate();

  virtual size_t getCostEstimate();

  void setSubject(const std::string& subject);
  void setObject(const std::string& object);
  void setSubtree(std::shared_ptr<QueryExecutionTree> subtree);
  void setSubtreeSubjectColumn(size_t colIndex);
  ScanType getType() const;

  const std::string& getObject() const;

  // These are made static and public mainly for easier testing
  static void computeFreeS(ResultTable* result, size_t objectId,
                           const std::vector<PatternID>& hasPattern,
                           const CompactStringVector<Id, Id>& hasRelation,
                           const CompactStringVector<size_t, Id>& patterns);

  static void computeFreeO(ResultTable* result, size_t subjectId,
                           const std::vector<PatternID>& hasPattern,
                           const CompactStringVector<Id, Id>& hasRelation,
                           const CompactStringVector<size_t, Id>& patterns);

  static void computeFullScan(ResultTable* result,
                              const std::vector<PatternID>& hasPattern,
                              const CompactStringVector<Id, Id>& hasRelation,
                              const CompactStringVector<size_t, Id>& patterns);

  static void computeSubqueryS(
      ResultTable* result, const std::shared_ptr<QueryExecutionTree> _subtree,
      const size_t subtreeColIndex, const std::vector<PatternID>& hasPattern,
      const CompactStringVector<Id, Id>& hasRelation,
      const CompactStringVector<size_t, Id>& patterns);

 private:
  ScanType _type;
  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _subtreeColIndex;

  std::string _subject;
  std::string _object;

  virtual void computeResult(ResultTable* result) const;
};
