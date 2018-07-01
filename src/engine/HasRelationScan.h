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

 private:
  ScanType _type;
  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _subtreeColIndex;

  std::string _subject;
  std::string _object;

  virtual void computeResult(ResultTable* result) const;
  void computeFreeS(ResultTable* result) const;
  void computeFreeO(ResultTable* result) const;
  void computeFullScan(ResultTable* result) const;
  void computeSubqueryS(ResultTable* result) const;
};
