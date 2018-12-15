// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../parser/ParsedQuery.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

class FullRelationScan : public Operation {
 public:
  enum class ScanType {
    // Only subject scans are curently supported
    SUBJECT,
    PREDICATE,
    OBJECT
  };

  FullRelationScan(QueryExecutionContext* qec, ScanType type,
                   const std::string& entity_var_name,
                   const std::string& count_var_name);

  virtual string asString(size_t indent = 0) const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit) override;

  virtual bool knownEmptyResult() override;

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

  ScanType getType() const;

  static void computeFullScan(ResultTable* result, const Index& index,
                              ScanType type);

  static void computeSubqueryS(
      ResultTable* result, const std::shared_ptr<QueryExecutionTree> _subtree,
      const size_t subtreeColIndex, const std::vector<PatternID>& hasPattern,
      const CompactStringVector<Id, Id>& hasPredicate,
      const CompactStringVector<size_t, Id>& patterns);

 private:
  ScanType _type;
  std::string _entity_var_name;
  std::string _count_var_name;

  virtual void computeResult(ResultTable* result) const override;
};
