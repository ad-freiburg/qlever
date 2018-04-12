// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <utility>
#include <memory>
#include <string>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"
#include "../parser/ParsedQuery.h"
#include "../global/Pattern.h"

using std::string;
using std::vector;

class CountAvailablePredicates : public Operation {
  public:

  CountAvailablePredicates(QueryExecutionContext *qec,
                           std::shared_ptr<QueryExecutionTree> subtree,
                           size_t subjectColumnIndex);

  virtual string asString(size_t indent = 0) const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const;

  std::unordered_map<string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit) {
    _subtree->setTextLimit(limit);
  }

  virtual bool knownEmptyResult() {
    return _subtree->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col);

  virtual size_t getSizeEstimate();

  virtual size_t getCostEstimate();

  void setVarNames(const std::string& predicateVarName,
                   const std::string& countVarName);

private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _subjectColumnIndex;
  std::string _predicateVarName;
  std::string _countVarName;

  virtual void computeResult(ResultTable* result) const;
};
