// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <utility>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include "./Operation.h"
#include "./QueryExecutionTree.h"
#include "../parser/ParsedQuery.h"

using std::string;
using std::vector;

class GroupBy : public Operation {
  public:
  enum class AggregateType {
    COUNT,
    GROUP_CONCAT,
    FIRST,
    LAST,
    SAMPLE,
    MIN,
    MAX,
    SUM,
    AVG
  };

  struct Aggregate {
    AggregateType _type;
    size_t _inCol, _outCol;
    // Used to store the string necessary for the group concat aggregate.
    void* _userData;
  };

  GroupBy(QueryExecutionContext *qec,
          std::shared_ptr<QueryExecutionTree> subtree,
          const vector<string>& groupByVariables,
          const std::unordered_map<std::string, ParsedQuery::Alias>& aliases);

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

  /**
   * @return The columns on which the input data should be sorted.
   */
  static vector<pair<size_t, bool>> computeSortColumns(
      std::shared_ptr<QueryExecutionTree> subtree,
      const vector<string>& groupByVariables,
      const std::unordered_map<std::string, ParsedQuery::Alias>& aliases);

private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  vector<string> _groupByVariables;
  std::vector<ParsedQuery::Alias> _aliases;
  std::unordered_map<string, size_t> _varColMap;

  virtual void computeResult(ResultTable* result) const;
};
