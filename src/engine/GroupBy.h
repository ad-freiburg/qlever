// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../parser/ParsedQuery.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::string;
using std::vector;

class GroupBy : public Operation {
 public:
  /**
   * @brief All supported types of aggregate aliases
   */
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

  /**
   * @brief Represents an aggregate alias in the select part of the query.
   */
  struct Aggregate {
    AggregateType _type;
    size_t _inCol, _outCol;
    // Used to store the string necessary for the group concat aggregate.
    void* _userData;
    bool _distinct;
  };

  GroupBy(QueryExecutionContext* qec,
          std::shared_ptr<QueryExecutionTree> subtree,
          const vector<string>& groupByVariables,
          const std::vector<ParsedQuery::Alias>& aliases);

  virtual string asString(size_t indent = 0) const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const;

  std::unordered_map<string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit) { _subtree->setTextLimit(limit); }

  virtual bool knownEmptyResult() { return _subtree->knownEmptyResult(); }

  virtual float getMultiplicity(size_t col);

  virtual size_t getSizeEstimate();

  virtual size_t getCostEstimate();

  /**
   * @return The columns on which the input data should be sorted or an empty
   *         list if no particular order is required for the grouping.
   *         The columns need to be known before the GroupBy Operation can be
   *         created, as the groupBy requires its parent operation on creation.
   */
  static vector<pair<size_t, bool>> computeSortColumns(
      std::shared_ptr<QueryExecutionTree> subtree,
      const vector<string>& groupByVariables,
      const std::vector<ParsedQuery::Alias>& aliases);

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  vector<string> _groupByVariables;
  std::vector<ParsedQuery::Alias> _aliases;
  std::unordered_map<string, size_t> _varColMap;

  virtual void computeResult(ResultTable* result) const;
};

// This method is declared here solely for unit testing purposes
template <typename A, typename R>
void doGroupBy(const vector<A>* input,
               const vector<ResultTable::ResultType>& inputTypes,
               const vector<size_t>& groupByCols,
               const vector<GroupBy::Aggregate>& aggregates, vector<R>* result,
               const ResultTable* inTable, ResultTable* outTable,
               const Index& index);
