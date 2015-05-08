// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <utility>
#include <vector>
#include <unordered_map>
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;
using std::unordered_map;
using std::pair;
using std::vector;

class TextOperationForEntities : public Operation {
public:

  TextOperationForEntities(QueryExecutionContext* qec, const string& words,
                const vector<pair<QueryExecutionTree, size_t>>& subtrees);

  virtual string asString() const;
  virtual size_t getResultWidth() const;
  virtual size_t resultSortedOn() const {
    return std::numeric_limits<size_t>::max();
  }

private:
  string _words;
  vector<pair<QueryExecutionTree, size_t>> _subtrees;

  virtual void computeResult(ResultTable* result) const;
};
