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

  TextOperationForEntities(
      QueryExecutionContext *qec, const string& words,
      const vector<pair<QueryExecutionTree, size_t>>& subtrees,
      size_t textLimit);

  virtual string asString() const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const {
    if (_subtrees.size() == 0) {
      return std::numeric_limits<size_t>::max();
    } else {
      return 0;
    };
  }

  virtual void setTextLimit(size_t limit) {
    _textLimit = limit;
    for (auto& st: _subtrees) {
      st.first.setTextLimit(limit);
    }
  }

private:
  string _words;
  vector<pair<QueryExecutionTree, size_t>> _subtrees;
  size_t _textLimit;

  virtual void computeResult(ResultTable *result) const;
};
