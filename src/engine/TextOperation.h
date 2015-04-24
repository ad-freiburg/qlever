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

class TextOperation : public Operation {
public:
  virtual size_t getResultWidth() const;

public:

  TextOperation(QueryExecutionContext* qec, const string& words,
                const vector<QueryExecutionTree>& subtrees);

  virtual string asString() const;

  virtual size_t resultSortedOn() const {
    return 0;
  }

private:
  string _words;
  vector<QueryExecutionTree> _subtrees;

  virtual void computeResult(ResultTable* result) const;
};
