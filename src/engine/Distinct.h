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
#include "../parser/ParsedQuery.h"

using std::list;
using std::unordered_map;
using std::pair;
using std::vector;

class Distinct : public Operation {
public:
  virtual size_t getResultWidth() const;

public:

  Distinct(QueryExecutionContext* qec, const QueryExecutionTree& subtree,
      const vector<size_t>& keepIndices);

  Distinct(const Distinct& other);

  Distinct& operator=(const Distinct& other);

  virtual ~Distinct();

  virtual string asString() const;

  virtual size_t resultSortedOn() const {
    return _subtree->resultSortedOn();
  }

private:
  QueryExecutionTree* _subtree;
  vector<size_t> _keepIndices;

  virtual void computeResult(ResultTable* result) const;
};
