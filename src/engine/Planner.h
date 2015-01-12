// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include "IndexMock.h"
#include "QueryExecutionTree.h"
#include "../parser/ParsedQuery.h"

// Planner for query execution.
// Which operations are done in which order.
class Planner {

public:
  Planner(const IndexMock& index): _index(index) {}

  QueryExecutionTree createQueryExecutionTree(const ParsedQuery& query);

private:
  IndexMock _index;
};


