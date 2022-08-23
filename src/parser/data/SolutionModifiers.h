//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include "GroupKey.h"
#include "LimitOffsetClause.h"
#include "OrderKey.h"
#include "SparqlFilter.h"
#include "Variable.h"

struct SolutionModifiers {
  vector<GroupKey> groupByVariables_;
  vector<SparqlFilter> havingClauses_;
  vector<OrderKey> orderBy_;
  LimitOffsetClause limitOffset_{};
};
