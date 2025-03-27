//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_SOLUTIONMODIFIERS_H
#define QLEVER_SRC_PARSER_DATA_SOLUTIONMODIFIERS_H

#include "GroupKey.h"
#include "LimitOffsetClause.h"
#include "OrderKey.h"
#include "SparqlFilter.h"
#include "Variable.h"

struct SolutionModifiers {
  vector<GroupKey> groupByVariables_;
  vector<SparqlFilter> havingClauses_;
  OrderClause orderBy_;
  LimitOffsetClause limitOffset_{};
};

#endif  // QLEVER_SRC_PARSER_DATA_SOLUTIONMODIFIERS_H
