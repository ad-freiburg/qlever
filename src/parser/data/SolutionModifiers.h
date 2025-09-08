//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_SOLUTIONMODIFIERS_H
#define QLEVER_SRC_PARSER_DATA_SOLUTIONMODIFIERS_H

#include "parser/data/GroupKey.h"
#include "parser/data/LimitOffsetClause.h"
#include "parser/data/OrderKey.h"
#include "parser/data/SparqlFilter.h"
#include "rdfTypes/Variable.h"

struct SolutionModifiers {
  std::vector<GroupKey> groupByVariables_;
  std::vector<SparqlFilter> havingClauses_;
  OrderClause orderBy_;
  LimitOffsetClause limitOffset_{};
};

#endif  // QLEVER_SRC_PARSER_DATA_SOLUTIONMODIFIERS_H
