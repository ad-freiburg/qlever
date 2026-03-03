//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_ALIAS_H
#define QLEVER_SRC_PARSER_ALIAS_H

#include <string>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "rdfTypes/Variable.h"

struct Alias {
  sparqlExpression::SparqlExpressionPimpl _expression;
  Variable _target;
  [[nodiscard]] std::string getDescriptor() const {
    return "(" + _expression.getDescriptor() + " as " + _target.name() + ")";
  }
  bool operator==(const Alias& other) const {
    return _expression.getDescriptor() == other._expression.getDescriptor() &&
           _target == other._target;
  }
};

#endif  // QLEVER_SRC_PARSER_ALIAS_H
