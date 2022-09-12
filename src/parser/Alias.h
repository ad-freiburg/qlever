//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#pragma once

#include <string>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/data/Variable.h"

using std::string;

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
