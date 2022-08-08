//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#pragma once

#include <string>

#include "../engine/sparqlExpressions/SparqlExpressionPimpl.h"

using std::string;

struct Alias {
  sparqlExpression::SparqlExpressionPimpl _expression;
  string _outVarName;
  [[nodiscard]] std::string getDescriptor() const {
    return "(" + _expression.getDescriptor() + " as " + _outVarName + ")";
  }
  bool operator==(const Alias& other) const {
    return _expression.getDescriptor() == other._expression.getDescriptor() &&
           _outVarName == other._outVarName;
  }

  [[nodiscard]] const std::string& targetVariable() const {
    return _outVarName;
  }
};
