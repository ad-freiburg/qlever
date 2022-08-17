//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <variant>

#include "../../engine/sparqlExpressions/SparqlExpressionPimpl.h"

using std::string;
using std::variant;

/// Store an expression that appeared in an ORDER BY clause.
class ExpressionOrderKey {
 public:
  bool isDescending_;
  sparqlExpression::SparqlExpressionPimpl expression_;
  // ___________________________________________________________________________
  explicit ExpressionOrderKey(
      sparqlExpression::SparqlExpressionPimpl expression,
      bool isDescending = false)
      : isDescending_{isDescending}, expression_{std::move(expression)} {}
};

/// Store a variable that appeared in an ORDER BY clause.
class VariableOrderKey {
 public:
  string variable_;
  bool isDescending_;
  // ___________________________________________________________________________
  explicit VariableOrderKey(string variable, bool isDescending = false)
      : variable_{std::move(variable)}, isDescending_{isDescending} {}

  bool operator==(const VariableOrderKey&) const = default;
};

// Represents an ordering by a variable or an expression.
using OrderKey = std::variant<VariableOrderKey, ExpressionOrderKey>;
