//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_ORDERKEY_H
#define QLEVER_SRC_PARSER_DATA_ORDERKEY_H

#include <string>
#include <variant>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"

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
  bool isDescending_;
  Variable variable_;
  // ___________________________________________________________________________
  explicit VariableOrderKey(Variable variable, bool isDescending = false)
      : isDescending_{isDescending}, variable_{std::move(variable)} {}

  bool operator==(const VariableOrderKey&) const = default;
};

// Represents an ordering by a variable or an expression.
using OrderKey = std::variant<VariableOrderKey, ExpressionOrderKey>;

enum struct IsInternalSort { True, False };
struct OrderClause {
  IsInternalSort isInternalSort = IsInternalSort::False;
  std::vector<OrderKey> orderKeys;
};

#endif  // QLEVER_SRC_PARSER_DATA_ORDERKEY_H
