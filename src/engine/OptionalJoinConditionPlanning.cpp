// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Nino Frey, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/OptionalJoinConditionPlanning.h"

#include "backports/algorithm.h"
#include "engine/sparqlExpressions/ExistsExpression.h"
#include "util/ParseException.h"

// _____________________________________________________________________________
bool isPartOfOptionalJoinCondition(
    const sparqlExpression::SparqlExpressionPimpl& filter,
    const ad_utility::HashSet<Variable>& variablesOfLeftSide,
    absl::FunctionRef<bool(const Variable&)> isAlwaysDefinedInGroup) {
  auto getsValueFromLeftSide = [&](const Variable& variable) {
    return variablesOfLeftSide.contains(variable) &&
           !isAlwaysDefinedInGroup(variable);
  };
  // The contained variables of an `EXISTS` are only those that are also
  // visible in the group (see `ExistsExpression::selectVariables`), so a
  // variable that is only bound on the left side is not among them. We
  // therefore have to look at the variables of the argument itself.
  for (const auto* expression : filter.getExistsExpressions()) {
    const auto& exists =
        dynamic_cast<const sparqlExpression::ExistsExpression&>(*expression);
    if (ql::ranges::any_of(exists.argument().getVisibleVariables(),
                           getsValueFromLeftSide)) {
      throw NotSupportedException{
          "A FILTER with EXISTS inside of an OPTIONAL, where the EXISTS uses "
          "a variable that is bound outside of the OPTIONAL but not always "
          "bound inside of it, is not supported yet."};
    }
  }
  return ql::ranges::any_of(filter.containedVariables(),
                            [&](const Variable* variable) {
                              return getsValueFromLeftSide(*variable);
                            });
}
