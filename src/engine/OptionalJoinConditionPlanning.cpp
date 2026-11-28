// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Nino Frey, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/OptionalJoinConditionPlanning.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include "backports/algorithm.h"
#include "engine/sparqlExpressions/ExistsExpression.h"
#include "util/Algorithm.h"
#include "util/ParseException.h"

namespace qlever::queryPlanner {

namespace {
// Return the message for the `filter` of an OPTIONAL that uses the
// `variables`, which are bound outside of the OPTIONAL, but not always inside
// of it. If `inExists` is true, the `variables` are used by an `EXISTS` in the
// `filter` and the message is for a `NotSupportedException`, otherwise it is
// for the warning that the `filter` is evaluated on the content of the
// OPTIONAL alone.
std::string makeMessage(const sparqlExpression::SparqlExpressionPimpl& filter,
                        bool inExists,
                        const std::vector<std::string>& variables) {
  return absl::StrCat(
      "The FILTER ", filter.getDescriptor(), " inside of an OPTIONAL",
      inExists ? " contains an EXISTS that" : "", " uses ",
      absl::StrJoin(variables, ", "), ", which ",
      variables.size() > 1 ? "are" : "is",
      " bound outside of the OPTIONAL but not always inside of it. The SPARQL "
      "standard evaluates such ",
      inExists ? "an EXISTS" : "a FILTER",
      " on the joined rows, which is not supported yet",
      inExists ? ""
               : ", so QLever evaluates it on the content of the OPTIONAL "
                 "alone",
      ".");
}

// Return the names of the `variables` (pointers) for which `predicate` is
// true, without duplicates and in the order of their first occurrence.
template <typename Variables, typename Predicate>
std::vector<std::string> getNamesOfVariables(const Variables& variables,
                                             const Predicate& predicate) {
  return ad_utility::removeDuplicates(
      ::ranges::to_vector(variables | ql::views::filter(predicate) |
                          ql::views::transform(&Variable::name)));
}

// Return a predicate that is true for a `variable` iff it can have a different
// value in the joined row than in the row of the group, that is, it is a
// variable of the left side, and it is not always defined in the group
// (possibly not even contained in it).
auto makeGetsValueFromLeftSide(
    const ad_utility::HashSet<Variable>& variablesOfLeftSide,
    absl::FunctionRef<bool(const Variable&)> isAlwaysDefinedInGroup) {
  return
      [&variablesOfLeftSide, isAlwaysDefinedInGroup](const Variable* variable) {
        return variablesOfLeftSide.contains(*variable) &&
               !isAlwaysDefinedInGroup(*variable);
      };
}
}  // namespace

// _____________________________________________________________________________
bool isPartOfOptionalJoinCondition(
    const sparqlExpression::SparqlExpressionPimpl& filter,
    const ad_utility::HashSet<Variable>& variablesOfLeftSide,
    absl::FunctionRef<bool(const Variable&)> isAlwaysDefinedInGroup) {
  auto getsValueFromLeftSide =
      makeGetsValueFromLeftSide(variablesOfLeftSide, isAlwaysDefinedInGroup);
  // The contained variables of an `EXISTS` are only those that are also
  // visible in the group (see `ExistsExpression::selectVariables`), so a
  // variable that is only bound on the left side is not among them. We
  // therefore have to look at the variables of the argument itself.
  for (const auto* expression : filter.getExistsExpressions()) {
    const auto& exists =
        dynamic_cast<const sparqlExpression::ExistsExpression&>(*expression);
    auto variables = getNamesOfVariables(
        exists.argument().getVisibleVariables() |
            ql::views::transform(
                [](const Variable& variable) { return &variable; }),
        getsValueFromLeftSide);
    if (!variables.empty()) {
      throw NotSupportedException{makeMessage(filter, true, variables)};
    }
  }
  return ql::ranges::any_of(filter.containedVariables(), getsValueFromLeftSide);
}

// _____________________________________________________________________________
std::string getOptionalJoinConditionWarning(
    const sparqlExpression::SparqlExpressionPimpl& filter,
    const ad_utility::HashSet<Variable>& variablesOfLeftSide,
    absl::FunctionRef<bool(const Variable&)> isAlwaysDefinedInGroup) {
  auto variables = getNamesOfVariables(
      filter.containedVariables(),
      makeGetsValueFromLeftSide(variablesOfLeftSide, isAlwaysDefinedInGroup));
  return makeMessage(filter, false, variables);
}

}  // namespace qlever::queryPlanner
