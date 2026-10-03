// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "parser/PayloadVariables.h"

#include "util/Exception.h"
#include "util/TypeTraits.h"

// ____________________________________________________________________________
PayloadVariables::PayloadVariables(std::vector<Variable> variables)
    : variables_{std::move(variables)} {}

// ____________________________________________________________________________
PayloadVariables PayloadVariables::all() {
  PayloadVariables pv{};
  pv.setToAll();
  return pv;
}

// ____________________________________________________________________________
void PayloadVariables::addVariable(const Variable& variable) {
  // If the payload variables has not been set to all, add the variable.
  ad_utility::visitIf(variables_, [&variable](std::vector<Variable>& value) {
    value.push_back(variable);
  });
}

// ____________________________________________________________________________
void PayloadVariables::setToAll() {
  variables_ = detail::PayloadAllVariables{};
}

// ____________________________________________________________________________
bool PayloadVariables::empty() const {
  return ad_utility::visitIf(
      variables_,
      [](const std::vector<Variable>& value) { return value.empty(); },
      [](const detail::PayloadAllVariables&) { return false; });
}

// ____________________________________________________________________________
bool PayloadVariables::isAll() const {
  return std::holds_alternative<detail::PayloadAllVariables>(variables_);
}

// ____________________________________________________________________________
const std::vector<Variable>& PayloadVariables::getVariables() const {
  return ad_utility::visitIf(
      variables_,
      [](const std::vector<Variable>& value) -> const std::vector<Variable>& {
        return value;
      },
      [](const detail::PayloadAllVariables&) -> const std::vector<Variable>& {
        AD_THROW(
            "getVariables may only be called on a non-all PayloadVariables "
            "object.");
      });
}
