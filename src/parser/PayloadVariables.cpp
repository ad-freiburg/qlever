// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "parser/PayloadVariables.h"

#include "util/Exception.h"

// ____________________________________________________________________________
PayloadVariables::PayloadVariables(qlever::Allocator<Variable> allocator)
    : variables_{qlever::vector<Variable>{std::move(allocator)}} {}

// ____________________________________________________________________________
PayloadVariables::PayloadVariables(qlever::vector<Variable> variables)
    : variables_{std::move(variables)} {}

// ____________________________________________________________________________
PayloadVariables PayloadVariables::all() {
  return PayloadVariables{detail::PayloadAllVariables{}};
}

// ____________________________________________________________________________
void PayloadVariables::addVariable(const Variable& variable) {
  // Helper visitor to check if the payload variables has not been set to all
  // and a variable can be added. If yes, add it.
  auto addVarVisitor = [&](auto& value) {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, qlever::vector<Variable>>) {
      value.push_back(variable);
    }
  };

  std::visit(addVarVisitor, variables_);
}

// ____________________________________________________________________________
void PayloadVariables::setToAll() {
  variables_ = detail::PayloadAllVariables{};
}

// ____________________________________________________________________________
bool PayloadVariables::empty() const {
  // Helper visitor to check if the payload variables are empty
  auto emptyVisitor = [](const auto& value) -> bool {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, detail::PayloadAllVariables>) {
      return false;
    } else {
      static_assert(std::is_same_v<T, qlever::vector<Variable>>);
      return value.empty();
    }
  };

  return std::visit(emptyVisitor, variables_);
}

// ____________________________________________________________________________
bool PayloadVariables::isAll() const {
  return std::holds_alternative<detail::PayloadAllVariables>(variables_);
}

// ____________________________________________________________________________
const qlever::vector<Variable>& PayloadVariables::getVariables() const {
  // Helper visitor to check if the payload variables has been set to all: then
  // throw, otherwise return the vector
  auto getVarVisitor = [](const auto& value) -> const qlever::vector<Variable>& {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, qlever::vector<Variable>>) {
      return value;
    } else {
      AD_THROW(
          "getVariables may only be called on a non-all PayloadVariables "
          "object.");
    }
  };

  return std::visit(getVarVisitor, variables_);
}
