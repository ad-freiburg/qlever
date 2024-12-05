// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "engine/PayloadVariables.h"

// ____________________________________________________________________________
PayloadVariables::PayloadVariables(std::vector<Variable> variables)
    : variables_{std::move(variables)} {};

// ____________________________________________________________________________
PayloadVariables PayloadVariables::all() {
  PayloadVariables pv{};
  pv.setToAll();
  return pv;
};

// ____________________________________________________________________________
void PayloadVariables::addVariable(const Variable& variable) {
  // Helper visitor to check if the payload variables has not been set to all
  // and a variable can be added. If yes, add it.
  auto addVarVisitor = [&]<typename T>(T& value) {
    if constexpr (std::is_same_v<T, std::vector<Variable>>) {
      value.push_back(variable);
    }
  };

  std::visit(addVarVisitor, variables_);
};

// ____________________________________________________________________________
void PayloadVariables::setToAll() {
  variables_ = detail::PayloadAllVariables{};
};

// ____________________________________________________________________________
bool PayloadVariables::empty() const {
  // Helper visitor to check if the payload variables are empty
  auto emptyVisitor = []<typename T>(const T& value) -> bool {
    if constexpr (std::is_same_v<T, detail::PayloadAllVariables>) {
      return false;
    } else {
      static_assert(std::is_same_v<T, std::vector<Variable>>);
      return value.empty();
    }
  };

  return std::visit(emptyVisitor, variables_);
};

// ____________________________________________________________________________
bool PayloadVariables::isAll() const {
  // Helper visitor to check if the payload variables has been set to all
  auto allVisitor = []<typename T>(const T&) -> bool {
    if constexpr (std::is_same_v<T, detail::PayloadAllVariables>) {
      return true;
    } else {
      static_assert(std::is_same_v<T, std::vector<Variable>>);
      return false;
    }
  };

  return std::visit(allVisitor, variables_);
};

// ____________________________________________________________________________
std::vector<Variable> PayloadVariables::getVariables() const {
  // Helper visitor to check if the payload variables has been set to all: then
  // throw, otherwise return the vector
  auto getVarVisitor = []<typename T>(const T& value) -> std::vector<Variable> {
    if constexpr (std::is_same_v<T, std::vector<Variable>>) {
      return value;
    } else {
      AD_THROW(
          "getVariables may only be called on a non-all PayloadVariables "
          "object.");
    }
  };

  return std::visit(getVarVisitor, variables_);
};
