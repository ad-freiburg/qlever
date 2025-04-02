// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_PAYLOADVARIABLES_H
#define QLEVER_SRC_PARSER_PAYLOADVARIABLES_H

#include <vector>

#include "parser/data/Variable.h"

namespace detail {
// Represents the selection of all variables as payload
struct PayloadAllVariables : std::monostate {
  bool operator==([[maybe_unused]] const std::vector<Variable>& other) const {
    return false;
  };
};
}  // namespace detail

// This class represents a list of variables to be included in the result of
// an operation. This is currently used in the spatial search.
class PayloadVariables {
 public:
  // Construct an empty payload variables object
  PayloadVariables() = default;

  // Construct a payload variables object from a vector of variables
  PayloadVariables(std::vector<Variable> variables);

  // Construct a payload variables object that is set to all
  static PayloadVariables all();

  // Add a variable to the payload variables or do nothing if all variables are
  // already selected
  void addVariable(const Variable& variable);

  // Select all variables.
  void setToAll();

  // Returns whether the payload variables object is empty.
  bool empty() const;

  // Returns whether all variables have been selected.
  bool isAll() const;

  // Returns a vector of variables if all has not been set. Otherwise throws.
  const std::vector<Variable>& getVariables() const;

  // For testing: equality operator
  bool operator==(const PayloadVariables& other) const {
    return variables_ == other.variables_;
  };

 private:
  std::variant<detail::PayloadAllVariables, std::vector<Variable>> variables_ =
      std::vector<Variable>{};
};

#endif  // QLEVER_SRC_PARSER_PAYLOADVARIABLES_H
