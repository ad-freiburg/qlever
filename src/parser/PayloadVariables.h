// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_PAYLOADVARIABLES_H
#define QLEVER_SRC_PARSER_PAYLOADVARIABLES_H

#include <vector>

#include "rdfTypes/Variable.h"
#include "util/Allocator.h"
#include "util/AllocatorTypes.h"

namespace detail {
// Represents the selection of all variables as payload
struct PayloadAllVariables : std::monostate {
  bool operator==(
      [[maybe_unused]] const qlever::vector<Variable>& other) const {
    return false;
  }
};
}  // namespace detail

// This class represents a list of variables to be included in the result of
// an operation. This is currently used in the spatial search.
class PayloadVariables {
 public:
  // Construct an empty (not all) payload variables object. `allocator` is the
  // real allocator that the (initially empty) list of variables is routed
  // through; there is no implicit unlimited-allocator fallback.
  explicit PayloadVariables(qlever::Allocator<Variable> allocator);

  // Construct a payload variables object from a vector of variables
  PayloadVariables(qlever::vector<Variable> variables);

  // Construct a payload variables object that is set to all. This needs no
  // allocator: the "all" state never holds a vector.
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
  const qlever::vector<Variable>& getVariables() const;

  // For testing: equality operator
  bool operator==(const PayloadVariables& other) const {
    return variables_ == other.variables_;
  }

 private:
  // Construct directly in the "all" state, without ever needing a vector (and
  // therefore without needing an allocator). Used by `all()`.
  explicit PayloadVariables(detail::PayloadAllVariables tag) : variables_{tag} {}

  std::variant<detail::PayloadAllVariables, qlever::vector<Variable>>
      variables_;
};

#endif  // QLEVER_SRC_PARSER_PAYLOADVARIABLES_H
