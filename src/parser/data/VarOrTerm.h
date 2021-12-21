// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <variant>

#include "../../engine/ResultTable.h"
#include "../../index/Index.h"
#include "./GraphTerm.h"
#include "./Variable.h"

class VarOrTerm : public std::variant<Variable, GraphTerm> {
 public:
  using std::variant<Variable, GraphTerm>::variant;

  [[nodiscard]] std::string toString(const Context& context) const {
    return std::visit(
        [&](const auto& object) { return object.toString(context); },
        static_cast<const std::variant<Variable, GraphTerm>&>(*this));
  }
};
