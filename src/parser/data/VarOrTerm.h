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

using VarOrTermBase = std::variant<Variable, GraphTerm>;

class VarOrTerm : public VarOrTermBase {
 public:
  using VarOrTermBase::VarOrTermBase;

  [[nodiscard]] std::optional<std::string> toString(const Context& context,
                                                    ContextRole role) const {
    return std::visit(
        [&](const auto& object) { return object.toString(context, role); },
        static_cast<const VarOrTermBase&>(*this));
  }

  [[nodiscard]] std::string toString() const {
    return std::visit([&](const auto& object) { return object.toString(); },
                      static_cast<const VarOrTermBase&>(*this));
  }
};
