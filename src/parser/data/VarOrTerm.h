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

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(const Context& context,
                                                    ContextRole role) const {
    // TODO<C++23>: remove static_cast as soon as we can visit types that
    // inherit from std::variant
    return std::visit(
        [&](const auto& object) { return object.evaluate(context, role); },
        static_cast<const VarOrTermBase&>(*this));
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const {
    // TODO<C++23>: remove static_cast as soon as we can visit types that
    // inherit from std::variant
    return std::visit([&](const auto& object) { return object.toSparql(); },
                      static_cast<const VarOrTermBase&>(*this));
  }
};
