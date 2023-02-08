// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <variant>

#include "qlever/engine/ResultTable.h"
#include "qlever/index/Index.h"
#include "qlever/parser/data/GraphTerm.h"
#include "qlever/parser/data/Variable.h"
#include "qlever/util/VisitMixin.h"

using VarOrTermBase = std::variant<Variable, GraphTerm>;

class VarOrTerm : public VarOrTermBase,
                  public VisitMixin<VarOrTerm, VarOrTermBase> {
 public:
  using VarOrTermBase::VarOrTermBase;

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(const Context& context,
                                                    ContextRole role) const {
    // TODO<C++23>: Use std::visit when it is possible
    return visit([&context, &role](const auto& object) {
      return object.evaluate(context, role);
    });
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const {
    // TODO<C++23>: Use std::visit when it is possible
    return visit([](const auto& object) { return object.toSparql(); });
  }
};
