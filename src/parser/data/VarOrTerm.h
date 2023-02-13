// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <variant>

#include "../../engine/ResultTable.h"
#include "../../index/Index.h"
#include "../../util/VisitMixin.h"
#include "./GraphTerm.h"
#include "./Variable.h"

using VarOrTermBase = std::variant<Variable, GraphTerm>;

class VarOrTerm : public VarOrTermBase,
                  public VisitMixin<VarOrTerm, VarOrTermBase> {
 public:
  using VarOrTermBase::VarOrTermBase;

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      const ConstructQueryExportContext& context, PositionInTriple role) const {
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
