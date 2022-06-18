// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <absl/types/variant.h>

#include <string>

#include "../../engine/ResultTable.h"
#include "../../index/Index.h"
#include "./GraphTerm.h"
#include "./Variable.h"

using VarOrTermBase = absl::variant<Variable, GraphTerm>;

class VarOrTerm : public VarOrTermBase {
 public:
  using VarOrTermBase::VarOrTermBase;

  // ___________________________________________________________________________
  [[nodiscard]] absl::optional<std::string> evaluate(const Context& context,
                                                     ContextRole role) const {
    // TODO<C++23>: remove static_cast as soon as we can visit types that
    // inherit from absl::variant
    return absl::visit(
        [&](const auto& object) { return object.evaluate(context, role); },
        static_cast<const VarOrTermBase&>(*this));
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const {
    // TODO<C++23>: remove static_cast as soon as we can visit types that
    // inherit from absl::variant
    return absl::visit([&](const auto& object) { return object.toSparql(); },
                       static_cast<const VarOrTermBase&>(*this));
  }
};
