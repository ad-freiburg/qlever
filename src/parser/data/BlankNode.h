// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include "./Context.h"

class BlankNode {
  bool _generated;
  std::string _label;

 public:
  BlankNode(bool generated, std::string label)
      : _generated{generated}, _label{std::move(label)} {}

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      const Context& context, [[maybe_unused]] ContextRole role) const {
    std::ostringstream stream;
    stream << "_:";
    // generated or user-defined
    stream << (_generated ? 'g' : 'u');
    stream << context._row << '_';
    stream << _label;
    return stream.str();
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const {
    std::ostringstream stream;
    stream << "_:";
    // generated or user-defined
    stream << (_generated ? 'g' : 'u');
    stream << '_';
    stream << _label;
    return stream.str();
  }
};
