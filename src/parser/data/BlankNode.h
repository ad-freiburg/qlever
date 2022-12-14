// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

#include "parser/data/ConstructQueryExportContext.h"

class BlankNode {
  bool _generated;
  std::string _label;

 public:
  BlankNode(bool generated, std::string label);

  // ___________________________________________________________________________
  // Used for testing
  [[nodiscard]] bool isGenerated() const { return _generated; }

  // ___________________________________________________________________________
  // Used for testing
  [[nodiscard]] const std::string& label() const { return _label; }

  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      const ConstructQueryExportContext& context,
      [[maybe_unused]] PositionInTriple positionInTriple) const;

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const;

  bool operator==(const BlankNode& other) const = default;
};
