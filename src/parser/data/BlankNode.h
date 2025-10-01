// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_BLANKNODE_H
#define QLEVER_SRC_PARSER_DATA_BLANKNODE_H

#include <string>

#include "backports/three_way_comparison.h"
#include "parser/data/ConstructQueryExportContext.h"

class BlankNode {
  bool _generated;
  std::string _label;

  QL_DEFINE_CLASS_MEMBERS_AS_TIE(_generated, _label)

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

  QL_DEFINE_EQUALITY_OPERATOR(BlankNode)
};

#endif  // QLEVER_SRC_PARSER_DATA_BLANKNODE_H
