// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_BLANKNODE_H
#define QLEVER_SRC_PARSER_DATA_BLANKNODE_H

#include <string>

#include "backports/three_way_comparison.h"
#include "util/Exception.h"

class BlankNode {
  bool _generated;
  std::string _label;

 public:
  BlankNode(bool generated, std::string label);

  // ___________________________________________________________________________
  // Used for testing
  bool isGenerated() const { return _generated; }

  // ___________________________________________________________________________
  // Used for testing
  const std::string& label() const { return _label; }

  // ___________________________________________________________________________
  std::string toSparql() const;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(BlankNode, _generated, _label)
};

#endif  // QLEVER_SRC_PARSER_DATA_BLANKNODE_H
