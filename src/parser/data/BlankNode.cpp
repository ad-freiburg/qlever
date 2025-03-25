//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "parser/data/BlankNode.h"

#include <ctre-unicode.hpp>
#include <sstream>

// _____________________________________________________________________________
BlankNode::BlankNode(bool generated, std::string label)
    : _generated{generated}, _label{std::move(label)} {
  // roughly check allowed characters as blank node labels.
  // Weaker than the SPARQL grammar, but good
  // enough so that it will likely never be an issue
  AD_CONTRACT_CHECK(ctre::match<"\\w(?:(?:\\w|-|\\.)*\\w)?">(_label));
}

// ___________________________________________________________________________
std::optional<std::string> BlankNode::evaluate(
    const ConstructQueryExportContext& context,
    [[maybe_unused]] PositionInTriple positionInTriple) const {
  std::ostringstream stream;
  stream << "_:";
  // generated or user-defined
  stream << (_generated ? 'g' : 'u');
  stream << context._row << '_';
  stream << _label;
  return stream.str();
}

// ___________________________________________________________________________
std::string BlankNode::toSparql() const {
  std::ostringstream stream;
  stream << "_:";
  // generated or user-defined
  stream << (_generated ? 'g' : 'u');
  stream << '_';
  stream << _label;
  return stream.str();
}
