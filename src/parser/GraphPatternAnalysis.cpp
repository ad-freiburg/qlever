// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "parser/GraphPatternAnalysis.h"

namespace graphPatternAnalysis {

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Optional&) const {
  // TODO<ullingerc> Analyze if the optional binds values from the outside
  // query.
  return false;
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Bind& bind) const {
  return !variables_.contains(bind._target);
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Values& values) const {
  return !std::ranges::any_of(
      values._inlineValues._variables,
      [this](const auto& var) { return variables_.contains(var); });
}

}  // namespace graphPatternAnalysis
