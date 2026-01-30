// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "parser/GraphPatternAnalysis.h"

namespace graphPatternAnalysis {

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Bind& bind) const {
  return !variables_.contains(bind._target);
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Values& values) const {
  return
      // The `VALUES` doesn't bind to any of the `variables_`.
      !std::ranges::any_of(
          values._inlineValues._variables,
          [this](const auto& var) { return variables_.contains(var); }) &&
      // There is exactly one row inside the `VALUES`.
      values._inlineValues._values.size() == 1;
}

}  // namespace graphPatternAnalysis
