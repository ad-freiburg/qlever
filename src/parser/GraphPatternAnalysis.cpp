// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "parser/GraphPatternAnalysis.h"

namespace graphPatternAnalysis {

// _____________________________________________________________________________
BasicGraphPatternsInvariantTo::BasicGraphPatternsInvariantTo(
    const parsedQuery::GraphPattern& gp) {
  variableCounts_(gp);
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Bind& bind) const {
  return variableAppearsAtMostOnce(bind._target);
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Values& valuesClause) const {
  const auto& [variables, values] = valuesClause._inlineValues;
  return
      // There is exactly one row inside the `VALUES`.
      values.size() == 1 &&
      // The `VALUES` doesn't bind to any of the `variables_`.
      ql::ranges::all_of(variables, [this](const auto& var) {
        return variableAppearsAtMostOnce(var);
      });
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::variableAppearsAtMostOnce(
    const Variable& var) const {
  const auto& counts = variableCounts_.counts();
  auto it = counts.find(var);
  return it == counts.end() || it->second <= 1;
}

}  // namespace graphPatternAnalysis
