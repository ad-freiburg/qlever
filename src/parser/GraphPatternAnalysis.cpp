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
    const parsedQuery::Values& valuesClause) const {
  const auto& [variables, values] = valuesClause._inlineValues;
  return
      // There is exactly one row inside the `VALUES`.
      values.size() == 1 &&
      // The `VALUES` doesn't bind to any of the `variables_`.
      ql::ranges::none_of(variables, [this](const auto& var) {
        return variables_.contains(var);
      });
}

// _____________________________________________________________________________
template <typename T>
bool BasicGraphPatternsInvariantTo::operator()(const T&) const {
  // The presence of any of these operations might remove or duplicate rows.
  namespace pq = parsedQuery;
  static_assert(
      ad_utility::SimilarToAny<
          T, pq::Optional, pq::Union, pq::Subquery, pq::TransPath,
          pq::BasicGraphPattern, pq::Service, pq::PathQuery, pq::SpatialQuery,
          pq::TextSearchQuery, pq::Minus, pq::GroupGraphPattern, pq::Describe,
          pq::Load, pq::NamedCachedResult, pq::MaterializedViewQuery>);
  return false;
}

}  // namespace graphPatternAnalysis
