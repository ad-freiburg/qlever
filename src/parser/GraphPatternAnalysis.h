// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_
#define QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_

#include "parser/GraphPatternOperation.h"
#include "parser/VariableCounter.h"

// This module contains helpers for analyzing the structure of graph patterns.

// _____________________________________________________________________________
namespace graphPatternAnalysis {

// Check whether a given `GraphPatternOperation` is invariant with respect to
// the variable usage in the surrounding `GraphPattern`, meaning that removing
// it would not affect the result of the query. The variable usage is counted
// once up front (across the whole `GraphPattern`) into `variableCounts_`; a
// later `operator()(op)` query then asks whether `op`'s variables appear at
// most once in the pattern (i.e. only inside `op` itself).
//
// For example: a `BIND(... AS ?y)` is invariant when `?y` is not referenced
// anywhere else in the pattern (the `BIND` only adds its own column without
// adding or removing rows). A single-row `VALUES` clause is invariant when
// none of its variables are referenced elsewhere.
//
// This is currently used for the `MaterializedViewsManager`'s
// `QueryPatternCache`.
//
// NOTE: This does not guarantee completeness, so it might return `false` even
// though we could be invariant to a `GraphPatternOperation`.
struct BasicGraphPatternsInvariantTo {
  parsedQuery::VariableCounter variableCounts_;

  // Initialize with a `GraphPattern`.
  explicit BasicGraphPatternsInvariantTo(const parsedQuery::GraphPattern& gp);

  explicit BasicGraphPatternsInvariantTo(parsedQuery::VariableCounter vc)
      : variableCounts_{std::move(vc)} {}

  bool operator()(const parsedQuery::Bind& bind) const;
  bool operator()(const parsedQuery::Values& values) const;

  template <typename T>
  bool operator()(const T&) const {
    // The presence of any of these operations might remove or duplicate rows.
    namespace pq = parsedQuery;
    static_assert(
        ad_utility::SimilarToAny<
            T, pq::Optional, pq::Union, pq::Subquery, pq::TransPath,
            pq::BasicGraphPattern, pq::Service, pq::PathQuery, pq::SpatialQuery,
            pq::TextSearchQuery, pq::Minus, pq::GroupGraphPattern, pq::Describe,
            pq::Load, pq::NamedCachedResult, pq::MaterializedViewQuery,
            pq::ExternalValuesQuery>);
    return false;
  }

 private:
  // Check that the given variable is counted at most once by `variableCounts_`.
  bool variableAppearsAtMostOnce(const Variable& var) const;
};

}  // namespace graphPatternAnalysis

#endif  // QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_
