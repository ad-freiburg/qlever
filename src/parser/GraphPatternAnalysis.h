// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_
#define QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_

#include "parser/GraphPatternOperation.h"

// This module contains helpers for analyzing the structure of graph patterns.

// _____________________________________________________________________________
namespace graphPatternAnalysis {

// Check whether certain graph patterns can be ignored when we are only
// interested in the bindings for variables from `variables_` as they do not
// affect the result for these `variables_`.
//
// For example: A basic graph pattern (a list of triples) is invariant to a
// `BIND` statement whose target variable is not contained in the basic graph
// pattern, because the `BIND` only adds its own column, but neither adds nor
// deletes result rows.
//
// This is currently used for the `MaterializedViewsManager`'s
// `QueryPatternCache`.
//
// NOTE: This does not guarantee completeness, so it might return `false` even
// though we could be invariant to a `GraphPatternOperation`.
struct BasicGraphPatternsInvariantTo {
  ad_utility::HashSet<Variable> variables_;

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
            pq::Load, pq::NamedCachedResult, pq::MaterializedViewQuery>);
    return false;
  }
};

}  // namespace graphPatternAnalysis

#endif  // QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_
