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
// affect the result of a query that only selects `variables_`. This is
// currently used for the `MaterializedViewsManager`'s `QueryPatternCache`.
//
// NOTE: This does not guarantee completeness, so it might return `false` even
// though we could be invariant to a `GraphPatternOperation`.
//
// NOTE: The selected query result is expected to be deduplicated, otherwise the
// result indicated by this helper is not correct.
struct BasicGraphPatternsInvariantTo {
  ad_utility::HashSet<Variable> variables_;

  bool operator()(const parsedQuery::Optional& optional) const;
  bool operator()(const parsedQuery::Bind& bind) const;
  bool operator()(const parsedQuery::Values& values) const;

  CPP_template(typename T)(requires(
      // TODO<ullingerc> Whitelist
      !ad_utility::SimilarToAny<T, parsedQuery::Optional, parsedQuery::Bind,
                                parsedQuery::Values>)) bool operator()(const T&)
      const {
    return false;
  }
};

}  // namespace graphPatternAnalysis

#endif  // QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_
