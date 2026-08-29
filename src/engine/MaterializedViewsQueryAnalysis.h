// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_

#include <absl/numeric/bits.h>

#include <cstdint>
#include <variant>

#include "engine/VariableToColumnMap.h"
#include "parser/GraphPatternAnalysis.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "util/HashMap.h"

// Forward declarations to prevent cyclic dependencies.
class MaterializedView;
class IndexScan;

// _____________________________________________________________________________
namespace materializedViewsQueryAnalysis {

using ViewPtr = std::shared_ptr<const MaterializedView>;
using graphPatternAnalysis::BasicGraphPatternsInvariantTo;

// An edge of a view's join pattern graph: a triple whose predicate has
// already been resolved to a simple IRI (see `buildPatternEdges`).
using PatternEdge = SparqlTripleBase<std::string>;

// The join pattern of a view: its triples, viewed as edges of a small labeled
// graph over the view's variables. Rewriting looks for a subgraph isomorphism
// from this graph into the triples of the user's query.
struct ViewPattern {
  std::vector<PatternEdge> edges_;
  ViewPtr view_;
};

// Map from predicate IRI to a bitmask of the triple indices.
using TriplesByPredicate = ad_utility::HashMap<std::string_view, uint64_t>;

// The limits bounding one call to `PatternMatcher::findReplacementPlans`
// (aliased there as `PatternMatcher::Limits`; here at namespace scope to avoid
// a circular include). Also used to report how much a call actually used, so
// that several calls can share one pool.
struct PatternMatcherLimits {
  size_t numAssignments_;
  size_t numReplacementPlans_;

  // A fixed share of `*this` per view, split evenly across `numViews` (must be
  // nonzero), so that the limits apply per query and not per (view, query)
  // pair. Never below a floor, so that a share stays usable for many views.
  PatternMatcherLimits perViewShare(size_t numViews) const;

  bool isExhausted() const {
    return numAssignments_ == 0 || numReplacementPlans_ == 0;
  }

  PatternMatcherLimits requestBounded(
      PatternMatcherLimits requestedAmount) const;

  void subtract(PatternMatcherLimits used);
};

struct ByCacheKeyInfo {
  ViewPtr view_;
  ad_utility::HashMap<size_t, size_t> colMapping_;
};
using ByCacheKeyInfoPtr = std::shared_ptr<const ByCacheKeyInfo>;

// Helper class that represents a possible join replacement and indicates the
// subset of triples it handles.
struct MaterializedViewJoinReplacement {
  std::shared_ptr<IndexScan> indexScan_;
  // Bitmask of covered query triple indices, like
  // `QueryPlanner::SubtreePlan::_idsOfIncludedNodes`.
  uint64_t coveredTriples_;

  // ___________________________________________________________________________
  size_t numJoins() const { return absl::popcount(coveredTriples_) - 1; }
};

// Cache data structure for the `MaterializedViewsManager`. This object can be
// used for quickly looking up if a given query can be optimized by making use
// of an existing materialized view.
class QueryPatternCache {
 private:
  // Predicates appearing in each view's pattern, for quickly ruling out views
  // that share no predicate with the query.
  ad_utility::HashMap<std::string, std::vector<ViewPtr>> predicateInView_;

  // All patterns extracted from materialized views.
  ad_utility::HashMap<ViewPtr, ViewPattern> patterns_;

  ad_utility::HashMap<std::string, ByCacheKeyInfoPtr> byCacheKey_;

  // NOTE: When a new data structure for caching is added here, the unloading
  // should also be implemented in the `removeView` method.
 public:
  // Given a materialized view, analyze the query it was created from and
  // populate the cache. This is called from
  // `MaterializedViewsManager::loadView`.
  bool analyzeView(ViewPtr view, QueryExecutionContext* qec);

  // Remove all pointers to a view from this `QueryPatternCache`. This is
  // required for unloading materialized views. A call to this function with a
  // `ViewPtr` that is not cached is a no-op.
  void removeView(ViewPtr view);

  // Given a set of triples, check if a subset of necessary join operations can
  // be replaced by scans on materialized views.
  std::vector<MaterializedViewJoinReplacement> makeJoinReplacementIndexScans(
      QueryExecutionContext* qec,
      const parsedQuery::BasicGraphPattern& triples) const;

  ByCacheKeyInfoPtr lookupByCacheKey(const std::string& cacheKey) const;

 private:
  // Helper for `analyzeView`: build the view's pattern graph (one `PatternEdge`
  // per triple), reordered by `connectedOrder` for `PatternMatcher`'s benefit.
  // Returns `nullopt` if the view is not eligible; the individual reasons are
  // documented at the corresponding checks.
  static std::optional<std::vector<PatternEdge>> buildPatternEdges(
      const ViewPtr& view, const std::vector<SparqlTriple>& triples);
};

// Helper that filters the graph patterns of a parsed query using
// `BasicGraphPatternInvariantTo`. For details, see the documentation for this
// helper.
std::vector<parsedQuery::GraphPatternOperation> graphPatternInvariantFilter(
    const ParsedQuery& parsed);

// Human-readable explanation why a query is not eligible for pattern-based
// rewriting, as returned by `getTriplesForPatternRewrite`.
using RewriteIgnoreReason = std::string;

// Helper for `QueryPatternCache::analyzeView` that extracts the triples of a
// view's defining query's single `BasicGraphPattern` for further pattern
// analysis, provided the query has a shape that pattern-based rewriting can
// apply to (see the checks in the implementation). Otherwise a
// `RewriteIgnoreReason` explaining why is returned.
std::variant<RewriteIgnoreReason, std::vector<SparqlTriple>>
getTriplesForPatternRewrite(const ParsedQuery& parsed);

// Hash map for the `BIND`-to-column map.
using BindExpressionAndTargetCol = ad_utility::HashMap<std::string, size_t>;

// Extract all `BIND` statements from a `ParsedQuery` and create a hash map
// mapping `BIND` expression cache keys to target variable column index.
BindExpressionAndTargetCol extractBindExpressions(
    const ParsedQuery& parsed, const VariableToColumnMap& varToColMap);

}  // namespace materializedViewsQueryAnalysis

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
