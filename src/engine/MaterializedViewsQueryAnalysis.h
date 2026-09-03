// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_

#include <cstdint>
#include <optional>
#include <variant>

#include "engine/VariableToColumnMap.h"
#include "parser/GraphPatternAnalysis.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/Variable.h"
#include "util/StringPairHashMap.h"

// Forward declarations to prevent cyclic dependencies.
class MaterializedView;
class IndexScan;

// _____________________________________________________________________________
namespace materializedViewsQueryAnalysis {

using ViewPtr = std::shared_ptr<const MaterializedView>;
using graphPatternAnalysis::BasicGraphPatternsInvariantTo;
using VariableToTripleIndices =
    ad_utility::HashMap<Variable, std::vector<size_t>>;

// Key and value types of the cache for simple chains, that is queries of the
// form `?s <p1> ?m . ?m <p2> ?o`.
using ChainedPredicates = ad_utility::detail::StringPair;
using ChainedPredicatesForLookup = ad_utility::detail::StringViewPair;
struct ChainInfo {
  Variable subject_;
  Variable chain_;
  Variable object_;
  ViewPtr view_;
};
using SimpleChainCache =
    ad_utility::StringPairHashMap<std::shared_ptr<std::vector<ChainInfo>>>;

// Types required to store cached join star patterns extracted from views. That
// is, queries of the form `?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p3> ?o3 ...`.
// The `StarInfo` holds the subject variable shared between all arms of the star
// and the `StarArm` for each of them. The `StarArm` stores the predicate IRI
// and object variable.
using StarArm = std::pair<std::string, Variable>;
struct StarInfo {
  Variable subject_;
  std::vector<StarArm> arms_;
};

struct ByCacheKeyInfo {
  ViewPtr view_;
  ad_utility::HashMap<size_t, size_t> colMapping_;

  // If set, the view's first column does not appear as a variable in the
  // matched query, but is restricted to this fixed value. The `colMapping_`
  // then has no entry that maps to view column `0`; instead the scan on the
  // view fixes that column (see `MaterializedView::makeIndexScan`).
  std::optional<TripleComponent> fixedFirstColumn_;
};
using ByCacheKeyInfoPtr = std::shared_ptr<const ByCacheKeyInfo>;

// A reusable, per-view cache-key "template" for a fixed first column,
// computed once when the view is loaded (see `QueryPatternCache::analyzeView`)
// by planning the view's own query with a real, sampled value (see
// `sampleFirstColumnValue` in the .cpp) substituted for its first column, in
// place of the value that any particular later query fixes it to. Substituting
// a real query value's serialized form for `placeholderValue_`'s in
// `cacheKeyTemplate_` (see `addCacheKeysWithFixedFirstColumn`) cheaply
// reproduces the cache key that a full replan with that value would produce --
// PROVIDED the plan has the same shape for both, which is not guaranteed (e.g.
// if cost-based planning picks a different join order due to a different
// value's estimated selectivity). This is a greedy performance optimization
// over replanning per candidate value: on a mismatch, the guessed cache key
// simply matches nothing in `QueryPatternCache::byCacheKey_` later, the same
// outcome as any other case that is not eligible for the rewrite.
struct FixedFirstColumnCacheKeyTemplate {
  ad_utility::HashMap<size_t, size_t> colMapping_;
  std::string cacheKeyTemplate_;
  TripleComponent placeholderValue_;
};

// Cache keys of views whose first column is fixed to a value taken from the
// query that is currently being planned. Unlike the cache keys in
// `QueryPatternCache::byCacheKey_` these cannot be computed when a view is
// loaded, because the values to substitute are only known once the query is
// known. They are therefore stored per query in the `QueryExecutionContext`,
// see `addCacheKeysWithFixedFirstColumn`. This is a `struct` and not a plain
// alias so that it can be forward-declared in
// `engine/QueryExecutionContext.h`, which cannot include this header.
struct ViewCacheKeysWithFixedFirstColumn {
  ad_utility::HashMap<std::string, ByCacheKeyInfoPtr> keys_;
};

// Helper class that represents a possible join replacement and indicates the
// subset of triples it handles.
struct MaterializedViewJoinReplacement {
  std::shared_ptr<IndexScan> indexScan_;
  // Bitmask of covered query triple indices, like
  // `QueryPlanner::SubtreePlan::_idsOfIncludedNodes`.
  uint64_t coveredTriples_;
};

// Cache data structure for the `MaterializedViewsManager`. This object can be
// used for quickly looking up if a given query can be optimized by making use
// of an existing materialized view.
class QueryPatternCache {
 private:
  // Simple chains can be found by direct access into a hash map.
  SimpleChainCache simpleChainCache_;

  // Cache for predicates appearing in a materialized view. The `ViewPtr`s are
  // kept sorted.
  ad_utility::HashMap<std::string, std::vector<ViewPtr>> predicateInView_;

  // All star patterns extracted from materialized views.
  ad_utility::HashMap<ViewPtr, StarInfo> starCache_;

  ad_utility::HashMap<std::string, ByCacheKeyInfoPtr> byCacheKey_;

  // Precomputed fixed-first-column cache-key templates, one entry per view
  // whose first column can be fixed at all (see `substituteFirstColumn`).
  ad_utility::HashMap<ViewPtr, std::vector<FixedFirstColumnCacheKeyTemplate>>
      fixedFirstColumnTemplates_;

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

  // Construct an `IndexScan` for a single chain join given the necessary
  // information from both the materialized view and the user's query.
  std::shared_ptr<IndexScan> makeScanForSingleChain(
      QueryExecutionContext* qec, ChainInfo cached, TripleComponent subject,
      std::optional<Variable> chain, Variable object) const;

  // Construct an `IndexScan` for a star join given the `RequestedColumns`
  // object that maps the columns of the materialized view to the subject and
  // object variable names from the user query. This assumes that the `starView`
  // represents the appropriate star join.
  std::shared_ptr<IndexScan> makeScanForStar(
      QueryExecutionContext* qec, ViewPtr starView,
      parsedQuery::MaterializedViewQuery::RequestedColumns columns) const;

  ByCacheKeyInfoPtr lookupByCacheKey(const std::string& cacheKey) const;

  // For each view with a `FixedFirstColumnCacheKeyTemplate` whose first column
  // can be fixed to a value occurring in `triples`, substitute that value into
  // the template (see `FixedFirstColumnCacheKeyTemplate`) and add it to
  // `result`. Candidate values are found by looking at the query triples whose
  // predicate and position (subject or object) match a top-level triple of the
  // view's query in which the first column's variable occurs.
  //
  // Unlike computing the cache key of the view's own query with a value
  // substituted from scratch (which requires a full replan per candidate
  // value), this only does a cheap string substitution into a template that
  // was already planned once when the view was loaded, so it is fast enough to
  // run for every query, regardless of whether the dynamic-programming or the
  // greedy query planner is used.
  void addCacheKeysWithFixedFirstColumn(
      const parsedQuery::BasicGraphPattern& triples,
      ViewCacheKeysWithFixedFirstColumn& result) const;

 private:
  // Helper for `analyzeView`, that checks for a simple chain. It returns `true`
  // iff a simple chain `a->b` is present.
  // NOTE: This function only checks one direction, so it should also be called
  // with `a` and `b` switched if it returns `false`.
  bool analyzeSimpleChain(ViewPtr view, const SparqlTriple& a,
                          const SparqlTriple& b);

  // Helper for `analyzeView`, that checks for a join star of arbitrary size.
  // A star requires all triples to share the same subject variable with
  // distinct simple IRI predicates and distinct variable objects.
  //
  // This function assumes that the query used for writing the `view` consists
  // of exactly one `BasicGraphPattern` with at least two triples and optionally
  // some invariant `BIND` statements. The argument `triples` is required to
  // contain the parsed triples from the `BasicGraphPattern` in `view`.
  //
  // Using `triples`, the function checks if the view represents a join star. If
  // yes, it adds the `view` to the cache for join stars. The function returns
  // `true` iff the view contains a star.
  bool analyzeJoinStar(ViewPtr view, const std::vector<SparqlTriple>& triples);

  // Given potential left and right sides of simple chains, check for available
  // replacement index scans, construct them and insert them into the `result`
  // vector.
  void makeScansFromChainCandidates(
      QueryExecutionContext* qec, const parsedQuery::BasicGraphPattern& triples,
      std::vector<MaterializedViewJoinReplacement>& result,
      const VariableToTripleIndices& chainLeft,
      const VariableToTripleIndices& chainRight) const;

  // Given triples grouped by subject, check for available star join replacement
  // index scans, construct them and insert them into the `result` vector.
  void makeScansFromStarCandidates(
      QueryExecutionContext* qec, const parsedQuery::BasicGraphPattern& triples,
      std::vector<MaterializedViewJoinReplacement>& result,
      const VariableToTripleIndices& starCandidates) const;
};

// Helper that filters the graph patterns of a parsed query using
// `BasicGraphPatternInvariantTo`. For details, see the documentation for this
// helper.
std::vector<parsedQuery::GraphPatternOperation> graphPatternInvariantFilter(
    const ParsedQuery& parsed);

// Substitute `value` for `variable` (the variable of a view's first column) in
// `parsed` (that view's own defining query), such that planning the result
// yields the query execution tree that the equivalent user query -- the view's
// query with the first column restricted to `value` -- is planned to. On
// success the variable is gone from the query and from its `SELECT` clause.
//
// Returns `false` (leaving `parsed` in an unspecified state) if the
// substitution would not be semantics-preserving or is unsupported, in
// particular if `variable` occurs anywhere but as the subject or object of a
// top-level triple. See the implementation for the full list of reasons.
bool substituteFirstColumn(ParsedQuery& parsed, const Variable& variable,
                           const TripleComponent& value);

// Human-readable explanation why a query is not eligible for pattern-based
// (star/chain) rewriting, as returned by `getTriplesForPatternRewrite`.
using RewriteIgnoreReason = std::string;

// Helper for `QueryPatternCache::analyzeView` that extracts the triples of a
// view's defining query's single `BasicGraphPattern` for further (star/chain)
// pattern analysis, provided the query even has a shape that pattern-based
// rewriting could apply to: no aggregation, no top-level
// `FILTER`/`VALUES`/`DISTINCT`/`REDUCED`/`LIMIT`/`OFFSET` or
// `FROM`/`FROM NAMED` that would restrict the on-disk rows, and exactly one
// `BasicGraphPattern` with at least one triple (after skipping invariant
// patterns like `BIND`). If `parsed` does not have such a shape, a
// `RewriteIgnoreReason` explaining why is returned instead.
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
