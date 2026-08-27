// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_

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

// The join pattern extracted from a materialized view's defining query: its
// triples, viewed as edges of a small labeled graph over the view's
// variables. Query rewriting looks for a subgraph isomorphism from this graph
// into (a subset of) the triples of the user's query.
struct ViewPattern {
  std::vector<PatternEdge> edges_;
  ViewPtr view_;
};

// Query-side triples with a simple IRI predicate, grouped by that predicate,
// for fast candidate lookup per pattern edge. Keyed by `string_view` into the
// query triple's own predicate string (valid only for the lifetime of the
// `BasicGraphPattern` it was built from).
using TriplesByPredicate =
    ad_utility::HashMap<std::string_view, std::vector<size_t>>;

struct ByCacheKeyInfo {
  ViewPtr view_;
  ad_utility::HashMap<size_t, size_t> colMapping_;
};
using ByCacheKeyInfoPtr = std::shared_ptr<const ByCacheKeyInfo>;

// Helper class that represents a possible join replacement and indicates the
// subset of triples it handles.
struct MaterializedViewJoinReplacement {
  std::shared_ptr<IndexScan> indexScan_;
  std::vector<size_t> coveredTriples_;

  // ___________________________________________________________________________
  size_t numJoins() const { return coveredTriples_.size() - 1; }
};

// Cache data structure for the `MaterializedViewsManager`. This object can be
// used for quickly looking up if a given query can be optimized by making use
// of an existing materialized view.
class QueryPatternCache {
 private:
  // Predicates appearing in each view's pattern, for quickly ruling out views
  // that share no predicate with the query.
  ad_utility::HashMap<std::string, std::vector<ViewPtr>> predicateInView_;

  // All patterns extracted from materialized views, one entry per view that
  // has at least one pattern-eligible edge.
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
  // per triple). Returns `nullopt` if a triple has a non-simple predicate, a
  // full-text pseudo-predicate (`ql:contains-word`/`ql:contains-entity`),
  // both subject and object fixed, a variable subject/object not in the
  // view's columns, (see `isConnected`) the pattern is disconnected, or the
  // view's physical column 0 never occurs in the pattern.
  static std::optional<std::vector<PatternEdge>> buildPatternEdges(
      const ViewPtr& view, const std::vector<SparqlTriple>& triples);

  // Search for all embeddings of `pattern` into `triples`, trying at most
  // `budget` candidate assignments, and add a `MaterializedViewJoinReplacement`
  // for each valid one (see `isLegalFixedValuePrefix`) to `result` -- capped,
  // independently of `budget`, at a fixed maximum total across all calls that
  // share `result` (see `kMaxReplacements` in the .cpp file).
  void matchPattern(QueryExecutionContext* qec, const ViewPattern& pattern,
                    const parsedQuery::BasicGraphPattern& triples,
                    const TriplesByPredicate& triplesByPredicate, size_t budget,
                    std::vector<MaterializedViewJoinReplacement>& result) const;
};

// Helper that filters the graph patterns of a parsed query using
// `BasicGraphPatternInvariantTo`. For details, see the documentation for this
// helper.
std::vector<parsedQuery::GraphPatternOperation> graphPatternInvariantFilter(
    const ParsedQuery& parsed);

// rewriting, as returned by `getTriplesForPatternRewrite`.
using RewriteIgnoreReason = std::string;

// Helper for `QueryPatternCache::analyzeView` that extracts the triples of a
// view's defining query's single `BasicGraphPattern` for further pattern
// analysis, provided the query even has a shape that pattern-based rewriting
// could apply to: no aggregation, no top-level
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
