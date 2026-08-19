// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_

#include "engine/VariableToColumnMap.h"
#include "parser/GraphPatternAnalysis.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/Variable.h"
#include "util/HashMap.h"

// Forward declarations to prevent cyclic dependencies.
class MaterializedView;
class IndexScan;

// _____________________________________________________________________________
namespace materializedViewsQueryAnalysis {

using ViewPtr = std::shared_ptr<const MaterializedView>;
using graphPatternAnalysis::BasicGraphPatternsInvariantTo;

// One edge of a materialized view's join pattern graph, that is one triple of
// the view's defining query. `subject_`/`object_` are variables of the view
// (checked by `QueryPatternCache::buildPatternEdges`); `predicate_` is a plain
// IRI (property paths and variable predicates disqualify the view from
// pattern-based rewriting, see `checkQueryPatternRewriteEligibility`).
struct PatternEdge {
  Variable subject_;
  std::string predicate_;
  Variable object_;
};

// The join pattern extracted from a materialized view's defining query: all of
// its triples, viewed as the edges of a small labeled graph over the view's
// variables. This generalizes the previous special cases of a "chain" (exactly
// two edges in sequence) and a "star" (all edges sharing one subject) to an
// arbitrary pattern of simple-IRI-predicate triples: query rewriting looks for
// an embedding of this graph into (a subset of) the triples of the user's
// query, i.e. a subgraph isomorphism from the view's pattern into the query.
struct ViewPattern {
  std::vector<PatternEdge> edges_;
  ViewPtr view_;
};

// A single candidate assignment while searching for an embedding of a
// `ViewPattern` into the triples of a user query. `assignment_` maps the
// view's pattern variables to the query-side `TripleComponent` (a variable or
// a fixed value) they have been matched to so far; `coveredTriples_` is the
// (in-order) list of query triple indices used by the match.
struct PatternMatchState {
  ad_utility::HashMap<Variable, TripleComponent> assignment_;
  std::vector<size_t> coveredTriples_;
};

// Query-side triples with a simple IRI predicate, grouped by that predicate,
// for fast lookup of match candidates for a given pattern edge.
using TriplesByPredicate =
    ad_utility::HashMap<std::string, std::vector<size_t>>;

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
  // Cache for predicates appearing in a materialized view. The `ViewPtr`s are
  // kept sorted. Used to quickly find candidate views for a query (views that
  // could not possibly match are ruled out without running the more expensive
  // pattern matching below), and to reject a candidate whose pattern uses a
  // predicate absent from the query without a full search.
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
  // per triple) from the triples of its defining query. Returns `nullopt` if
  // any triple disqualifies the view from pattern-based rewriting: a
  // non-simple predicate (property path or variable predicate), a fixed
  // subject/object, or a subject/object that is not a column of the view (not
  // selected, or removed by aggregation).
  static std::optional<std::vector<PatternEdge>> buildPatternEdges(
      const ViewPtr& view, const std::vector<SparqlTriple>& triples);

  // Search for all embeddings of `pattern` into `triples` (using
  // `triplesByPredicate` for fast candidate lookup) and add a
  // `MaterializedViewJoinReplacement` for each valid one (i.e. one where fixed
  // values from the query only end up on a legal prefix of the view's columns,
  // see `isLegalFixedValuePrefix`) to `result`.
  void matchPattern(QueryExecutionContext* qec, const ViewPattern& pattern,
                    const parsedQuery::BasicGraphPattern& triples,
                    const TriplesByPredicate& triplesByPredicate,
                    std::vector<MaterializedViewJoinReplacement>& result) const;
};

// Helper that filters the graph patterns of a parsed query using
// `BasicGraphPatternInvariantTo`. For details, see the documentation for this
// helper.
std::vector<parsedQuery::GraphPatternOperation> graphPatternInvariantFilter(
    const ParsedQuery& parsed);

// Result of `checkQueryPatternRewriteEligibility`: either the reason why
// `parsed` is not eligible for pattern-based rewriting, or (if
// `ignoreReason_` is `std::nullopt`) the triples of its single
// `BasicGraphPattern` for further analysis.
struct QueryPatternRewriteEligibility {
  std::optional<std::string> ignoreReason_;
  std::vector<SparqlTriple> triples_;
};

// Helper for `QueryPatternCache::analyzeView` that checks whether a view's
// defining query even has a shape that pattern-based rewriting could apply
// to, before the actual (more expensive) triple-level analysis:
// no aggregation, no top-level FILTER/VALUES/DISTINCT/REDUCED/LIMIT/OFFSET
// that would restrict the on-disk rows, and exactly one `BasicGraphPattern`
// with at least one triple (after skipping invariant patterns like `BIND`).
QueryPatternRewriteEligibility checkQueryPatternRewriteEligibility(
    const ParsedQuery& parsed);

// Hash map for the `BIND`-to-column map.
using BindExpressionAndTargetCol = ad_utility::HashMap<std::string, size_t>;

// Extract all `BIND` statements from a `ParsedQuery` and create a hash map
// mapping `BIND` expression cache keys to target variable column index.
BindExpressionAndTargetCol extractBindExpressions(
    const ParsedQuery& parsed, const VariableToColumnMap& varToColMap);

}  // namespace materializedViewsQueryAnalysis

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
