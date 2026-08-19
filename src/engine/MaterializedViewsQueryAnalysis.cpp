// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/MaterializedViewsQueryAnalysis.h"

#include <algorithm>
#include <optional>
#include <variant>

#include "backports/algorithm.h"
#include "engine/IndexScan.h"
#include "engine/MaterializedViews.h"
#include "engine/VariableToColumnMap.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/VariantRangeFilter.h"

namespace materializedViewsQueryAnalysis {

// _____________________________________________________________________________
std::optional<std::vector<PatternEdge>> QueryPatternCache::buildPatternEdges(
    const ViewPtr& view, const std::vector<SparqlTriple>& triples) {
  const auto& viewCols = view->variableToColumnMap();
  std::vector<PatternEdge> edges;
  edges.reserve(triples.size());
  for (const auto& triple : triples) {
    // Variables as predicate and sequence property paths are not supported by
    // query rewriting; the latter are expected to be replaced by joins during
    // earlier stages of query planning.
    auto predicate = triple.getSimplePredicate();
    if (!predicate.has_value() || !triple.s_.isVariable() ||
        !triple.o_.isVariable()) {
      return std::nullopt;
    }
    auto subject = triple.s_.getVariable();
    auto object = triple.o_.getVariable();
    // All three variables must actually be columns of the view (e.g. this is
    // not the case if they do not appear in the `SELECT` clause, or have been
    // removed by aggregation).
    if (!viewCols.contains(subject) || !viewCols.contains(object)) {
      return std::nullopt;
    }
    edges.push_back({std::move(subject), std::string{predicate.value()},
                     std::move(object)});
  }
  return edges;
}

// _____________________________________________________________________________
namespace {

// Check whether `boundColumns` (the set of view column indices that a match
// would bind to a fixed value from the user's query) forms a legal prefix for
// a single SPO-sorted view permutation: fixed values are only allowed in the
// arrangements subject, subject+predicate, subject+predicate+object (see
// `MaterializedView::throwIfColumnsHaveIllegalFixedValues`). Columns beyond
// index 2 are payload-only and can never be fixed.
bool isLegalFixedValuePrefix(const ad_utility::HashSet<size_t>& boundColumns) {
  for (size_t col : boundColumns) {
    if (col > 2) {
      return false;
    }
  }
  for (size_t col : {size_t{1}, size_t{2}}) {
    if (boundColumns.contains(col) && !boundColumns.contains(col - 1)) {
      return false;
    }
  }
  return true;
}

// Try to assign `viewVar` to `queryNode` inside `state`, respecting
// injectivity (two distinct view variables must not be matched to the same
// query-side variable or fixed value). Returns `false` (leaving `state`
// unmodified) if `viewVar` is already assigned to a different node, or if
// `queryNode` is already used by a different view variable.
bool tryAssign(PatternMatchState& state, const Variable& viewVar,
               const TripleComponent& queryNode) {
  auto it = state.assignment_.find(viewVar);
  if (it != state.assignment_.end()) {
    return it->second == queryNode;
  }
  for (const auto& [otherVar, otherNode] : state.assignment_) {
    if (otherNode == queryNode) {
      return false;
    }
  }
  state.assignment_.emplace(viewVar, queryNode);
  return true;
}

// Recursively extend `state` (which already covers `edges[0, edgeIdx)`) by
// matching `edges[edgeIdx]` against every not-yet-used candidate query triple
// with the same predicate, and so on for the remaining edges. Every full
// match (one for each edge) is appended to `results`. This is a plain
// backtracking search (in the style of VF2's core loop): at the scale of a
// materialized view's defining query and a single SPARQL basic graph pattern
// (both realistically well under a hundred triples), this is fast enough
// without the extra preprocessing that large-graph subgraph-isomorphism
// algorithms (VF3, RI, ...) rely on to pay for themselves.
//
// `stepsRemaining` bounds the total number of candidate assignments tried
// across the whole search (not just this call), so that a pathological case
// (e.g. many query triples sharing a predicate that a view's pattern also
// uses repeatedly, which can blow up combinatorially) aborts the search for
// this view instead of stalling query planning. Once it reaches `0`, no
// further candidates are tried and the function returns; `results` may then
// be missing some (or all) valid embeddings for this view.
void extendMatch(const std::vector<PatternEdge>& edges, size_t edgeIdx,
                 const parsedQuery::BasicGraphPattern& triples,
                 const TriplesByPredicate& triplesByPredicate,
                 ad_utility::HashSet<size_t>& usedTriples,
                 PatternMatchState& state,
                 std::vector<PatternMatchState>& results,
                 size_t& stepsRemaining) {
  if (edgeIdx == edges.size()) {
    results.push_back(state);
    return;
  }
  const auto& edge = edges[edgeIdx];
  auto it = triplesByPredicate.find(edge.predicate_);
  if (it == triplesByPredicate.end()) {
    return;
  }
  for (size_t tripleIdx : it->second) {
    if (stepsRemaining == 0) {
      return;
    }
    --stepsRemaining;
    if (usedTriples.contains(tripleIdx)) {
      continue;
    }
    const auto& triple = triples._triples.at(tripleIdx);
    auto assignmentBackup = state.assignment_;
    if (tryAssign(state, edge.subject_, triple.s_) &&
        tryAssign(state, edge.object_, triple.o_)) {
      usedTriples.insert(tripleIdx);
      state.coveredTriples_.push_back(tripleIdx);

      extendMatch(edges, edgeIdx + 1, triples, triplesByPredicate, usedTriples,
                  state, results, stepsRemaining);

      state.coveredTriples_.pop_back();
      usedTriples.erase(tripleIdx);
    }
    state.assignment_ = assignmentBackup;
  }
}

}  // namespace

// _____________________________________________________________________________
void QueryPatternCache::matchPattern(
    QueryExecutionContext* qec, const ViewPattern& pattern,
    const parsedQuery::BasicGraphPattern& triples,
    const TriplesByPredicate& triplesByPredicate,
    std::vector<MaterializedViewJoinReplacement>& result) const {
  // Quick reject: every predicate used by the pattern must appear at least
  // once among the query's triples, otherwise no embedding can possibly
  // exist and the (more expensive) search below can be skipped entirely.
  for (const auto& edge : pattern.edges_) {
    if (!triplesByPredicate.contains(edge.predicate_)) {
      return;
    }
  }

  std::vector<PatternMatchState> matches;
  ad_utility::HashSet<size_t> usedTriples;
  PatternMatchState state;
  size_t stepsRemaining = getRuntimeParameter<
      &RuntimeParameters::materializedViewPatternMatchBudget_>();
  extendMatch(pattern.edges_, 0, triples, triplesByPredicate, usedTriples,
              state, matches, stepsRemaining);
  if (stepsRemaining == 0) {
    AD_LOG_WARN << "Pattern matching for materialized view '"
                << pattern.view_->name()
                << "' exceeded the `materialized-view-pattern-match-budget`; "
                   "some applicable rewrites using this view may have been "
                   "missed for this query."
                << std::endl;
  }

  const auto& viewCols = pattern.view_->variableToColumnMap();
  for (auto& match : matches) {
    ad_utility::HashSet<size_t> boundColumns;
    for (const auto& [viewVar, node] : match.assignment_) {
      if (!node.isVariable()) {
        boundColumns.insert(viewCols.at(viewVar).columnIndex_);
      }
    }
    if (!isLegalFixedValuePrefix(boundColumns)) {
      continue;
    }

    parsedQuery::MaterializedViewQuery::RequestedColumns cols;
    for (auto& [viewVar, node] : match.assignment_) {
      cols.insert({viewVar, std::move(node)});
    }
    result.push_back(
        {pattern.view_->makeIndexScan(
             qec, parsedQuery::MaterializedViewQuery{pattern.view_->name(),
                                                     std::move(cols)}),
         std::move(match.coveredTriples_)});
  }
}

// _____________________________________________________________________________
std::vector<MaterializedViewJoinReplacement>
QueryPatternCache::makeJoinReplacementIndexScans(
    QueryExecutionContext* qec,
    const parsedQuery::BasicGraphPattern& triples) const {
  std::vector<MaterializedViewJoinReplacement> result;
  if (patterns_.empty()) {
    return result;
  }

  // Group the query's triples by predicate and collect all views that could
  // possibly match (share at least one predicate with the query). Only
  // triples with a simple IRI predicate that is covered by at least one
  // loaded view's pattern are relevant.
  TriplesByPredicate triplesByPredicate;
  ad_utility::HashSet<ViewPtr> candidateViews;
  for (const auto& [tripleIdx, triple] :
       ::ranges::views::enumerate(triples._triples)) {
    auto iri = triple.getSimplePredicate();
    if (!iri.has_value() || !predicateInView_.contains(iri.value())) {
      continue;
    }
    triplesByPredicate[std::string{iri.value()}].push_back(tripleIdx);
    ql::ranges::copy(predicateInView_.at(iri.value()),
                     std::inserter(candidateViews, candidateViews.end()));
  }

  for (const auto& view : candidateViews) {
    matchPattern(qec, patterns_.at(view), triples, triplesByPredicate, result);
  }
  return result;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeView(ViewPtr view, QueryExecutionContext* qec) {
  auto explainIgnore = [&](const std::string& reason) {
    AD_LOG_INFO << "Materialized view '" << view->name()
                << "' will not be added to the query pattern cache for "
                   "pattern-based query rewriting. Reason: "
                << reason << "." << std::endl;
  };

  const auto& parsed = view->parsedQuery();
  if (!parsed.has_value()) {
    explainIgnore(
        "The view was built without remembering the original query string.");
    return false;
  }

  // Save the cache key for this view: once in full for matching the entire
  // unchanged view query, and once with invariant patterns (such as `BIND`s)
  // removed for matching queries that do not contain all of these patterns.
  auto [full, withoutInvariant] = view->computeCacheKey(qec);
  auto insert = [&](auto& cacheKeyAndCol) {
    if (!cacheKeyAndCol.has_value()) {
      return false;
    }
    auto [it, inserted] = byCacheKey_.insert(
        {std::move(cacheKeyAndCol.value().cacheKey_),
         std::make_shared<ByCacheKeyInfo>(
             view, std::move(cacheKeyAndCol.value().columnMapping_))});
    // If `inserted` is `false` because the entry already belongs to `view`
    // itself (its "full" and "without invariants" cache keys coincide, e.g.
    // because the view has no `BIND` to strip), this is expected and not a
    // collision worth logging.
    if (!inserted && it->second->view_ != view) {
      AD_LOG_INFO << "Materialized view '" << view->name()
                  << "' has the same cache key as the already loaded view '"
                  << it->second->view_->name()
                  << "'. Only the latter can be matched by cache key."
                  << std::endl;
    }
    return inserted;
  };
  // Not `||`: both calls must always be evaluated.
  bool cacheKeyAdded = insert(full);
  cacheKeyAdded = insert(withoutInvariant) || cacheKeyAdded;

  auto eligibility = checkQueryPatternRewriteEligibility(parsed.value());
  if (eligibility.ignoreReason_.has_value()) {
    explainIgnore(eligibility.ignoreReason_.value());
    return cacheKeyAdded;
  }
  const auto& triples = eligibility.triples_;
  bool patternFound = false;

  // A pattern needs at least two triples: a single triple has no join for
  // pattern-based rewriting to eliminate (a plain `IndexScan` on the main
  // index is already as good as reading the view).
  if (triples.size() >= 2) {
    if (auto edges = buildPatternEdges(view, triples)) {
      patterns_.insert({view, ViewPattern{std::move(edges.value()), view}});
      patternFound = true;
    }
  }

  // Remember predicates that appear in certain views, only if any pattern is
  // detected.
  if (patternFound) {
    for (const auto& triple : triples) {
      auto predicate = triple.getSimplePredicate();
      if (predicate.has_value()) {
        auto& vec = predicateInView_[predicate.value()];
        // Sort-preserving insert into the vector s.t. we can later merge
        // multiple vectors of views.
        auto it = std::lower_bound(vec.begin(), vec.end(), view);
        vec.insert(it, view);
      }
    }
  }

  if (!patternFound) {
    explainIgnore(
        "No supported query pattern for rewriting joins was found (this does "
        "not affect cache-key based rewriting)");
  }

  return patternFound || cacheKeyAdded;
}

// _____________________________________________________________________________
std::vector<parsedQuery::GraphPatternOperation> graphPatternInvariantFilter(
    const ParsedQuery& parsed) {
  BasicGraphPatternsInvariantTo invariantCheck{parsed._rootGraphPattern};

  // Filter out graph patterns that do not change the result of the basic graph
  // pattern analyzed.
  return parsed._rootGraphPattern._graphPatterns |
         ql::views::filter([&](const auto& pattern) {
           return !pattern.visit(invariantCheck);
         }) |
         ::ranges::to<std::vector>();
}

// _____________________________________________________________________________
QueryPatternRewriteEligibility checkQueryPatternRewriteEligibility(
    const ParsedQuery& parsed) {
  if (parsed.isAggregatingQuery()) {
    return {
        "The view's query aggregates (GROUP BY, either explicit or implicit "
        "via an aggregate expression in the SELECT clause)",
        {}};
  }

  // A top-level `FILTER` restricts which rows end up on disk, but (unlike the
  // triples analyzed below) is not part of `_graphPatterns` and would
  // otherwise go unnoticed by `graphPatternInvariantFilter`.
  if (!parsed._rootGraphPattern._filters.empty()) {
    return {"The view's query has a top-level FILTER", {}};
  }

  // A trailing `VALUES` clause also restricts the on-disk rows and is stored
  // separately from `_rootGraphPattern`, so it needs an explicit check too.
  if (parsed.postQueryValuesClause_.has_value()) {
    return {"The view's query has a trailing VALUES clause", {}};
  }

  // `DISTINCT`/`REDUCED` change the cardinality of the result and thus of the
  // join, which the pattern-based rewriting below does not account for.
  const auto& selectClause = parsed.selectClause();
  if (selectClause.distinct_ || selectClause.reduced_) {
    return {"The view's query uses DISTINCT or REDUCED", {}};
  }

  // A `LIMIT`/`OFFSET` restricts the view to an (order-dependent, arbitrary)
  // subset of the join's rows, which must not be treated as a complete source
  // for pattern-based rewriting of unrelated queries.
  if (!parsed._limitOffset.isUnconstrained()) {
    return {"The view's query has a LIMIT or OFFSET clause", {}};
  }

  auto graphPatternsFiltered = graphPatternInvariantFilter(parsed);
  if (graphPatternsFiltered.size() != 1) {
    return {
        "The view has more than one graph pattern (even after skipping "
        "ignored patterns)",
        {}};
  }
  auto& graphPattern = graphPatternsFiltered.at(0);
  if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(graphPattern)) {
    return {"The graph pattern is not a basic set of triples", {}};
  }
  // TODO<ullingerc> Property path is stored as a single predicate here.
  auto triples = std::move(graphPattern.getBasic()._triples);
  if (triples.empty()) {
    return {"The query body is empty", {}};
  }

  return {std::nullopt, std::move(triples)};
}

// _____________________________________________________________________________
void QueryPatternCache::removeView(ViewPtr view) {
  // Remove `view` from pattern cache.
  patterns_.erase(view);

  // Remove `view` from predicate cache.
  for (auto& [pred, views] : predicateInView_) {
    ql::erase_if(views, [&view](ViewPtr pView) { return pView == view; });
  }

  // Remove `view` from cache key hash map. We use `absl::erase_if` here as it
  // works natively with our hash map unlike `ql::erase_if`.
  absl::erase_if(byCacheKey_, [&view](const auto& pair) {
    AD_CORRECTNESS_CHECK(pair.second != nullptr);
    return pair.second->view_ == view;
  });
}

// _____________________________________________________________________________
BindExpressionAndTargetCol extractBindExpressions(
    const ParsedQuery& parsed, const VariableToColumnMap& varToColMap) {
  BindExpressionAndTargetCol map;

  // Iterate over all `BIND`s in the parsed query and add them to the map.
  for (const auto& bind :
       ad_utility::filterRangeOfVariantsByType<parsedQuery::Bind>(
           parsed._rootGraphPattern._graphPatterns)) {
    // Check that the `VariableToColumnMap` covers both all variables from the
    // `BIND` expression as well as the target variable.
    // IMPORTANT: This is not the `VariableToColumnMap` that we would get from
    // the parsed query, but the one which represents the final permuted column
    // ordering in the view.
    bool exprVarsCovered = ql::ranges::all_of(
        bind._expression.containedVariables(),
        [&varToColMap](const auto* v) { return varToColMap.contains(*v); });
    if (!exprVarsCovered || !varToColMap.contains(bind._target)) {
      continue;
    }

    // Store the mapping from cache key to target variable column index.
    // Note that while expression cache keys are not stable between compilers
    // this is still fine, because we are computing the key while loading a view
    // and thus will use it only within the same process.
    map.insert({bind._expression.getCacheKey(varToColMap),
                varToColMap.at(bind._target).columnIndex_});
  }
  return map;
}

// _____________________________________________________________________________
ByCacheKeyInfoPtr QueryPatternCache::lookupByCacheKey(
    const std::string& cacheKey) const {
  if (auto info = ad_utility::findOptional(byCacheKey_, cacheKey)) {
    return info.value();
  }
  return nullptr;
}

}  // namespace materializedViewsQueryAnalysis
