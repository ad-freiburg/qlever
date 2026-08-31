// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/MaterializedViewsQueryAnalysis.h"

#include <algorithm>
#include <map>
#include <optional>
#include <variant>

#include "backports/algorithm.h"
#include "engine/IndexScan.h"
#include "engine/MaterializedViews.h"
#include "engine/MaterializedViewsPatternMatcher.h"
#include "engine/VariableToColumnMap.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/VariantRangeFilter.h"

namespace materializedViewsQueryAnalysis {

namespace {

// BFS to determine if the `edges` given constitute a connected component.
std::optional<std::vector<size_t>> connectedOrder(
    const std::vector<PatternEdge>& edges) {
  AD_CORRECTNESS_CHECK(!edges.empty());
  // Collect all edges containing each variable.
  ad_utility::HashMap<Variable, std::vector<size_t>> edgesByVariable;
  for (size_t i = 0; i < edges.size(); ++i) {
    for (const auto& side : {edges[i].s_, edges[i].o_}) {
      if (side.isVariable()) {
        edgesByVariable[side.getVariable()].push_back(i);
      }
    }
  }

  ad_utility::HashSet<size_t> reached{0};
  std::vector<size_t> order{0};
  // `order` is used as the BFS queue: indices not yet processed for
  // neighbors are simply the ones not yet reached by `idx`. Therefore this
  // cannot be `for (auto current : order)`.
  for (size_t idx = 0; idx < order.size(); ++idx) {
    size_t current = order[idx];
    for (const auto& side : {edges[current].s_, edges[current].o_}) {
      if (!side.isVariable()) {
        continue;
      }
      for (size_t neighbor : edgesByVariable.at(side.getVariable())) {
        if (reached.insert(neighbor).second) {
          order.push_back(neighbor);
        }
      }
    }
  }
  if (order.size() != edges.size()) {
    return std::nullopt;
  }
  return order;
}

}  // namespace

// _____________________________________________________________________________
PatternMatcherLimits PatternMatcherLimits::perViewShare(size_t numViews) const {
  AD_CORRECTNESS_CHECK(numViews > 0);
  constexpr size_t minNumAssignmentsPerView = 1'000;
  constexpr size_t minNumReplacementPlansPerView = 15;
  return {
      std::max(minNumAssignmentsPerView, numAssignments_ / numViews),
      std::max(minNumReplacementPlansPerView, numReplacementPlans_ / numViews)};
}

// _____________________________________________________________________________
PatternMatcherLimits PatternMatcherLimits::requestBounded(
    PatternMatcherLimits requestedAmount) const {
  return {std::min(numAssignments_, requestedAmount.numAssignments_),
          std::min(numReplacementPlans_, requestedAmount.numReplacementPlans_)};
}

// _____________________________________________________________________________
void PatternMatcherLimits::subtract(PatternMatcherLimits used) {
  numAssignments_ -= std::min(numAssignments_, used.numAssignments_);
  numReplacementPlans_ -=
      std::min(numReplacementPlans_, used.numReplacementPlans_);
}

// _____________________________________________________________________________
std::optional<std::vector<PatternEdge>> QueryPatternCache::buildPatternEdges(
    const ViewPtr& view, const std::vector<SparqlTriple>& triples) {
  const auto& viewCols = view->variableToColumnMap();
  std::vector<PatternEdge> edges;
  edges.reserve(triples.size());

  // Check each triple for unsupported cases and fill `edges`.
  for (const auto& triple : triples) {
    // Variable predicates and non-trivial property paths are unsupported.
    auto predicate = triple.getSimplePredicate();
    if (!predicate.has_value()) {
      return std::nullopt;
    }

    // Full-text pseudo-predicates are unsupported.
    if (isFullTextPseudoPredicate(predicate.value())) {
      return std::nullopt;
    }

    // At least one of subject or object must be a variable, or the triples can
    // never be a connected component.
    if (!triple.s_.isVariable() && !triple.o_.isVariable()) {
      return std::nullopt;
    }

    // A variable subject or object must be a column of the view.
    auto isUsableColumn = [&viewCols](const TripleComponent& side) {
      return !side.isVariable() || viewCols.contains(side.getVariable());
    };
    if (!isUsableColumn(triple.s_) || !isUsableColumn(triple.o_)) {
      return std::nullopt;
    }
    edges.push_back({triple.s_, std::string{predicate.value()}, triple.o_});
  }

  // Check that the edges are a connected component.
  auto order = connectedOrder(edges);
  if (!order.has_value()) {
    return std::nullopt;
  }
  edges =
      ql::views::transform(order.value(),
                           [&edges](size_t i) { return std::move(edges[i]); }) |
      ::ranges::to<std::vector>();

  // The view's first column must be assigned during matching (for example, it
  // may not come from an invariant, stripped-away `BIND`).
  auto col0 = ql::ranges::find_if(viewCols, [](const auto& varAndCol) {
    return varAndCol.second.columnIndex_ == 0;
  });
  AD_CORRECTNESS_CHECK(col0 != viewCols.end());
  auto isCol0 = [&col0](const TripleComponent& side) {
    return side.isVariable() && side.getVariable() == col0->first;
  };
  bool col0InPattern = ql::ranges::any_of(edges, [&isCol0](const auto& edge) {
    return isCol0(edge.s_) || isCol0(edge.o_);
  });
  if (!col0InPattern) {
    return std::nullopt;
  }
  return edges;
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

  // A limit of `0` assignments disables pattern-based rewriting entirely.
  size_t totalNumAssignments = getRuntimeParameter<
      &RuntimeParameters::materializedViewPatternMatchNumAssignments_>();
  if (totalNumAssignments == 0) {
    return result;
  }
  size_t totalNumReplacementPlans = getRuntimeParameter<
      &RuntimeParameters::materializedViewPatternMatchNumReplacementPlans_>();

  // We use a 64-bit bitmask of triple indices, so more than 64 triples are not
  // supported.
  if (triples._triples.size() > 64) {
    return result;
  }

  // Group the query triples by predicate and collect the views sharing a
  // predicate with the query. Keyed by name, so that the sharing of the limits
  // pool below is deterministic (a hash set of `shared_ptr`s is not).
  TriplesByPredicate triplesByPredicate;
  std::map<std::string_view, ViewPtr> candidateViews;
  for (const auto& [tripleIdx, triple] :
       ::ranges::views::enumerate(triples._triples)) {
    auto iri = triple.getSimplePredicate();
    if (!iri.has_value()) {
      continue;
    }
    auto views = ad_utility::findOptional(predicateInView_, iri.value());
    if (!views.has_value()) {
      continue;
    }
    triplesByPredicate[iri.value()] |= (uint64_t{1} << tripleIdx);
    for (const auto& view : views.value()) {
      candidateViews.emplace(view->name(), view);
    }
  }
  if (candidateViews.empty()) {
    return result;
  }

  // Match all `candidateViews` against the query, sharing one pool of limits
  // across them.
  PatternMatcherLimits remaining{totalNumAssignments, totalNumReplacementPlans};
  const PatternMatcherLimits share =
      remaining.perViewShare(candidateViews.size());
  for (const auto& [name, view] : candidateViews) {
    if (remaining.isExhausted()) {
      break;
    }
    remaining.subtract(PatternMatcher::findReplacementPlans(
                           patterns_.at(view), triples, triplesByPredicate, qec,
                           remaining.requestBounded(share), result)
                           .used_);
  }
  // Only the exhaustion of the shared pool is worth a warning: a single view
  // hitting its share only means the other views get their turn.
  if (remaining.isExhausted()) {
    AD_LOG_WARN
        << "Pattern matching for materialized views hit the `"
        << (remaining.numAssignments_ == 0
                ? "materialized-view-pattern-match-num-assignments"
                : "materialized-view-pattern-match-num-replacement-plans")
        << "` cap; some applicable rewrites may have been missed for this "
           "query."
        << std::endl;
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
    // NOTE: `ByCacheKeyInfo` is an aggregate, and `make_shared` initializes
    // with parentheses, which only works for aggregates since C++20. The
    // explicit `ByCacheKeyInfo{...}` is therefore required for C++17.
    auto [it, inserted] = byCacheKey_.insert(
        {std::move(cacheKeyAndCol.value().cacheKey_),
         std::make_shared<ByCacheKeyInfo>(ByCacheKeyInfo{
             view, std::move(cacheKeyAndCol.value().columnMapping_)})});
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

  auto triplesForRewrite = getTriplesForPatternRewrite(parsed.value());
  if (std::holds_alternative<RewriteIgnoreReason>(triplesForRewrite)) {
    explainIgnore(std::get<RewriteIgnoreReason>(triplesForRewrite));
    return cacheKeyAdded;
  }
  const auto& triples = std::get<std::vector<SparqlTriple>>(triplesForRewrite);
  bool patternFound = false;

  // A single triple has no join to eliminate, so a pattern needs at least two.
  if (triples.size() >= 2) {
    if (auto edges = buildPatternEdges(view, triples)) {
      patterns_.insert({view, ViewPattern{std::move(edges.value()), view}});
      patternFound = true;
    }
  }

  // A view using the same predicate twice (e.g. two star arms) is listed
  // twice here; harmless, since `removeView` erases all copies.
  if (patternFound) {
    for (const auto& triple : triples) {
      auto predicate = triple.getSimplePredicate();
      if (predicate.has_value()) {
        predicateInView_[predicate.value()].push_back(view);
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
std::variant<RewriteIgnoreReason, std::vector<SparqlTriple>>
getTriplesForPatternRewrite(const ParsedQuery& parsed) {
  if (parsed.isAggregatingQuery()) {
    return "The view's query aggregates (GROUP BY, either explicit or "
           "implicit via an aggregate expression in the SELECT clause)";
  }

  // A top-level `FILTER` restricts which rows end up on disk, but (unlike the
  // triples analyzed below) is not part of `_graphPatterns` and would
  // otherwise go unnoticed by `graphPatternInvariantFilter`.
  if (!parsed._rootGraphPattern._filters.empty()) {
    return "The view's query has a top-level FILTER";
  }

  // A trailing `VALUES` clause also restricts the on-disk rows and is stored
  // separately from `_rootGraphPattern`, so it needs an explicit check too.
  if (parsed.postQueryValuesClause_.has_value()) {
    return "The view's query has a trailing VALUES clause";
  }

  // `DISTINCT`/`REDUCED` change the cardinality of the result and thus of the
  // join, which the pattern-based rewriting below does not account for.
  const auto& selectClause = parsed.selectClause();
  if (selectClause.distinct_ || selectClause.reduced_) {
    return "The view's query uses DISTINCT or REDUCED";
  }

  // A `LIMIT`/`OFFSET` restricts the view to an (order-dependent, arbitrary)
  // subset of the join's rows, which must not be treated as a complete source
  // for pattern-based rewriting of unrelated queries.
  //
  // NOTE: `isUnconstrained()` deliberately ignores a `TEXTLIMIT` clause, so a
  // view with text-search triples and a `TEXTLIMIT` is not rejected here.
  if (!parsed._limitOffset.isUnconstrained()) {
    return "The view's query has a LIMIT or OFFSET clause";
  }

  // A `FROM` or `FROM NAMED` clause changes the dataset against which the
  // query is evaluated (with only `FROM NAMED`, the active default graph is
  // even empty) and thus also restricts which rows end up on disk.
  if (!parsed.datasetClauses_.isUnconstrainedOrWithClause()) {
    return "The view's query has a FROM or FROM NAMED clause";
  }

  auto graphPatternsFiltered = graphPatternInvariantFilter(parsed);
  if (graphPatternsFiltered.size() != 1) {
    return "The view has more than one graph pattern (even after skipping "
           "ignored patterns)";
  }
  auto& graphPattern = graphPatternsFiltered.at(0);
  if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(graphPattern)) {
    return "The graph pattern is not a basic set of triples";
  }
  // TODO<ullingerc> Property path is stored as a single predicate here.
  auto triples = std::move(graphPattern.getBasic()._triples);
  if (triples.empty()) {
    return "The query body is empty";
  }

  return triples;
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
