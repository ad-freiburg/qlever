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

namespace {

// Whether `edges` (the view's pattern graph) is connected, i.e. every edge is
// reachable from `edges[0]` via a shared variable. `edges` must be non-empty.
// Rejected here rather than left to the query planner: its DP table is built
// per connected component of the *query's* triple graph, so a replacement
// spanning more than one component could never be selected regardless of
// cost. Fixed edge sides (see `PatternEdge`) don't connect anything.
bool isConnected(const std::vector<PatternEdge>& edges) {
  AD_CORRECTNESS_CHECK(!edges.empty());
  ad_utility::HashMap<Variable, std::vector<size_t>> edgesByVariable;
  for (size_t i = 0; i < edges.size(); ++i) {
    for (const auto& side : {edges[i].subject_, edges[i].object_}) {
      if (side.isVariable()) {
        edgesByVariable[side.getVariable()].push_back(i);
      }
    }
  }

  ad_utility::HashSet<size_t> reached{0};
  std::vector<size_t> toVisit{0};
  while (!toVisit.empty()) {
    size_t current = toVisit.back();
    toVisit.pop_back();
    for (const auto& side : {edges[current].subject_, edges[current].object_}) {
      if (!side.isVariable()) {
        continue;
      }
      for (size_t neighbor : edgesByVariable.at(side.getVariable())) {
        if (reached.insert(neighbor).second) {
          toVisit.push_back(neighbor);
        }
      }
    }
  }
  return reached.size() == edges.size();
}

}  // namespace

// _____________________________________________________________________________
std::optional<std::vector<PatternEdge>> QueryPatternCache::buildPatternEdges(
    const ViewPtr& view, const std::vector<SparqlTriple>& triples) {
  const auto& viewCols = view->variableToColumnMap();
  std::vector<PatternEdge> edges;
  edges.reserve(triples.size());
  for (const auto& triple : triples) {
    // Variable predicates and non-trivial property paths are unsupported.
    auto predicate = triple.getSimplePredicate();
    if (!predicate.has_value()) {
      return std::nullopt;
    }
    // At least one endpoint must be a variable, or the edge can't connect to
    // the rest of the pattern (see `isConnected`).
    if (!triple.s_.isVariable() && !triple.o_.isVariable()) {
      return std::nullopt;
    }
    // A variable endpoint must be a column of the view.
    auto isUsableColumn = [&viewCols](const TripleComponent& side) {
      return !side.isVariable() || viewCols.contains(side.getVariable());
    };
    if (!isUsableColumn(triple.s_) || !isUsableColumn(triple.o_)) {
      return std::nullopt;
    }
    edges.push_back({triple.s_, std::string{predicate.value()}, triple.o_});
  }
  if (!isConnected(edges)) {
    return std::nullopt;
  }
  return edges;
}

// _____________________________________________________________________________
namespace {

// Whether `boundColumnsMask` (bit `i` set iff view column `i` is bound to a
// fixed value from the query) is a legal prefix for a single SPO-sorted view
// permutation: only subject, subject+predicate, or subject+predicate+object
// (`0b001`/`0b011`/`0b111`, or `0b000`) may be fixed.
bool isLegalFixedValuePrefix(unsigned boundColumnsMask) {
  return boundColumnsMask == 0b000u || boundColumnsMask == 0b001u ||
         boundColumnsMask == 0b011u || boundColumnsMask == 0b111u;
}

// Match `viewSide` against `queryNode` inside `state`. A fixed `viewSide` is
// a plain equality check (not bound; it's not a view column). Otherwise
// `viewSide` is a view variable: enforces injectivity (no two view variables
// map to the same query node) and prunes bindings that can never satisfy
// `isLegalFixedValuePrefix` later (a smaller-column variable already bound to
// a query *variable* rules out fixing this one). Returns `false`, leaving
// `state` unmodified, on rejection.
bool tryAssign(PatternMatchState& state, const TripleComponent& viewSide,
               const TripleComponent& queryNode,
               const VariableToColumnMap& viewCols) {
  if (!viewSide.isVariable()) {
    return queryNode == viewSide;
  }
  const Variable& viewVar = viewSide.getVariable();
  auto it = state.assignment_.find(viewVar);
  if (it != state.assignment_.end()) {
    return it->second == queryNode;
  }
  size_t col = queryNode.isVariable() ? 0 : viewCols.at(viewVar).columnIndex_;
  for (const auto& [otherVar, otherNode] : state.assignment_) {
    if (otherNode == queryNode) {
      return false;
    }
    if (!queryNode.isVariable() && otherNode.isVariable() &&
        viewCols.at(otherVar).columnIndex_ < col) {
      return false;
    }
  }
  state.assignment_.emplace(viewVar, queryNode);
  return true;
}

// Whether `tryAssign(state, viewSide, ...)` would add a new binding (that
// `undoAssign` then needs to remove on backtrack). Must be called before
// `tryAssign`, which may itself insert that binding.
bool isNewBinding(const PatternMatchState& state,
                  const TripleComponent& viewSide) {
  return viewSide.isVariable() &&
         !state.assignment_.contains(viewSide.getVariable());
}

// Reverses `tryAssign(state, viewSide, ...)` if it added a new binding
// (`wasNew`, from `isNewBinding`); otherwise a no-op.
void undoAssign(PatternMatchState& state, const TripleComponent& viewSide,
                bool wasNew) {
  if (wasNew) {
    state.assignment_.erase(viewSide.getVariable());
  }
}

// Recursively extend `state` (which already covers `edges[0, edgeIdx)`) by
// matching `edges[edgeIdx]` against each of its candidate query triples
// (`candidatesByEdge[edgeIdx]`, precomputed per predicate in `matchPattern`),
// then recursing into the remaining edges. Every full match is appended to
// `results`. Plain VF2-style backtracking: `undoAssign` only removes the
// bindings this step added, and "already used" is a linear scan of
// `state.coveredTriples_` (both O(pattern size), cheaper than hashing at this
// scale).
//
// `stepsRemaining` caps the total number of candidates tried across the whole
// search, guarding against combinatorial blowup (e.g. a predicate repeated
// many times on both sides). On reaching 0, `truncated` is set and the search
// stops; `results` may then be incomplete.
void extendMatch(
    const std::vector<PatternEdge>& edges,
    const std::vector<const std::vector<size_t>*>& candidatesByEdge,
    size_t edgeIdx, const parsedQuery::BasicGraphPattern& triples,
    const VariableToColumnMap& viewCols, PatternMatchState& state,
    std::vector<PatternMatchState>& results, size_t& stepsRemaining,
    bool& truncated) {
  if (edgeIdx == edges.size()) {
    results.push_back(state);
    return;
  }
  const auto& edge = edges[edgeIdx];
  const auto& covered = state.coveredTriples_;
  for (size_t tripleIdx : *candidatesByEdge[edgeIdx]) {
    if (std::find(covered.begin(), covered.end(), tripleIdx) != covered.end()) {
      continue;
    }
    if (stepsRemaining == 0) {
      truncated = true;
      return;
    }
    --stepsRemaining;
    const auto& triple = triples._triples.at(tripleIdx);
    bool subjectWasNew = isNewBinding(state, edge.subject_);
    if (tryAssign(state, edge.subject_, triple.s_, viewCols)) {
      bool objectWasNew = isNewBinding(state, edge.object_);
      if (tryAssign(state, edge.object_, triple.o_, viewCols)) {
        state.coveredTriples_.push_back(tripleIdx);

        extendMatch(edges, candidatesByEdge, edgeIdx + 1, triples, viewCols,
                    state, results, stepsRemaining, truncated);

        state.coveredTriples_.pop_back();
      }
      undoAssign(state, edge.object_, objectWasNew);
    }
    undoAssign(state, edge.subject_, subjectWasNew);
  }
}

}  // namespace

// _____________________________________________________________________________
void QueryPatternCache::matchPattern(
    QueryExecutionContext* qec, const ViewPattern& pattern,
    const parsedQuery::BasicGraphPattern& triples,
    const TriplesByPredicate& triplesByPredicate, size_t budget,
    std::vector<MaterializedViewJoinReplacement>& result) const {
  // Quick reject: every edge's predicate must appear in the query, else no
  // embedding can exist. Also builds each edge's candidate list once.
  std::vector<const std::vector<size_t>*> candidatesByEdge;
  candidatesByEdge.reserve(pattern.edges_.size());
  for (const auto& edge : pattern.edges_) {
    auto it = triplesByPredicate.find(edge.predicate_);
    if (it == triplesByPredicate.end()) {
      return;
    }
    candidatesByEdge.push_back(&it->second);
  }

  const auto& viewCols = pattern.view_->variableToColumnMap();

  std::vector<PatternMatchState> matches;
  PatternMatchState state;
  size_t stepsRemaining = budget;
  bool truncated = false;
  extendMatch(pattern.edges_, candidatesByEdge, 0, triples, viewCols, state,
              matches, stepsRemaining, truncated);
  if (truncated) {
    AD_LOG_WARN << "Pattern matching for materialized view '"
                << pattern.view_->name()
                << "' exceeded the `materialized-view-pattern-match-budget`; "
                   "some applicable rewrites using this view may have been "
                   "missed for this query."
                << std::endl;
  }

  for (auto& match : matches) {
    // Fixed query values must land on a legal column prefix; a payload
    // column (index > 2) bound to a fixed value is always illegal.
    unsigned boundColumnsMask = 0;
    bool hasIllegalColumn = false;
    for (const auto& [viewVar, node] : match.assignment_) {
      if (!node.isVariable()) {
        size_t col = viewCols.at(viewVar).columnIndex_;
        if (col > 2) {
          hasIllegalColumn = true;
          break;
        }
        boundColumnsMask |= (1u << col);
      }
    }
    if (hasIllegalColumn || !isLegalFixedValuePrefix(boundColumnsMask)) {
      continue;
    }

    // `assignment_` and `RequestedColumns` are the same type.
    result.push_back(
        {pattern.view_->makeIndexScan(
             qec,
             parsedQuery::MaterializedViewQuery{pattern.view_->name(),
                                                std::move(match.assignment_)}),
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

  // A budget of `0` disables pattern-based rewriting entirely.
  size_t budget = getRuntimeParameter<
      &RuntimeParameters::materializedViewPatternMatchBudget_>();
  if (budget == 0) {
    return result;
  }

  // Group query triples by predicate and collect views sharing a predicate
  // with the query.
  TriplesByPredicate triplesByPredicate;
  ad_utility::HashSet<ViewPtr> candidateViews;
  for (const auto& [tripleIdx, triple] :
       ::ranges::views::enumerate(triples._triples)) {
    auto iri = triple.getSimplePredicate();
    if (!iri.has_value()) {
      continue;
    }
    auto it = predicateInView_.find(iri.value());
    if (it == predicateInView_.end()) {
      continue;
    }
    triplesByPredicate[iri.value()].push_back(tripleIdx);
    ql::ranges::copy(it->second,
                     std::inserter(candidateViews, candidateViews.end()));
  }

  for (const auto& view : candidateViews) {
    matchPattern(qec, patterns_.at(view), triples, triplesByPredicate, budget,
                 result);
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
