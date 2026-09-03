// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/MaterializedViewsQueryAnalysis.h"

#include <absl/strings/str_replace.h>

#include <algorithm>
#include <optional>
#include <variant>

#include "backports/algorithm.h"
#include "engine/IndexScan.h"
#include "engine/MaterializedViews.h"
#include "engine/MaterializedViewsPatternMatcher.h"
#include "engine/VariableToColumnMap.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "index/ExportIds.h"
#include "index/TripleComponentConversions.h"
#include "parser/GraphPatternOperation.h"
#include "parser/VariableCounter.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/VariantRangeFilter.h"

namespace materializedViewsQueryAnalysis {

namespace {

// A real value for `view`'s first column, read directly from the view's own
// materialized data (its smallest value, since the view is sorted on this
// column), for use as the placeholder when computing a reusable
// `FixedFirstColumnCacheKeyTemplate`. A synthetic value that provably does not
// occur in the view (e.g. a reserved-namespace IRI) would not do: the query
// planner's cost estimates for a scan depend on the actual selectivity of the
// fixed value, so planning with a value that is known upfront to match zero
// rows can pick a different (degenerate) plan shape than planning with a
// value that occurs for real -- which is what every actual candidate value
// from a user query, fixing a column of a view built to answer queries about
// that column, is expected to look like. Returns `std::nullopt` if the view is
// empty or its data cannot be read back as a `TripleComponent` (should not
// normally happen).
std::optional<TripleComponent> sampleFirstColumnValue(
    QueryExecutionContext* qec, const MaterializedView& view) {
  const auto& varToCol = view.variableToColumnMap();
  MaterializedView::ColumnMapping identityColMap;
  for (const auto& col : varToCol | ql::views::values) {
    identityColMap.insert({col.columnIndex_, col.columnIndex_});
  }
  auto scan = view.makeIndexScan(qec, varToCol, identityColMap);
  auto readFirstRow =
      [&](const auto& idTable,
          const LocalVocab& localVocab) -> std::optional<TripleComponent> {
    if (idTable.numRows() == 0) {
      return std::nullopt;
    }
    auto literalOrIri = ql::exportIds::idToLiteralOrIri(
        qec->getIndex(), idTable(0, 0), localVocab);
    if (!literalOrIri.has_value()) {
      return std::nullopt;
    }
    return literalOrIri->isIri() ? TripleComponent{literalOrIri->getIri()}
                                 : TripleComponent{literalOrIri->getLiteral()};
  };
  auto result = scan->getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
  if (result->isFullyMaterialized()) {
    return readFirstRow(result->idTableView(), result->localVocab());
  }
  for (auto& [idTable, localVocab] : result->idTables()) {
    return readFirstRow(idTable, localVocab);
  }
  return std::nullopt;
}

// The exact substring that `IndexScan::getCacheKeyImpl` (and any other
// operation that embeds a fixed `TripleComponent` the same way) produces for a
// triple component fixed to `value`. Anchored on `= "..."` so that replacing
// it can only ever touch an actually-embedded value, never unrelated cache-key
// syntax (column counts, operation names, etc.). NOTE: this only rules out
// colliding with unrelated *syntax*; it does not rule out colliding with an
// unrelated, already-fixed value elsewhere in the same cache key that happens
// to equal `value` -- see the occurrence-count check in `analyzeView`.
std::string cacheKeyValueAnchor(const TripleComponent& value) {
  return absl::StrCat(" = \"", toRdfLiteral(value), "\"");
}

// The number of times `needle` occurs in `haystack`, non-overlapping.
size_t countOccurrences(std::string_view haystack, std::string_view needle) {
  size_t count = 0;
  for (size_t pos = haystack.find(needle); pos != std::string_view::npos;
       pos = haystack.find(needle, pos + needle.size())) {
    ++count;
  }
  return count;
}

// The top-level triples of `parsed`, that is the triples of all
// `BasicGraphPattern`s that are direct children of the root graph pattern.
// These are the only triples in which `substituteFirstColumn` substitutes.
template <typename Query, typename Func>
void forEachTopLevelTriple(Query& parsed, Func func) {
  for (auto& pattern : parsed._rootGraphPattern._graphPatterns) {
    if (std::holds_alternative<parsedQuery::BasicGraphPattern>(pattern)) {
      for (auto& triple : pattern.getBasic()._triples) {
        func(triple);
      }
    }
  }
}

// Call `func(side, isSubject)` for the subject and the object of `triple`, if
// they are the given `variable`. The predicate is deliberately not visited: a
// variable predicate cannot be substituted by a `TripleComponent` (see
// `substituteFirstColumn`, which detects such an occurrence because it is
// counted by the `VariableCounter` but not here).
template <typename Triple, typename Func>
void forEachMatchingSide(Triple& triple, const Variable& variable, Func func) {
  for (bool isSubject : {true, false}) {
    auto& side = isSubject ? triple.s_ : triple.o_;
    if (side.isVariable() && side.getVariable() == variable) {
      func(side, isSubject);
    }
  }
}

// One position in a view's own query at which the variable of its first column
// occurs: the IRI of that triple's predicate, and whether the variable is the
// triple's subject (as opposed to its object). Used to find the candidate
// values for a fixed first column in a query, without having to try every
// fixed value that occurs anywhere in it.
struct FirstColumnAnchor {
  std::string_view predicate_;
  bool isSubject_;
};

// The `FirstColumnAnchor`s of `variable` in the top-level triples of `parsed`.
// The `string_view`s of the result point into `parsed`. There is exactly one
// anchor per legitimate substitution position, so `.size()` also doubles as
// the expected number of times a correctly-substituted value appears in a
// cache key computed from `parsed` (see the occurrence-count check in
// `analyzeView`).
std::vector<FirstColumnAnchor> firstColumnAnchors(const ParsedQuery& parsed,
                                                  const Variable& variable) {
  std::vector<FirstColumnAnchor> anchors;
  forEachTopLevelTriple(parsed, [&](const SparqlTriple& triple) {
    auto predicate = triple.getSimplePredicate();
    if (!predicate.has_value()) {
      return;
    }
    forEachMatchingSide(triple, variable,
                        [&](const TripleComponent&, bool isSubject) {
                          anchors.push_back({predicate.value(), isSubject});
                        });
  });
  return anchors;
}

// Hard cap on the number of candidate values considered per view and query.
// The anchors already restrict the candidates to the fixed values that occur
// with the right predicate in the right position, of which a real query has
// one or two; this is just a defensive bound against a pathological query
// (e.g. a huge `VALUES` clause). ponytail: fixed constant, promote to a
// runtime parameter only if a real workload needs it tuned.
constexpr size_t kMaxCandidateValuesPerView = 8;

// Cheaply approximate the cache key that a full replan of a view's query with
// its first column fixed to `value` would produce, by substituting `value`
// for the sampled placeholder value that `cacheKeyTemplate` was planned with
// once (see `sampleFirstColumnValue` and `QueryPatternCache::analyzeView`).
// This is exact whenever the plan's shape does not depend on which value is
// fixed (the common case for a view's own, typically small, defining query);
// if it does not hold for a particular value, the result simply matches
// nothing real later on -- see `FixedFirstColumnCacheKeyTemplate`.
std::string substituteValueInCacheKeyTemplate(
    const FixedFirstColumnCacheKeyTemplate& cacheKeyTemplate,
    const TripleComponent& value) {
  return absl::StrReplaceAll(
      cacheKeyTemplate.cacheKeyTemplate_,
      {{cacheKeyValueAnchor(cacheKeyTemplate.placeholderValue_),
        cacheKeyValueAnchor(value)}});
}

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
PatternMatcherLimits PatternMatcherLimits::requestBounded(
    PatternMatcherLimits requestedAmount) const {
  return {std::min(numAssignments_, requestedAmount.numAssignments_),
          std::min(numReplacementPlans_, requestedAmount.numReplacementPlans_)};
}

// _____________________________________________________________________________
PatternMatcherLimits PatternMatcherLimits::nextViewShare(
    size_t viewsLeft) const {
  AD_CORRECTNESS_CHECK(viewsLeft > 0);
  constexpr size_t minNumAssignmentsPerView = 1'000;
  constexpr size_t minNumReplacementPlansPerView = 15;
  return requestBounded(
      {std::max(minNumAssignmentsPerView, numAssignments_ / viewsLeft),
       std::max(minNumReplacementPlansPerView,
                numReplacementPlans_ / viewsLeft)});
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
  // predicate with the query. Sorted and deduplicated by name below, so
  // that the sharing of the limits pool is deterministic.
  TriplesByPredicate triplesByPredicate;
  std::vector<std::pair<std::string_view, ViewPtr>> candidateViews;
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
      candidateViews.emplace_back(view->name(), view);
    }
  }
  if (candidateViews.empty()) {
    return result;
  }
  ql::ranges::sort(candidateViews, {}, ad_utility::first);
  candidateViews.erase(std::unique(candidateViews.begin(), candidateViews.end(),
                                   [](const auto& a, const auto& b) {
                                     return a.first == b.first;
                                   }),
                       candidateViews.end());

  // Match all `candidateViews` against the query, using a shared pool of limits
  // across them. Each view's share is recomputed from the budget still
  // `remaining` and the number of views left to process.
  PatternMatcherLimits remaining{totalNumAssignments, totalNumReplacementPlans};
  bool truncatedByNumAssignments = false;
  bool truncatedByNumReplacementPlans = false;
  size_t numViewsLeft = candidateViews.size();
  for (const auto& [name, view] : candidateViews) {
    if (remaining.isExhausted()) {
      break;
    }
    auto report = PatternMatcher::findReplacementPlans(
        patterns_.at(view), triples, triplesByPredicate, qec,
        remaining.nextViewShare(numViewsLeft), result);
    remaining.subtract(report.used_);
    truncatedByNumAssignments |=
        report.status_ ==
        PatternMatcher::MatchStatus::TruncatedByNumAssignments;
    truncatedByNumReplacementPlans |=
        report.status_ ==
        PatternMatcher::MatchStatus::TruncatedByNumReplacementPlans;
    --numViewsLeft;
  }
  if (truncatedByNumAssignments || truncatedByNumReplacementPlans) {
    AD_LOG_WARN
        << "Pattern matching for materialized views hit the `"
        << (truncatedByNumAssignments
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

  // If the view's first column can be fixed at all (see
  // `substituteFirstColumn`), precompute a `FixedFirstColumnCacheKeyTemplate`
  // once here, so that fixing it to an actual value later, for any number of
  // queries, is a cheap string substitution instead of a full replan (see
  // `addCacheKeysWithFixedFirstColumn`).
  if (auto variable = view->firstColumnVariable();
      qec != nullptr && variable.has_value()) {
    if (auto placeholder = sampleFirstColumnValue(qec, *view);
        placeholder.has_value()) {
      auto [fixedFull, fixedWithoutInvariant] =
          view->computeCacheKey(qec, placeholder.value());
      // The number of places in the view's own query that
      // `substituteFirstColumn` legitimately fixes to the placeholder (see
      // `firstColumnAnchors`). A cache key produced from a different number of
      // occurrences of the placeholder's exact text means the sampled value
      // coincidentally also occurs as an unrelated, already-fixed constant
      // elsewhere in the view's own query (or, symmetrically, that two
      // legitimate occurrences collapsed into one scan) -- either way,
      // `substituteValueInCacheKeyTemplate`'s blind text substitution could no
      // longer be trusted to touch only the legitimate occurrences, so the
      // template is discarded rather than risking an incorrect rewrite.
      size_t expectedOccurrences =
          firstColumnAnchors(parsed.value(), variable.value()).size();
      auto anchor = cacheKeyValueAnchor(placeholder.value());
      std::vector<FixedFirstColumnCacheKeyTemplate> templates;
      auto addTemplate = [&](auto& cacheKeyAndCol) {
        if (!cacheKeyAndCol.has_value()) {
          return;
        }
        const auto& key = cacheKeyAndCol.value().cacheKey_;
        if (countOccurrences(key, anchor) != expectedOccurrences) {
          AD_LOG_INFO
              << "Materialized view '" << view->name()
              << "' will not use the fast, template-based rewriting for a "
                 "fixed first column: the sampled placeholder value "
                 "coincidentally collides with an unrelated fixed value "
                 "elsewhere in the view's own query."
              << std::endl;
          return;
        }
        templates.push_back({std::move(cacheKeyAndCol.value().columnMapping_),
                             key, placeholder.value()});
      };
      addTemplate(fixedFull);
      addTemplate(fixedWithoutInvariant);
      if (!templates.empty()) {
        fixedFirstColumnTemplates_.insert({view, std::move(templates)});
      }
    }
  }

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
      // `buildPatternEdges` already required a simple predicate for every
      // triple here, or `patternFound` would be `false`.
      auto predicate = triple.getSimplePredicate();
      AD_CORRECTNESS_CHECK(predicate.has_value());
      predicateInView_[predicate.value()].push_back(view);
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

  // Remove `view`'s fixed-first-column cache-key template, if any.
  fixedFirstColumnTemplates_.erase(view);
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
bool substituteFirstColumn(ParsedQuery& parsed, const Variable& variable,
                           const TripleComponent& value) {
  // A `LIMIT`/`OFFSET` restricts the view to an (order-dependent, arbitrary)
  // subset of its query's rows. The rows of that subset with the first column
  // equal to `value` are not the same as the rows of the restricted query.
  if (!parsed._limitOffset.isUnconstrained()) {
    return false;
  }

  if (!parsed.hasSelectClause()) {
    return false;
  }
  auto& select = parsed.selectClause();

  // `GROUP BY` would have to group by something that is no longer a column,
  // and simply dropping it is not equivalent: for a value that does not occur
  // in the view, an implicit `GROUP BY` returns one row (with `COUNT` = 0,
  // say), a scan on the view none. `ORDER BY` puts a `Sort` at the root of the
  // view's query, which a scan on the view (sorted by the view's own columns)
  // does not reproduce. `HAVING`, a trailing `VALUES` clause, and an alias in
  // the `SELECT` clause may all refer to `variable`.
  if (!parsed._groupByVariables.empty() || !parsed._orderBy.empty() ||
      !parsed._havingClauses.empty() ||
      parsed.postQueryValuesClause_.has_value() ||
      !select.getAliases().empty()) {
    return false;
  }

  // Only occurrences as the subject or object of a top-level triple are
  // substituted below, so `variable` must not occur anywhere else: in a
  // `FILTER`, a `BIND`, a subquery, as a predicate, and so on.
  parsedQuery::VariableCounter counter;
  counter(parsed._rootGraphPattern);
  auto totalCount = ad_utility::findOptional(counter.counts(), variable);
  size_t numSubstituted = 0;
  forEachTopLevelTriple(parsed, [&](SparqlTriple& triple) {
    forEachMatchingSide(triple, variable,
                        [&](TripleComponent&, bool) { ++numSubstituted; });
  });
  if (numSubstituted == 0 || !totalCount.has_value() ||
      totalCount.value() != numSubstituted) {
    return false;
  }

  forEachTopLevelTriple(parsed, [&](SparqlTriple& triple) {
    forEachMatchingSide(
        triple, variable,
        [&value](TripleComponent& side, bool) { side = value; });
  });

  // `variable` is not bound by the query anymore, so it must not be selected.
  auto isVariable = [&variable](const Variable& var) {
    return var == variable;
  };
  ql::erase_if(select.visibleVariables_, isVariable);
  if (!select.isAsterisk()) {
    auto selected = select.getSelectedVariables();
    ql::erase_if(selected, isVariable);
    if (selected.empty()) {
      return false;
    }
    select.setSelected(std::move(selected));
  } else if (select.visibleVariables_.empty()) {
    return false;
  }
  return true;
}

// _____________________________________________________________________________
void QueryPatternCache::addCacheKeysWithFixedFirstColumn(
    const parsedQuery::BasicGraphPattern& triples,
    ViewCacheKeysWithFixedFirstColumn& result) const {
  for (const auto& [view, templates] : fixedFirstColumnTemplates_) {
    const auto& parsed = view->parsedQuery();
    auto variable = view->firstColumnVariable();
    // A template only ever gets stored for a view with a parsed query and a
    // first-column variable, see `analyzeView`.
    AD_CORRECTNESS_CHECK(parsed.has_value() && variable.has_value());
    auto anchors = firstColumnAnchors(parsed.value(), variable.value());
    if (anchors.empty()) {
      continue;
    }

    // The fixed values of the query's triples that sit at one of the anchors.
    std::vector<TripleComponent> values;
    for (const auto& triple : triples._triples) {
      if (values.size() >= kMaxCandidateValuesPerView) {
        break;
      }
      auto predicate = triple.getSimplePredicate();
      if (!predicate.has_value()) {
        continue;
      }
      for (const auto& anchor : anchors) {
        if (anchor.predicate_ != predicate.value()) {
          continue;
        }
        const auto& side = anchor.isSubject_ ? triple.s_ : triple.o_;
        if (!side.isVariable() && !ad_utility::contains(values, side)) {
          values.push_back(side);
        }
      }
    }

    for (const auto& value : values) {
      for (const auto& cacheKeyTemplate : templates) {
        // A key that is already known (from an earlier graph pattern of the
        // same query, or from the view's other template) is kept.
        result.keys_.insert(
            {substituteValueInCacheKeyTemplate(cacheKeyTemplate, value),
             std::make_shared<ByCacheKeyInfo>(
                 ByCacheKeyInfo{view, cacheKeyTemplate.colMapping_, value})});
      }
    }
  }
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
