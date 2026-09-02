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
#include "parser/GraphPatternOperation.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/VariantRangeFilter.h"

namespace materializedViewsQueryAnalysis {

// _____________________________________________________________________________
std::vector<MaterializedViewJoinReplacement>
QueryPatternCache::makeJoinReplacementIndexScans(
    QueryExecutionContext* qec,
    const parsedQuery::BasicGraphPattern& triples) const {
  // We do not allow `triples` to contain more than 64 triples, because we use a
  // 64-bit bitmask for them. This is not a problem, because `QueryPlanner` does
  // not allow graph patterns with more than 64 triples anyway.
  AD_CONTRACT_CHECK(triples._triples.size() <= 64,
                    "At most 64 triples allowed at the moment.");

  std::vector<MaterializedViewJoinReplacement> result;

  // All triples of the form `anything <iri> ?variable` where `<iri>` is covered
  // by a materialized view, stored by `?variable` for finding chains.
  VariableToTripleIndices chainLeft;

  // All triples of the form `?variable <iri> ?otherVariable` where `<iri>` is
  // covered by a materialized view, where `?variable` is different from
  // `?otherVariable`, stored by `?variable` for finding chains.
  // The same is required for star join detection.
  VariableToTripleIndices chainRight;

  for (const auto& [tripleIdx, triple] :
       ::ranges::views::enumerate(triples._triples)) {
    const auto iri = triple.getSimplePredicate();
    // Variables as predicate are not supported by query rewriting and sequence
    // property paths are expected to be replaced by joins during earlier stages
    // of query planning.
    if (!iri.has_value()) {
      continue;
    }
    // If no view that we know of contains this predicate so we can ignore
    // this triple altogether.
    if (!predicateInView_.contains(iri.value())) {
      continue;
    }
    // Check for potential join chain or star triple.
    if (!triple.o_.isVariable()) {
      continue;
    }
    if (triple.s_.isVariable()) {
      // This triple could be the right side of a chain join.
      chainRight[triple.s_.getVariable()].push_back(tripleIdx);
    }
    if (triple.s_ != triple.o_) {
      // This triple could be the left side of a chain join.
      chainLeft[triple.o_.getVariable()].push_back(tripleIdx);
    }
  }

  // Using the information collected by the pass over all triples, assemble all
  // chains that can potentially be rewritten.
  makeScansFromChainCandidates(qec, triples, result, chainLeft, chainRight);

  // Assemble all stars that can potentially be rewritten. Reuses the analysis
  // performed for chain joins.
  makeScansFromStarCandidates(qec, triples, result, chainRight);

  return result;
}

// _____________________________________________________________________________
// Check whether `component` is an internal variable that occurs in no other
// position of `triples`, i.e., it only states the existence of a triple
// without making its object usable anywhere. The parser replaces blank nodes
// (`[]` or `_:label`) in a query body by such internal variables; a user
// cannot write internal variables directly, so they cannot occur in
// expressions or in a SELECT clause, and counting the subject and object
// positions of `triples` is exhaustive (internal variables introduced by the
// parser itself, e.g. for property paths, occur in two triples and are
// therefore not existential).
static bool isExistentialObject(const TripleComponent& component,
                                const std::vector<SparqlTriple>& triples) {
  if (!component.isVariable() || !component.getVariable().name().starts_with(
                                     QLEVER_INTERNAL_VARIABLE_PREFIX)) {
    return false;
  }
  size_t numOccurrences = 0;
  for (const auto& triple : triples) {
    numOccurrences += triple.s_ == component;
    numOccurrences += triple.o_ == component;
  }
  return numOccurrences == 1;
}

// _____________________________________________________________________________
// Check that every restriction triple of the view's chain (e.g.
// `?s rdf:type <SomeClass>`, see `ChainInfo::restrictions_`) has an exact
// counterpart among the query's `triples`, with the view's chain variables
// translated to the query's corresponding components. The indices of the
// matched query triples are added to the `coveredTriples` bitmask (they are
// covered by
// the view scan just like the two chain triples). Returns `false` if any
// restriction has no counterpart; the view must not be used for the query
// then, because it only contains the rows satisfying the restrictions.
static bool chainRestrictionsCoveredByQuery(
    const ChainInfo& chainInfo, const TripleComponent& querySubject,
    const Variable& queryChain, const Variable& queryObject,
    const parsedQuery::BasicGraphPattern& triples,
    uint64_t& coveredTriples) {
  for (const auto& restriction : chainInfo.restrictions_) {
    // Translate the restriction's subject (one of the view's three chain
    // variables, enforced by `analyzeSimpleChain`) to the query's side.
    const auto& restrictionSubject = restriction.s_.getVariable();
    TripleComponent mappedSubject =
        restrictionSubject == chainInfo.subject_ ? querySubject
        : restrictionSubject == chainInfo.chain_ ? TripleComponent{queryChain}
                                                 : TripleComponent{queryObject};

    auto matches = [&](const SparqlTriple& triple) {
      if (triple.s_ != mappedSubject ||
          triple.getSimplePredicate() != restriction.getSimplePredicate()) {
        return false;
      }
      // An existential restriction (the view's object is an anonymous blank
      // node, see `isExistentialObject`) matches a query triple whose object
      // is likewise an anonymous blank node used nowhere else; such a triple
      // asks for the same existence check that the view has materialized.
      // An explicit query variable is NOT matched: it might be used in other
      // parts of the query that are not visible here.
      if (restriction.o_.isVariable()) {
        return isExistentialObject(triple.o_, triples._triples);
      }
      return triple.o_ == restriction.o_;
    };
    auto it = ql::ranges::find_if(triples._triples, matches);
    if (it == triples._triples.end()) {
      return false;
    }
    // If the chain's subject is fixed in the query, the matched restriction
    // triple has no variables at all and thus forms its own connected
    // component in the query planner, where a replacement plan spanning two
    // components could not be used. Require the triple's presence (checked
    // above), but let it be evaluated separately (a cheap existence check)
    // instead of covering it.
    if (mappedSubject.isVariable()) {
      size_t idx = std::distance(triples._triples.begin(), it);
      coveredTriples |= (uint64_t{1} << idx);
    }
  }
  // Duplicate restrictions in the view's query would match the same query
  // triple twice; the number of covered triples determines the plan's round
  // in the query planner's dynamic programming table, so it must be exact.
  // Setting a bit twice is idempotent, so the bitmask needs no explicit
  // deduplication.
  return true;
}

// _____________________________________________________________________________
void QueryPatternCache::makeScansFromChainCandidates(
    QueryExecutionContext* qec, const parsedQuery::BasicGraphPattern& triples,
    std::vector<MaterializedViewJoinReplacement>& result,
    const VariableToTripleIndices& chainLeft,
    const VariableToTripleIndices& chainRight) const {
  for (const auto& [varLeft, triplesLeft] : chainLeft) {
    // No triples for the right side on the same variable have been collected.
    if (!chainRight.contains(varLeft)) {
      continue;
    }

    // Iterate over all chains present and check if they can be rewritten to a
    // view scan.
    for (auto tripleIdxRight : chainRight.at(varLeft)) {
      for (auto tripleIdxLeft : triplesLeft) {
        const auto& left = triples._triples.at(tripleIdxLeft);
        const auto& right = triples._triples.at(tripleIdxRight);

        // Lookup key based on `std::string_view` avoids copying the IRIs. We
        // have already checked that the triples have single IRIs as predicates.
        ChainedPredicatesForLookup key{left.getSimplePredicate().value(),
                                       right.getSimplePredicate().value()};

        // Lookup if there are matching views. There could potentially be
        // multiple (e.g. with different sorting).
        auto it = simpleChainCache_.find(key);
        if (it == simpleChainCache_.end()) {
          continue;
        }
        // A degenerate chain (`?a <p1> ?b . ?b <p2> ?a` or
        // `?a <p1> ?b . ?b <p2> ?b`) would require adding a filter on top of
        // the view's `IndexScan`, which is not supported.
        if (right.s_ == right.o_ ||
            (left.s_.isVariable() &&
             left.s_.getVariable() == right.o_.getVariable())) {
          continue;
        }

        for (const auto& chainInfo : *(it->second)) {
          // If the subject of the chain is fixed, but the subject is not the
          // first column of the view, rewriting cannot be applied.
          if (!left.s_.isVariable() && chainInfo.view_->variableToColumnMap()
                                               .at(chainInfo.subject_)
                                               .columnIndex_ != 0) {
            continue;
          }
          // If the view was built with restriction triples, the query must
          // contain matching triples, which the scan then also covers.
          uint64_t coveredTriples = (uint64_t{1} << tripleIdxLeft) |
                                    (uint64_t{1} << tripleIdxRight);
          if (!chainRestrictionsCoveredByQuery(chainInfo, left.s_, varLeft,
                                               right.o_.getVariable(), triples,
                                               coveredTriples)) {
            continue;
          }
          // We have found a materialized view for this chain. Construct an
          // `IndexScan`.
          result.push_back(
              {makeScanForSingleChain(qec, chainInfo, left.s_, varLeft,
                                      right.o_.getVariable()),
               coveredTriples});
        }
      }
    }
  }
}

// _____________________________________________________________________________
void QueryPatternCache::makeScansFromStarCandidates(
    QueryExecutionContext* qec, const parsedQuery::BasicGraphPattern& triples,
    std::vector<MaterializedViewJoinReplacement>& result,
    const VariableToTripleIndices& starCandidates) const {
  if (starCache_.empty()) {
    return;
  }
  auto getTriples =
      [&triples](size_t idx) -> std::pair<size_t, const SparqlTriple&> {
    return {idx, triples._triples.at(idx)};
  };

  for (const auto& [subject, members] : starCandidates) {
    // The candidates are the triples of the query grouped by subject variable.
    // If there aren't at least two triples sharing the subject, this group
    // can't be a star.
    if (members.size() < 2) {
      continue;
    }

    ad_utility::HashSet<ViewPtr> candidateViews;
    ad_utility::HashMap<std::string_view, size_t> predicateToTripleIdx;
    ad_utility::HashSet<Variable> objects;

    for (const auto& [idx, triple] :
         ql::views::transform(members, getTriples)) {
      // Check constraints on object: must be a variable different from the
      // subject and not appear multiple times.
      if (!triple.o_.isVariable() || triple.o_ == subject ||
          !objects.insert(triple.o_.getVariable()).second) {
        continue;
      }
      // Each predicate may only appear once.
      if (!predicateToTripleIdx
               .insert({triple.getSimplePredicate().value(), idx})
               .second) {
        continue;
      }
      // Remember all views that have this predicate.
      const auto& it =
          predicateInView_.find(triple.getSimplePredicate().value());
      if (it == predicateInView_.end()) {
        continue;
      }
      ql::ranges::copy(it->second,
                       std::inserter(candidateViews, candidateViews.end()));
    }

    // Compute a sorted vector of all the predicates in the query star.
    auto queryPredicates = predicateToTripleIdx | ql::views::keys |
                           ::ranges::to<std::vector<std::string_view>>();
    ql::ranges::sort(queryPredicates);

    // Check all the possible views if they are actually applicable.
    for (auto view : candidateViews) {
      // Does this view provide a join star?
      auto it = starCache_.find(view);
      if (it == starCache_.end()) {
        continue;
      }
      // Does the query contain a superset of the star arms of the view?
      const auto& starInfo = it->second;
      if (ql::ranges::includes(queryPredicates,
                               starInfo.arms_ | ql::views::keys)) {
        parsedQuery::MaterializedViewQuery::RequestedColumns cols;
        uint64_t coveredTriples = 0;

        // The subject must be read.
        cols.insert({starInfo.subject_, subject});

        // The variable to variable mapping for all the objects of the star.
        for (const auto& [predicate, object] : starInfo.arms_) {
          size_t idx = predicateToTripleIdx.at(predicate);
          auto queryObject = triples._triples.at(idx).o_;
          cols.insert({object, queryObject});
          coveredTriples |= (uint64_t{1} << idx);
        }

        // Construct the `MaterializedViewJoinReplacement`, in particular the
        // `IndexScan`.
        result.push_back(
            {makeScanForStar(qec, view, std::move(cols)), coveredTriples});
      }
    }
  }
}

// _____________________________________________________________________________
std::shared_ptr<IndexScan> QueryPatternCache::makeScanForSingleChain(
    QueryExecutionContext* qec, ChainInfo cached, TripleComponent subject,
    std::optional<Variable> chain, Variable object) const {
  auto& view = cached.view_;
  parsedQuery::MaterializedViewQuery::RequestedColumns cols{
      {std::move(cached.subject_), std::move(subject)},
      {std::move(cached.object_), std::move(object)},
  };
  if (chain.has_value()) {
    cols.insert({std::move(cached.chain_), std::move(chain.value())});
  }
  return view->makeIndexScan(
      qec, parsedQuery::MaterializedViewQuery{view->name(), std::move(cols)});
}

// _____________________________________________________________________________
std::shared_ptr<IndexScan> QueryPatternCache::makeScanForStar(
    QueryExecutionContext* qec, ViewPtr view,
    parsedQuery::MaterializedViewQuery::RequestedColumns cols) const {
  return view->makeIndexScan(
      qec, parsedQuery::MaterializedViewQuery{view->name(), std::move(cols)});
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeSimpleChain(
    ViewPtr view, const SparqlTriple& a, const SparqlTriple& b,
    const std::vector<SparqlTriple>& restrictions) {
  // Check predicates.
  auto aPred = a.getSimplePredicate();
  if (!aPred.has_value()) {
    return false;
  }
  auto bPred = b.getSimplePredicate();
  if (!bPred.has_value()) {
    return false;
  }

  // Check variables.
  if (!a.s_.isVariable()) {
    return false;
  }
  auto aSubj = a.s_.getVariable();

  if (!a.o_.isVariable() || a.o_.getVariable() == aSubj) {
    return false;
  }
  auto chainVar = a.o_.getVariable();

  if (!b.s_.isVariable() || b.s_.getVariable() != chainVar) {
    return false;
  }

  if (!b.o_.isVariable() || b.o_.getVariable() == chainVar ||
      b.o_.getVariable() == aSubj) {
    return false;
  }
  auto bObj = b.o_.getVariable();

  // All three variables must actually be columns of the view (e.g. this is
  // not the case if they do not appear in the `SELECT` clause).
  const auto& viewCols = view->variableToColumnMap();
  if (!viewCols.contains(aSubj) || !viewCols.contains(chainVar) ||
      !viewCols.contains(bObj)) {
    return false;
  }

  // Each restriction triple's subject must be one of the three chain
  // variables, otherwise it cannot be translated to the query's variables
  // when matching (see `chainRestrictionsCoveredByQuery`).
  if (!ql::ranges::all_of(restrictions, [&](const SparqlTriple& restriction) {
        const auto& s = restriction.s_.getVariable();
        return s == aSubj || s == chainVar || s == bObj;
      })) {
    return false;
  }

  // Insert chain to cache.
  ChainedPredicates preds{aPred.value(), bPred.value()};
  auto [it, wasNew] = simpleChainCache_.try_emplace(preds, nullptr);
  if (it->second == nullptr) {
    it->second = std::make_shared<std::vector<ChainInfo>>();
  }
  it->second->push_back(ChainInfo{std::move(aSubj), std::move(chainVar),
                                  std::move(bObj), view, restrictions});
  return true;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeJoinStar(
    ViewPtr view, const std::vector<SparqlTriple>& triples) {
  AD_CORRECTNESS_CHECK(triples.size() >= 2);

  // All triples must have the same variable subject.
  if (!triples[0].s_.isVariable()) {
    return false;
  }
  Variable subject = triples[0].s_.getVariable();

  // The subject must actually be a column of the view (e.g. this is not the
  // case if they do not appear in the `SELECT` clause).
  const auto& viewCols = view->variableToColumnMap();
  if (!viewCols.contains(subject)) {
    return false;
  }

  std::vector<StarArm> arms;
  ad_utility::HashSet<std::string> predicates;
  ad_utility::HashSet<Variable> objects;

  for (const auto& triple : triples) {
    // Same subject variable.
    if (!triple.s_.isVariable() || triple.s_.getVariable() != subject) {
      return false;
    }
    // Simple IRI predicate.
    auto pred = triple.getSimplePredicate();
    if (!pred.has_value()) {
      return false;
    }
    // Predicates must be distinct.
    if (!predicates.insert(std::string{pred.value()}).second) {
      return false;
    }
    // The object must be a variable.
    if (!triple.o_.isVariable()) {
      return false;
    }
    Variable obj = triple.o_.getVariable();
    if (obj == subject) {
      return false;
    }
    // Object variables must be distinct.
    if (!objects.insert(obj).second) {
      return false;
    }
    // The object must actually be a column of the view.
    if (!viewCols.contains(obj)) {
      return false;
    }
    arms.push_back({std::string{pred.value()}, obj});
  }

  // Sort arms by predicate for linear-time matching.
  ql::ranges::sort(arms, {}, &StarArm::first);

  // Insert star into cache.
  starCache_.insert({view, StarInfo{subject, std::move(arms)}});
  return true;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeView(ViewPtr view, QueryExecutionContext* qec) {
  auto explainIgnore = [&](const std::string& reason) {
    AD_LOG_INFO << "Materialized view '" << view->name()
                << "' will not be added to the query pattern cache for "
                   "pattern-based (star/chain) query rewriting. Reason: "
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

  // Partition the view's triples into join triples (with a variable object)
  // and restriction triples (with a variable subject and a simple IRI
  // predicate). Restriction triples restrict which rows end up in the view; a
  // query can still be rewritten to scan the view if it contains the very
  // same triples (checked at matching time by
  // `chainRestrictionsCoveredByQuery`). There are two kinds: triples with a
  // fixed object (e.g. `?s rdf:type <SomeClass>`) and existential
  // restrictions, whose object is an anonymous blank node (e.g.
  // `?s <somePredicate> []`, "a triple with this predicate exists").
  std::vector<SparqlTriple> joinTriples;
  std::vector<SparqlTriple> restrictions;
  for (const auto& triple : triples) {
    if (triple.s_.isVariable() && triple.getSimplePredicate().has_value() &&
        isExistentialObject(triple.o_, triples)) {
      restrictions.push_back(triple);
    } else if (triple.o_.isVariable()) {
      joinTriples.push_back(triple);
    } else if (triple.s_.isVariable() &&
               triple.getSimplePredicate().has_value()) {
      restrictions.push_back(triple);
    }
  }
  // Triples with a fixed subject and a fixed object fall into neither
  // category; such views are not supported for pattern-based rewriting.
  const bool allTriplesPartitioned =
      joinTriples.size() + restrictions.size() == triples.size();

  // TODO<ullingerc> Possibly handle chain by property path.
  if (allTriplesPartitioned && joinTriples.size() == 2) {
    const auto& a = joinTriples.at(0);
    const auto& b = joinTriples.at(1);
    patternFound = analyzeSimpleChain(view, a, b, restrictions) ||
                   analyzeSimpleChain(view, b, a, restrictions);
  }

  // Check for a join star of arbitrary size (>= 2 arms).
  if (!patternFound && triples.size() >= 2) {
    patternFound = analyzeJoinStar(view, triples);
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
  // join, which the chain/star rewriting below does not account for.
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
  // Remove `view` from chain cache.
  for (auto& [chain, views] : simpleChainCache_) {
    ql::erase_if(*views,
                 [&view](const ChainInfo& info) { return info.view_ == view; });
  }

  // Remove `view` from predicate cache.
  for (auto& [pred, views] : predicateInView_) {
    ql::erase_if(views, [&view](ViewPtr pView) { return pView == view; });
  }

  // Remove `view` from star cache.
  starCache_.erase(view);

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
