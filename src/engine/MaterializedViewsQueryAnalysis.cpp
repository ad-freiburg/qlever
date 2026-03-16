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
#include "parser/GraphPatternOperation.h"
#include "parser/PropertyPath.h"
#include "parser/SparqlParser.h"
#include "util/VariantRangeFilter.h"

namespace materializedViewsQueryAnalysis {

// _____________________________________________________________________________
std::vector<MaterializedViewJoinReplacement>
QueryPatternCache::makeJoinReplacementIndexScans(
    QueryExecutionContext* qec,
    const parsedQuery::BasicGraphPattern& triples) const {
  std::vector<MaterializedViewJoinReplacement> result;

  // All triples of the form `anything <iri> ?variable` where `<iri>` is covered
  // by a materialized view, stored by `?variable` for finding chains.
  ChainSideCandidates chainLeft;

  // All triples of the form `?variable <iri> ?otherVariable` where `<iri>` is
  // covered by a materialized view, where `?variable` is different from
  // `?otherVariable`, stored by `?variable` for finding chains.
  ChainSideCandidates chainRight;

  // All triples of the form `anything <iri> ?variable` where `<iri>` is covered
  // by a materialized view, stored by `anything` for finding join stars.
  StarCandidatesBySubject starCandidates;

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
      // This triple could be an arm of a join star.
      // TODO<ullingerc> Maybe already enforce distinct predicate/object here?
      starCandidates[triple.s_].push_back(tripleIdx);
    }
  }

  // Using the information collected by the pass over all triples, assemble all
  // chains that can potentially be rewritten.
  makeScansFromChainCandidates(qec, triples, result, chainLeft, chainRight);

  // Assemble all stars that can potentially be rewritten.
  makeScansFromStarCandidates(qec, triples, result, starCandidates);

  return result;
}

// _____________________________________________________________________________
void QueryPatternCache::makeScansFromChainCandidates(
    QueryExecutionContext* qec, const parsedQuery::BasicGraphPattern& triples,
    std::vector<MaterializedViewJoinReplacement>& result,
    const ChainSideCandidates& chainLeft,
    const ChainSideCandidates& chainRight) const {
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
        for (const auto& chainInfo : *(it->second)) {
          // We have found a materialized view for this chain. Construct an
          // `IndexScan`.
          result.push_back(
              {makeScanForSingleChain(qec, chainInfo, left.s_, varLeft,
                                      right.o_.getVariable()),
               {tripleIdxLeft, tripleIdxRight}});
        }
      }
    }
  }
}

// _____________________________________________________________________________
void QueryPatternCache::makeScansFromStarCandidates(
    QueryExecutionContext* qec, const parsedQuery::BasicGraphPattern& triples,
    std::vector<MaterializedViewJoinReplacement>& result,
    const StarCandidatesBySubject& starCandidates) const {
  if (starCache_.empty()) {
    return;
  }
  auto getTriples =
      [&triples](size_t idx) -> std::pair<size_t, const SparqlTriple&> {
    return {idx, triples._triples.at(idx)};
  };

  for (const auto& [subject, members] : starCandidates) {
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
      if (it != predicateInView_.end()) {
        for (auto view : (*it).second) {
          candidateViews.insert(view);
        }
      }
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
      const auto& starInfo = (*it).second;
      if (ql::ranges::includes(queryPredicates,
                               starInfo.arms_ | ql::views::keys)) {
        // If yes, assemble `RequestedColumns`.
        parsedQuery::MaterializedViewQuery::RequestedColumns cols;
        std::vector<size_t> coveredTriples;

        // The subject must be read.
        cols.insert({starInfo.subject_, subject});

        // The variable to variable mapping for all the objects of the star.
        for (const auto& [predicate, object] : starInfo.arms_) {
          size_t idx = predicateToTripleIdx.at(predicate);
          auto queryObject = triples._triples.at(idx).o_;
          cols.insert({object, queryObject});
          coveredTriples.push_back(idx);
        }

        // Construct the `MaterializedViewJoinReplacement`, in particular the
        // `IndexScan`.
        result.push_back({makeScanForStar(qec, view, std::move(cols)),
                          std::move(coveredTriples)});
      }
    }
  }
}

// _____________________________________________________________________________
std::shared_ptr<IndexScan> QueryPatternCache::makeScanForSingleChain(
    QueryExecutionContext* qec, ChainInfo cached, TripleComponent subject,
    std::optional<Variable> chain, Variable object) const {
  auto& [cSubject, cChainVar, cObject, view] = cached;
  parsedQuery::MaterializedViewQuery::RequestedColumns cols{
      {std::move(cSubject), std::move(subject)},
      {std::move(cObject), std::move(object)},
  };
  if (chain.has_value()) {
    cols.insert({std::move(cChainVar), std::move(chain.value())});
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
bool QueryPatternCache::analyzeSimpleChain(ViewPtr view, const SparqlTriple& a,
                                           const SparqlTriple& b) {
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

  // TODO<ullingerc> Check column indices and undef status in view.

  // Insert chain to cache.
  ChainedPredicates preds{aPred.value(), bPred.value()};
  auto [it, wasNew] = simpleChainCache_.try_emplace(preds, nullptr);
  if (it->second == nullptr) {
    it->second = std::make_shared<std::vector<ChainInfo>>();
  }
  it->second->push_back(
      ChainInfo{std::move(aSubj), std::move(chainVar), std::move(bObj), view});
  return true;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeJoinStar(
    ViewPtr view, const std::vector<SparqlTriple>& triples) {
  if (triples.size() < 2) {
    return false;
  }
  // All triples must have the same variable subject.
  if (!triples[0].s_.isVariable()) {
    return false;
  }
  Variable subject = triples[0].s_.getVariable();

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
    arms.push_back({std::string{pred.value()}, obj});
  }

  // Sort arms by predicate for linear-time matching.
  ql::ranges::sort(arms, {}, &StarArm::first);

  // TODO<ullingerc> Check column indices and undef status in view.

  // Insert star into cache.
  starCache_.insert({view, StarInfo{subject, std::move(arms)}});
  return true;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeView(ViewPtr view) {
  auto explainIgnore = [&](const std::string& reason) {
    AD_LOG_INFO << "Materialized view '" << view->name()
                << "' will not be added to the query pattern cache for query "
                   "rewriting. Reason: "
                << reason << "." << std::endl;
  };

  const auto& parsed = view->parsedQuery();
  if (!parsed.has_value()) {
    explainIgnore(
        "The view was built without remembering the original query string.");
    return false;
  }

  auto graphPatternsFiltered = graphPatternInvariantFilter(parsed.value());
  if (graphPatternsFiltered.size() != 1) {
    explainIgnore(
        "The view has more than one graph pattern (even after skipping ignored "
        "patterns)");
    return false;
  }
  const auto& graphPattern = graphPatternsFiltered.at(0);
  if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(graphPattern)) {
    explainIgnore("The graph pattern is not a basic set of triples");
    return false;
  }
  // TODO<ullingerc> Property path is stored as a single predicate here.
  const auto& triples = graphPattern.getBasic()._triples;
  if (triples.size() == 0) {
    explainIgnore("The query body is empty");
    return false;
  }
  bool patternFound = false;

  // TODO<ullingerc> Possibly handle chain by property path.
  if (triples.size() == 2) {
    const auto& a = triples.at(0);
    const auto& b = triples.at(1);
    patternFound =
        analyzeSimpleChain(view, a, b) || analyzeSimpleChain(view, b, a);
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
    explainIgnore("No supported query pattern for rewriting joins was found");
  }

  return patternFound;
}

// _____________________________________________________________________________
std::vector<parsedQuery::GraphPatternOperation> graphPatternInvariantFilter(
    const ParsedQuery& parsed) {
  BasicGraphPatternsInvariantTo invariantCheck{
      getVariablesPresentInFirstBasicGraphPattern(
          parsed._rootGraphPattern._graphPatterns)};

  // Filter out graph patterns that do not change the result of the basic graph
  // pattern analyzed.
  return parsed._rootGraphPattern._graphPatterns |
         ql::views::filter([&](const auto& pattern) {
           return !std::visit(invariantCheck, pattern);
         }) |
         ::ranges::to<std::vector>();
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

}  // namespace materializedViewsQueryAnalysis
