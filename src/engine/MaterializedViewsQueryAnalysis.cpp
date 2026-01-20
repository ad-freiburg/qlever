// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/MaterializedViewsQueryAnalysis.h"

#include <optional>
#include <variant>

#include "engine/IndexScan.h"
#include "engine/MaterializedViews.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlParser.h"

namespace materializedViewsQueryAnalysis {

// _____________________________________________________________________________
ad_utility::HashSet<Variable> getVariablesPresentInBasicGraphPatterns(
    const std::vector<parsedQuery::GraphPatternOperation>& graphPatterns) {
  ad_utility::HashSet<Variable> vars;
  for (const auto& graphPattern : graphPatterns) {
    if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(graphPattern)) {
      continue;
    }
    for (const auto& triple : graphPattern.getBasic()._triples) {
      if (triple.s_.isVariable()) {
        vars.insert(triple.s_.getVariable());
      }
      if (triple.o_.isVariable()) {
        vars.insert(triple.o_.getVariable());
      }
      if (auto p = triple.getPredicateVariable()) {
        vars.insert(p.value());
      }
    }
  }
  return vars;
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Optional&) const {
  // TODO<ullingerc> Analyze if the optional binds values from the outside
  // query.
  return false;
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Bind& bind) const {
  return !variables_.contains(bind._target);
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Values& values) const {
  return !std::ranges::any_of(
      values._inlineValues._variables,
      [this](const auto& var) { return variables_.contains(var); });
}

// _____________________________________________________________________________
std::optional<UserQueryChain> QueryPatternCache::checkSimpleChain(
    std::shared_ptr<IndexScan> left, std::shared_ptr<IndexScan> right) const {
  if (!left || !right || !left->predicate().isIri() ||
      !right->predicate().isIri()) {
    return std::nullopt;
  }
  if (left->object() == right->subject() &&
      left->subject() != right->object() && left->subject() != left->object() &&
      right->subject() != right->object() && left->object().isVariable() &&
      right->object().isVariable()) {
    materializedViewsQueryAnalysis::ChainedPredicates preds{
        left->predicate().getIri().toStringRepresentation(),
        right->predicate().getIri().toStringRepresentation()};
    if (simpleChainCache_.contains(preds)) {
      return UserQueryChain{left->subject(), left->object().getVariable(),
                            right->object().getVariable(),
                            simpleChainCache_.at(preds)};
    }
  }
  return std::nullopt;
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

  // Insert chain to cache.
  ChainedPredicates preds{aPred.value(), bPred.value()};
  if (!simpleChainCache_.contains(preds)) {
    simpleChainCache_[preds] = {};
  }
  simpleChainCache_[preds].push_back(
      ChainInfo{std::move(aSubj), std::move(chainVar), std::move(bObj), view});
  return true;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeView(ViewPtr view) {
  AD_LOG_INFO << view->name() << std::endl;
  auto q = view->originalQuery();
  // We do not need the `EncodedIriManager` because we are only interested in
  // analyzing the query structure, not in converting its components to
  // `ValueId`s.
  EncodedIriManager e;
  auto parsed = SparqlParser::parseQuery(&e, q, {});

  // TODO<ullingerc> Do we want to report the reason for non-optimizable
  // queries?

  const auto& graphPatterns = parsed._rootGraphPattern._graphPatterns;
  BasicGraphPatternsInvariantTo invariantCheck{
      getVariablesPresentInBasicGraphPatterns(graphPatterns)};
  // Filter out graph patterns that do not change the result of the basic graph
  // pattern analyzed.
  // TODO<ullingerc> Deduplication necessary when reading, the variables should
  // not be in the first three
  auto graphPatternsFiltered =
      ::ranges::to<std::vector>(parsed._rootGraphPattern._graphPatterns |
                                ql::views::filter([&](const auto& pattern) {
                                  return !std::visit(invariantCheck, pattern);
                                }));
  if (graphPatternsFiltered.size() != 1) {
    return false;
  }
  const auto& graphPattern = graphPatternsFiltered.at(0);
  if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(graphPattern)) {
    return false;
  }
  // TODO<ullingerc> Property path is stored as a single predicate here.
  const auto& triples = graphPattern.getBasic()._triples;
  if (triples.size() == 0) {
    return false;
  }
  bool patternFound = false;

  // TODO<ullingerc> Possibly handle chain by property path.
  if (triples.size() == 2) {
    const auto& a = triples.at(0);
    const auto& b = triples.at(1);
    if (!analyzeSimpleChain(view, a, b)) {
      patternFound = patternFound || analyzeSimpleChain(view, b, a);
    } else {
      patternFound = true;
    }
  }

  // TODO<ullingerc> Add support for other patterns, in particular, stars.

  // Remember predicates that appear in certain views, only if any pattern is
  // detected.
  if (patternFound) {
    for (const auto& triple : triples) {
      auto predicate = triple.getSimplePredicate();
      if (predicate.has_value()) {
        if (!predicateInView_.contains(predicate.value())) {
          predicateInView_[predicate.value()] = {};
        }
        predicateInView_[predicate.value()].push_back(view);
      }
    }
  }

  return patternFound;
}

}  // namespace materializedViewsQueryAnalysis
