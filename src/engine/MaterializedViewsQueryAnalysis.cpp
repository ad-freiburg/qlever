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
#include "parser/PropertyPath.h"
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
std::vector<MaterializedViewJoinReplacement>
QueryPatternCache::makeJoinReplacementIndexScans(
    QueryExecutionContext* qec,
    const parsedQuery::BasicGraphPattern& triples) const {
  std::vector<MaterializedViewJoinReplacement> result;

  // All triples of the form `anything <iri> ?variable` where `<iri>` is covered
  // by a materialized view, stored by `?variable` for finding chains.
  ad_utility::HashMap<Variable, std::vector<size_t>> chainLeft;
  // All triples of the form `?variable <iri> ?otherVariable` where `<iri>` is
  // covered by a materialized view, where `?variable` is different from
  // `?otherVariable`, stored by `?variable` for finding chains.
  ad_utility::HashMap<Variable, std::vector<size_t>> chainRight;

  for (const auto& [i, triple] : ::ranges::views::enumerate(triples._triples)) {
    if (std::holds_alternative<PropertyPath>(triple.p_)) {
      const auto& path = std::get<PropertyPath>(triple.p_);
      if (path.isIri()) {
        const auto& iri = path.getIri().toStringRepresentation();
        // If no view that we know of contains this predicate so we can ignore
        // this triple altogether.
        if (!predicateInView_.contains(iri)) {
          continue;
        }
        if (triple.s_.isVariable() && triple.o_.isVariable()) {
          // This triple could be the right side of a chain join.
          chainRight[triple.s_.getVariable()].push_back(i);
        }
        if (triple.o_.isVariable() && triple.s_ != triple.o_) {
          // This triple could be the left side of a chain join.
          chainLeft[triple.o_.getVariable()].push_back(i);
        }
      } else if (path.isSequence()) {
        // CHECK THIS - This doesn't seem to occur in practice
        AD_THROW("This should not happen");
        /*const auto& seq = path.getSequence();
         if (seq.size() == 2 && seq.at(0).isIri() && seq.at(1).isIri() &&
             triple.o_.isVariable()) {
           const auto& chainIriLeft =
               seq.at(0).getIri().toStringRepresentation();
           const auto& chainIriRight =
               seq.at(1).getIri().toStringRepresentation();
           // TODO use std::string_view version to avoid copy - `is_transparent`
           // hash for hash map
           ChainedPredicates key{chainIriLeft, chainIriRight};
           if (auto it = simpleChainCache_.find(key);
               it != simpleChainCache_.end()) {
             auto& chainInfoPtr = it->second;
             for (const auto& chainInfo : *chainInfoPtr) {
               result.push_back({makeScanForSingleChain(qec, chainInfo,
                                                        triple.s_, std::nullopt,
                                                        triple.o_.getVariable()),
                                 {static_cast<size_t>(i)}});
             }
           }
         }*/
      }
    }
  }

  for (const auto& [varLeft, triplesLeft] : chainLeft) {
    if (!chainRight.contains(varLeft)) {
      continue;
    }
    for (auto tripleIdxRight : chainRight.at(varLeft)) {
      for (auto tripleIdxLeft : triplesLeft) {
        const auto& left = triples._triples.at(tripleIdxLeft);
        const auto& right = triples._triples.at(tripleIdxRight);

        const auto& leftP = std::get<PropertyPath>(left.p_);
        const auto& rightP = std::get<PropertyPath>(right.p_);

        if (!leftP.isIri() || !rightP.isIri()) {
          continue;
        }

        const auto& chainIriLeft = leftP.getIri().toStringRepresentation();
        const auto& chainIriRight = rightP.getIri().toStringRepresentation();

        //
        ChainedPredicatesForLookup key{chainIriLeft, chainIriRight};
        if (auto it = simpleChainCache_.find(key);
            it != simpleChainCache_.end()) {
          auto& chainInfoPtr = it->second;
          for (const auto& chainInfo : *chainInfoPtr) {
            result.push_back(
                {makeScanForSingleChain(qec, chainInfo, left.s_, varLeft,
                                        right.o_.getVariable()),
                 {tripleIdxLeft, tripleIdxRight}});
          }
        }
      }
    }
  }

  AD_LOG_INFO << "MV PLANS: " << result.size() << std::endl;
  return result;
}

// _____________________________________________________________________________
std::shared_ptr<IndexScan> QueryPatternCache::makeScanForSingleChain(
    QueryExecutionContext* qec, ChainInfo cached, TripleComponent subject,
    std::optional<Variable> chain, Variable object) const {
  auto& [cSubj, cChain, cObj, view] = cached;
  parsedQuery::MaterializedViewQuery::RequestedColumns cols{
      {std::move(cSubj), std::move(subject)},
      {std::move(cObj), std::move(object)},
  };
  if (chain.has_value()) {
    cols.insert({std::move(cChain), std::move(chain.value())});
  }
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

  // Insert chain to cache.
  // TODO avoid hashing 3x
  ChainedPredicates preds{aPred.value(), bPred.value()};
  if (!simpleChainCache_.contains(preds)) {
    simpleChainCache_[preds] = std::make_shared<std::vector<ChainInfo>>();
  }
  simpleChainCache_[preds]->push_back(
      ChainInfo{std::move(aSubj), std::move(chainVar), std::move(bObj), view});
  return true;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeView(ViewPtr view) {
  AD_LOG_INFO << view->name() << std::endl;
  const auto& query = view->originalQuery();
  if (!query.has_value()) {
    return false;
  }

  // We do not need the `EncodedIriManager` because we are only interested in
  // analyzing the query structure, not in converting its components to
  // `ValueId`s.
  EncodedIriManager e;
  auto parsed = SparqlParser::parseQuery(&e, query.value(), {});

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
