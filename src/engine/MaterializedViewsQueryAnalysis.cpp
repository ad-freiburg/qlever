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

  // TODO<ullingerc> Optimizations for stars.

  for (const auto& [tripleIdx, triple] :
       ::ranges::views::enumerate(triples._triples)) {
    if (std::holds_alternative<PropertyPath>(triple.p_)) {
      const auto& path = std::get<PropertyPath>(triple.p_);
      if (path.isIri()) {
        const auto& iri = path.getIri().toStringRepresentation();
        // If no view that we know of contains this predicate so we can ignore
        // this triple altogether.
        if (!predicateInView_.contains(iri)) {
          continue;
        }
        // Check for potential join chain triple.
        if (triple.o_.isVariable()) {
          if (triple.s_.isVariable()) {
            // This triple could be the right side of a chain join.
            chainRight[triple.s_.getVariable()].push_back(tripleIdx);
          }
          if (triple.s_ != triple.o_) {
            // This triple could be the left side of a chain join.
            chainLeft[triple.o_.getVariable()].push_back(tripleIdx);
          }
        }
      } else if (path.isSequence()) {
        AD_THROW(
            "Sequence property paths are expected to be replaced by joins "
            "during earlier stages of query planning.");
      }
    }
  }

  // Using the information collected by the pass over all triples, assemble all
  // chains that can potentially be rewritten.
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

        // We have already checked that this holds the correct alternative
        // above.
        const auto& leftP = std::get<PropertyPath>(left.p_);
        const auto& rightP = std::get<PropertyPath>(right.p_);

        if (!leftP.isIri() || !rightP.isIri()) {
          continue;
        }

        // Lookup key based on `std::string_view` avoids copying the IRIs.
        ChainedPredicatesForLookup key{
            leftP.getIri().toStringRepresentation(),
            rightP.getIri().toStringRepresentation()};

        // Lookup if there are matching views. There could potentially be
        // multiple (e.g. with different sorting).
        if (auto it = simpleChainCache_.find(key);
            it != simpleChainCache_.end()) {
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

  return result;
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
  auto [it, wasNew] = simpleChainCache_.try_emplace(preds, nullptr);
  if (it->second == nullptr) {
    it->second = std::make_shared<std::vector<ChainInfo>>();
  }
  it->second->push_back(
      ChainInfo{std::move(aSubj), std::move(chainVar), std::move(bObj), view});
  return true;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeView(ViewPtr view) {
  const auto& query = view->originalQuery();
  if (!query.has_value()) {
    return false;
  }

  auto explainIgnore = [&](const std::string& reason) {
    AD_LOG_INFO << "Materialized view '" << view->name()
                << "' will not be added to the query pattern cache for query "
                   "rewriting. Reason: "
                << reason << "." << std::endl;
  };

  // We do not need the `EncodedIriManager` because we are only interested in
  // analyzing the query structure, not in converting its components to
  // `ValueId`s.
  EncodedIriManager e;
  auto parsed = SparqlParser::parseQuery(&e, query.value(), {});

  auto graphPatternsFiltered = graphPatternInvariantFilter(parsed);
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
        predicateInView_[predicate.value()].push_back(view);
      }
    }
  }

  if (!patternFound) {
    explainIgnore("No supported query pattern for rewriting joins was found");
  }

  return patternFound;
}

// _____________________________________________________________________________
std::vector<parsedQuery::GraphPatternOperation>
QueryPatternCache::graphPatternInvariantFilter(const ParsedQuery& parsed) {
  BasicGraphPatternsInvariantTo invariantCheck{
      getVariablesPresentInBasicGraphPatterns(
          parsed._rootGraphPattern._graphPatterns)};

  // Filter out graph patterns that do not change the result of the basic graph
  // pattern analyzed.
  return ::ranges::to<std::vector>(parsed._rootGraphPattern._graphPatterns |
                                   ql::views::filter([&](const auto& pattern) {
                                     return !std::visit(invariantCheck,
                                                        pattern);
                                   }));
}

}  // namespace materializedViewsQueryAnalysis
