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
#include "engine/VariableToColumnMap.h"
#include "index/ExportIds.h"
#include "index/TripleComponentConversions.h"
#include "parser/GraphPatternOperation.h"
#include "parser/VariableCounter.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
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

// Call `func` for the subject and the object of `triple`, if they are the
// given `variable`. The predicate is deliberately not visited: a variable
// predicate cannot be substituted by a `TripleComponent` (see
// `substituteFirstColumn`, which detects such an occurrence because it is
// counted by the `VariableCounter` but not here).
template <typename Func>
void forEachMatchingSide(SparqlTriple& triple, const Variable& variable,
                         Func func) {
  for (TripleComponent* side : {&triple.s_, &triple.o_}) {
    if (side->isVariable() && side->getVariable() == variable) {
      func(*side);
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
    for (bool isSubject : {true, false}) {
      const auto& side = isSubject ? triple.s_ : triple.o_;
      if (side.isVariable() && side.getVariable() == variable) {
        anchors.push_back({predicate.value(), isSubject});
      }
    }
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

}  // namespace

// _____________________________________________________________________________
std::vector<MaterializedViewJoinReplacement>
QueryPatternCache::makeJoinReplacementIndexScans(
    QueryExecutionContext* qec,
    const parsedQuery::BasicGraphPattern& triples) const {
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
        for (const auto& chainInfo : *(it->second)) {
          // If the subject of the chain is fixed, but the subject is not the
          // first column of the view, rewriting cannot be applied.
          if (!left.s_.isVariable() && chainInfo.view_->variableToColumnMap()
                                               .at(chainInfo.subject_)
                                               .columnIndex_ != 0) {
            continue;
          }
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

  // All three variables must actually be columns of the view (e.g. this is
  // not the case if they do not appear in the `SELECT` clause).
  const auto& viewCols = view->variableToColumnMap();
  if (!viewCols.contains(aSubj) || !viewCols.contains(chainVar) ||
      !viewCols.contains(bObj)) {
    return false;
  }

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

  if (parsed.value().isAggregatingQuery()) {
    explainIgnore(
        "The view's query aggregates (GROUP BY, either explicit or implicit "
        "via an aggregate expression in the SELECT clause)");
    return cacheKeyAdded;
  }

  auto graphPatternsFiltered = graphPatternInvariantFilter(parsed.value());
  if (graphPatternsFiltered.size() != 1) {
    explainIgnore(
        "The view has more than one graph pattern (even after skipping ignored "
        "patterns)");
    return cacheKeyAdded;
  }
  const auto& graphPattern = graphPatternsFiltered.at(0);
  if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(graphPattern)) {
    explainIgnore("The graph pattern is not a basic set of triples");
    return cacheKeyAdded;
  }
  // TODO<ullingerc> Property path is stored as a single predicate here.
  const auto& triples = graphPattern.getBasic()._triples;
  if (triples.size() == 0) {
    explainIgnore("The query body is empty");
    return cacheKeyAdded;
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
                        [&](TripleComponent&) { ++numSubstituted; });
  });
  if (numSubstituted == 0 || !totalCount.has_value() ||
      totalCount.value() != numSubstituted) {
    return false;
  }

  forEachTopLevelTriple(parsed, [&](SparqlTriple& triple) {
    forEachMatchingSide(triple, variable,
                        [&value](TripleComponent& side) { side = value; });
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
