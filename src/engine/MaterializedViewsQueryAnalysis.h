// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_

#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/Variable.h"
#include "util/HashMap.h"
#include "util/TypeTraits.h"

// Forward declarations to prevent cyclic dependencies.
class MaterializedView;
class IndexScan;

// _____________________________________________________________________________
namespace ad_utility::detail {

using StringPair = std::pair<std::string, std::string>;
using StringViewPair = std::pair<std::string_view, std::string_view>;

// _____________________________________________________________________________
struct StringPairHash {
  // Allows looking up values from a hash map with `StringPair` keys also with
  // `StringViewPair`.
  using is_transparent = void;

  size_t operator()(const StringPair& p) const {
    return absl::HashOf(p.first, p.second);
  }

  size_t operator()(const StringViewPair& p) const {
    return absl::HashOf(p.first, p.second);
  }
};

// _____________________________________________________________________________
struct StringPairEq {
  using is_transparent = void;

  bool operator()(const StringPair& a, const StringPair& b) const {
    return a == b;
  }

  bool operator()(const StringPair& a, const StringViewPair& b) const {
    return a.first == b.first && a.second == b.second;
  }

  bool operator()(const StringViewPair& a, const StringPair& b) const {
    return b.first == a.first && b.second == a.second;
  }
};

}  // namespace ad_utility::detail

// _____________________________________________________________________________
namespace materializedViewsQueryAnalysis {

using ViewPtr = std::shared_ptr<const MaterializedView>;

// Key and value types of the cache for simple chains, that is queries of the
// form `?s <p1> ?m . ?m <p2> ?o`.
using ChainedPredicates = ad_utility::detail::StringPair;
using ChainedPredicatesForLookup = ad_utility::detail::StringViewPair;
struct ChainInfo {
  Variable subject_;
  Variable chain_;
  Variable object_;
  ViewPtr view_;
};

// Extract all variables present in a set of graph patterns. Required for
// `BasicGraphPatternsInvariantTo` below.
ad_utility::HashSet<Variable> getVariablesPresentInBasicGraphPatterns(
    const std::vector<parsedQuery::GraphPatternOperation>& graphPatterns);

// Check whether certain graph patterns can be ignored as they do not affect the
// result of a query when we are only interested in the bindings for variables
// from `variables_`.
struct BasicGraphPatternsInvariantTo {
  ad_utility::HashSet<Variable> variables_;

  bool operator()(const parsedQuery::Optional& optional) const;
  bool operator()(const parsedQuery::Bind& bind) const;
  bool operator()(const parsedQuery::Values& values) const;

  CPP_template(typename T)(requires(
      !ad_utility::SimilarToAny<T, parsedQuery::Optional, parsedQuery::Bind,
                                parsedQuery::Values>)) bool
  operator()(const T&) const {
    return false;
  }
};

// Similar to `ChainInfo`, this struct represents a simple chain, however it may
// bind the subject.
struct UserQueryChain {
  TripleComponent subject_;  // Allow fixing the subject of the chain.
  Variable chain_;
  Variable object_;
  std::shared_ptr<const std::vector<ChainInfo>> chainInfos_;
};

//
struct MaterializedViewJoinReplacement {
  std::shared_ptr<IndexScan> indexScan_;
  std::vector<size_t> coveredTriples_;

  size_t numJoins() const { return coveredTriples_.size() - 1; }
};

// Cache data structure for the `MaterializedViewsManager`. This object can be
// used for quickly looking up if a given query can be optimized by making use
// of an existing materialized view.
class QueryPatternCache {
  // Simple chains can be found by direct access into a hash map.
  ad_utility::HashMap<
      ChainedPredicates, std::shared_ptr<std::vector<ChainInfo>>,
      ad_utility::detail::StringPairHash, ad_utility::detail::StringPairEq>
      simpleChainCache_;

  // Cache for predicates appearing in a materialized view.
  ad_utility::HashMap<std::string, std::vector<ViewPtr>> predicateInView_;

  // TODO<ullinger> Data structure for join stars.
 public:
  // Given a materialized view, analyze its write query and populate the cache.
  // This is called from `MaterializedViewsManager::loadView`.
  bool analyzeView(ViewPtr view);

  //
  std::vector<MaterializedViewJoinReplacement> makeJoinReplacementIndexScans(
      QueryExecutionContext* qec,
      const parsedQuery::BasicGraphPattern& triples) const;

  std::shared_ptr<IndexScan> makeScanForSingleChain(
      QueryExecutionContext* qec, ChainInfo cached, TripleComponent subject,
      std::optional<Variable> chain, Variable object) const;

 private:
  // Helper for `analyzeView`, that checks for a simple chain. It returns `true`
  // iff a simple chain `a->b` is present.
  // NOTE: This function only checks one direction, so it should also be called
  // with `a` and `b` switched if it returns `false`.
  bool analyzeSimpleChain(ViewPtr view, const SparqlTriple& a,
                          const SparqlTriple& b);
};

}  // namespace materializedViewsQueryAnalysis

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
