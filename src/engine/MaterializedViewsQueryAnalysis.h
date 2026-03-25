// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_

#include "engine/VariableToColumnMap.h"
#include "parser/GraphPatternAnalysis.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/Variable.h"
#include "util/StringPairHashMap.h"

// Forward declarations to prevent cyclic dependencies.
class MaterializedView;
class IndexScan;

// _____________________________________________________________________________
namespace materializedViewsQueryAnalysis {

using ViewPtr = std::shared_ptr<const MaterializedView>;
using graphPatternAnalysis::BasicGraphPatternsInvariantTo;
using VariableToTripleIndices =
    ad_utility::HashMap<Variable, std::vector<size_t>>;

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
using SimpleChainCache =
    ad_utility::StringPairHashMap<std::shared_ptr<std::vector<ChainInfo>>>;

// Types required to stored cached join star patterns extracted from views. That
// is, queries of the form `?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p3> ?o3 ...`.
// The `StarInfo` holds the subject variable shared between all arms of the star
// and the `StarArm` for each of them. The `StarArm` stores the predicate IRI
// and object variable.
using StarArm = std::pair<std::string, Variable>;
struct StarInfo {
  Variable subject_;
  std::vector<StarArm> arms_;
};

// Helper class that represents a possible join replacement and indicates the
// subset of triples it handles.
struct MaterializedViewJoinReplacement {
  std::shared_ptr<IndexScan> indexScan_;
  std::vector<size_t> coveredTriples_;

  // ___________________________________________________________________________
  size_t numJoins() const { return coveredTriples_.size() - 1; }
};

// Cache data structure for the `MaterializedViewsManager`. This object can be
// used for quickly looking up if a given query can be optimized by making use
// of an existing materialized view.
class QueryPatternCache {
 private:
  // Simple chains can be found by direct access into a hash map.
  SimpleChainCache simpleChainCache_;

  // Cache for predicates appearing in a materialized view. The `ViewPtr`s are
  // kept sorted.
  ad_utility::HashMap<std::string, std::vector<ViewPtr>> predicateInView_;

  // All star patterns extracted from materialized views.
  ad_utility::HashMap<ViewPtr, StarInfo> starCache_;

  // NOTE: When a new data structure for caching is added here, the unloading
  // should also be implemented in the `removeView` method.
 public:
  // Given a materialized view, analyze the query it was created from and
  // populate the cache. This is called from
  // `MaterializedViewsManager::loadView`.
  bool analyzeView(ViewPtr view);

  // Remove all pointers to a view from this `QueryPatternCache`. This is
  // required for unloading materialized views. A call to this function with a
  // `ViewPtr` that is not cached is a no-op.
  void removeView(ViewPtr view);

  // Given a set of triples, check if a subset of necessary join operations can
  // be replaced by scans on materialized views.
  std::vector<MaterializedViewJoinReplacement> makeJoinReplacementIndexScans(
      QueryExecutionContext* qec,
      const parsedQuery::BasicGraphPattern& triples) const;

  // Construct an `IndexScan` for a single chain join given the necessary
  // information from both the materialized view and the user's query.
  std::shared_ptr<IndexScan> makeScanForSingleChain(
      QueryExecutionContext* qec, ChainInfo cached, TripleComponent subject,
      std::optional<Variable> chain, Variable object) const;

  // Construct an `IndexScan` for a star join given the `RequestedColumns`
  // object that maps the columns of the materialized view to the subject and
  // object variable names from the user query. This assumes that the `starView`
  // represents the appropriate star join.
  std::shared_ptr<IndexScan> makeScanForStar(
      QueryExecutionContext* qec, ViewPtr starView,
      parsedQuery::MaterializedViewQuery::RequestedColumns columns) const;

 private:
  // Helper for `analyzeView`, that checks for a simple chain. It returns `true`
  // iff a simple chain `a->b` is present.
  // NOTE: This function only checks one direction, so it should also be called
  // with `a` and `b` switched if it returns `false`.
  bool analyzeSimpleChain(ViewPtr view, const SparqlTriple& a,
                          const SparqlTriple& b);

  // Helper for `analyzeView`, that checks for a join star of arbitrary size.
  //
  // Given at least two triples of a view with one `BasicGraphPattern`, check if
  // the view represents a join star. If yes, add the view to the cache for join
  // stars. This function returns `true` iff the view contains a star.
  //
  // A star requires all triples to share the same subject variable with
  // distinct simple IRI predicates and distinct variable objects.
  bool analyzeJoinStar(ViewPtr view, const std::vector<SparqlTriple>& triples);

  // Given potential left and right sides of simple chains, check for available
  // replacement index scans, construct them and insert them into the `result`
  // vector.
  void makeScansFromChainCandidates(
      QueryExecutionContext* qec, const parsedQuery::BasicGraphPattern& triples,
      std::vector<MaterializedViewJoinReplacement>& result,
      const VariableToTripleIndices& chainLeft,
      const VariableToTripleIndices& chainRight) const;

  // Given triples grouped by subject, check for available star join replacement
  // index scans, construct them and insert them into the `result` vector.
  void makeScansFromStarCandidates(
      QueryExecutionContext* qec, const parsedQuery::BasicGraphPattern& triples,
      std::vector<MaterializedViewJoinReplacement>& result,
      const VariableToTripleIndices& starCandidates) const;
};

// Helper that filters the graph patterns of a parsed query using
// `BasicGraphPatternInvariantTo`. For details, see the documentation for this
// helper.
std::vector<parsedQuery::GraphPatternOperation> graphPatternInvariantFilter(
    const ParsedQuery& parsed);

// Hash map for the `BIND`-to-column map.
using BindExpressionAndTargetCol = ad_utility::HashMap<std::string, size_t>;

// Extract all `BIND` statements from a `ParsedQuery` and create a hash map
// mapping `BIND` expression cache keys to target variable column index.
BindExpressionAndTargetCol extractBindExpressions(
    const ParsedQuery& parsed, const VariableToColumnMap& varToColMap);

}  // namespace materializedViewsQueryAnalysis

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
