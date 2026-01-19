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

class MaterializedView;
class IndexScan;

namespace materializedViewsQueryAnalysis {

//
using ViewPtr = std::shared_ptr<const MaterializedView>;

//
using ChainedPredicates = std::pair<std::string, std::string>;
struct ChainInfo {
  Variable subject_;
  Variable chain_;
  Variable object_;
  ViewPtr view_;
};

//
ad_utility::HashSet<Variable> getVariablesPresentInBasicGraphPatterns(
    const std::vector<parsedQuery::GraphPatternOperation>& graphPatterns);

//
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

struct UserQueryChain {
  TripleComponent subject_;  // Allow fixing the subject of the chain
  Variable chain_;
  Variable object_;
  const std::vector<ChainInfo>& chainInfos_;
};

class QueryPatternCache {
  // Simple chains can be found by direct access into a hash map.
  ad_utility::HashMap<ChainedPredicates, std::vector<ChainInfo>>
      simpleChainCache_;

  // cache for predicates
  ad_utility::HashMap<std::string, std::vector<ViewPtr>> predicateInView_;

  // TODO cache for stars
 public:
  bool analyzeView(ViewPtr view);

  std::optional<UserQueryChain> checkSimpleChain(
      std::shared_ptr<IndexScan> left, std::shared_ptr<IndexScan> right) const;

 private:
  // checks only one direction, so call with a-b and b-a
  bool analyzeSimpleChain(ViewPtr view, const SparqlTriple& a,
                          const SparqlTriple& b);
};

}  // namespace materializedViewsQueryAnalysis

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
