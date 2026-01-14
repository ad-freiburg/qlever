// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_

#include "rdfTypes/Variable.h"
#include "util/HashMap.h"

class MaterializedView;

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

class QueryPatternCache {
  // Simple chains can be found by direct access into a hash map.
  ad_utility::HashMap<ChainedPredicates, std::vector<ChainInfo>>
      simpleChainCache_;

  // cache for predicates
  ad_utility::HashMap<std::string, std::vector<ViewPtr>> predicateInView_;

  // TODO cache for stars
 public:
  void analyzeView(ViewPtr view);
};

}  // namespace materializedViewsQueryAnalysis

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSQUERYANALYSIS_H_
