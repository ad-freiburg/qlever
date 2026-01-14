// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/MaterializedViewsQueryAnalysis.h"

#include <variant>

#include "engine/MaterializedViews.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlParser.h"

namespace materializedViewsQueryAnalysis {

// _____________________________________________________________________________
void QueryPatternCache::analyzeView(ViewPtr view) {
  AD_LOG_INFO << view->name() << std::endl;
  auto q = view->originalQuery();
  EncodedIriManager e;  // TODO currently we dont use this
  auto parsed = SparqlParser::parseQuery(&e, q, {});

  // parsed._rootGraphPattern._graphPatterns.at(0)
  //     .getBasic()
  //     ._triples.at(0)
  //     .asString()
  auto graphPatterns = ::ranges::to<std::vector>(
      parsed._rootGraphPattern._graphPatterns |
      ql::views::filter([](const auto& pattern) {
        // it should be safe to ignore certain kinds of graph patterns like
        // BIND, VALUES, OPTIONAL (where the values of the other cols dont
        // change and no rows are omitted, only possibly repeated)
        return !std::holds_alternative<parsedQuery::Optional>(pattern) &&
               !std::holds_alternative<parsedQuery::Bind>(pattern) &&
               !std::holds_alternative<parsedQuery::Values>(pattern);
      }));
  if (graphPatterns.size() != 1) {
    return;
  }
  const auto& graphPattern = graphPatterns.at(0);
  if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(graphPattern)) {
    return;
  }
  const auto& triples = graphPattern.getBasic()._triples;
  if (triples.size() == 0) {
    return;
  }
  // TODO Property path is stored as a single predicate here
  AD_LOG_INFO << triples.size() << std::endl;
  if (triples.size() == 2) {
    // Could be chain
  }
  // Predicate in view
  for (const auto& triple : triples) {
    auto predicate = triple.getSimplePredicate();
    if (predicate.has_value()) {
      if (!predicateInView_.contains(predicate.value())) {
        predicateInView_[predicate.value()] = {};
      }
      predicateInView_[predicate.value()].push_back(view);
    }
  }
  // TODO other
}

}  // namespace materializedViewsQueryAnalysis
