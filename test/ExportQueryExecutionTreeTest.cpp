//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "engine/ExportQueryExecutionTrees.h"
#include "./IndexTestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/QueryPlanner.h"
#include "parser/SparqlParser.h"


TEST(ExportQueryExecutionTree, SelectQueryTSV) {
  auto qec = ad_utility::testing::getQec();
  QueryPlanner qp{qec};
  auto pq = SparqlParser::parseQuery("SELECT ?s ?p ?o WHERE {?s ?p ?o } ORDER BY ?s ?p ?o");
  auto qet = qp.createExecutionTree(pq);
  auto tsvGenerator = ExportQueryExecutionTrees::queryToStreamableGenerator(pq, qet, ad_utility::MediaType::tsv);
  std::string result;
  for (const auto& block : tsvGenerator) {
    result += block;
  }
  ASSERT_EQ(result, "SomeResult");
}