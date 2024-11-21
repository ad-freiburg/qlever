// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <gtest/gtest_prod.h>

#include "parser/ParsedQuery.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

class GraphStoreProtocol {
 public:
  // Every Graph Store Protocol requests has equivalent SPARQL Query or Update.
  // Transform the Graph Store Protocol request into it's equivalent Query or
  // Update.
  static ParsedQuery transformGraphStoreProtocol(
      const ad_utility::httpUtils::HttpRequest auto& rawRequest,
      const ad_utility::url_parser::ParsedRequest& request);

 private:
  // Extract the graph to be acted upon using `Indirect Graph
  // Identification`.
  static GraphOrDefault extractTargetGraph(
      const ad_utility::url_parser::ParamValueMap& params);
  FRIEND_TEST(GraphStoreProtocolTest, extractTargetGraph);
};
