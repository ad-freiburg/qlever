
// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/SPARQLProtocol.h"

// ____________________________________________________________________________
GraphOrDefault SPARQLProtocol::extractTargetGraph(
    const ad_utility::url_parser::ParamValueMap& params) {
  const std::optional<std::string> graphIri =
      ad_utility::url_parser::checkParameter(params, "graph", std::nullopt);
  const bool isDefault =
      ad_utility::url_parser::checkParameter(params, "default", "").has_value();
  if (graphIri.has_value() == isDefault) {
    throw std::runtime_error(
        "Exactly one of the query parameters `default` or `graph` must be set "
        "to identify the graph for the graph store protocol request.");
  }
  if (graphIri.has_value()) {
    return GraphRef::fromIrirefWithoutBrackets(graphIri.value());
  } else {
    AD_CORRECTNESS_CHECK(isDefault);
    return DEFAULT{};
  }
}
