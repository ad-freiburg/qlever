// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/GraphStoreProtocol.h"

#include <boost/beast.hpp>

// ____________________________________________________________________________
GraphOrDefault GraphStoreProtocol::extractTargetGraph(
    const ad_utility::url_parser::ParamValueMap& params) {
  // Extract the graph to be acted upon using `Indirect Graph
  // Identification`.
  const auto graphIri =
      ad_utility::url_parser::checkParameter(params, "graph", std::nullopt);
  const auto isDefault =
      ad_utility::url_parser::checkParameter(params, "default", "").has_value();
  if (!(graphIri.has_value() || isDefault)) {
    throw std::runtime_error("No graph IRI specified in the request.");
  }
  if (graphIri.has_value() && isDefault) {
    throw std::runtime_error(
        "Only one of `default` and `graph` may be used for graph "
        "identification.");
  }
  if (graphIri.has_value()) {
    return GraphRef::fromIrirefWithoutBrackets(graphIri.value());
  } else {
    AD_CORRECTNESS_CHECK(isDefault);
    return DEFAULT{};
  }
}
