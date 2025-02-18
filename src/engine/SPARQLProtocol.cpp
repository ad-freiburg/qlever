
// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/SPARQLProtocol.h"

using namespace ad_utility::use_type_identity;
using namespace ad_utility::url_parser::sparqlOperation;

// ____________________________________________________________________________
ad_utility::url_parser::ParsedRequest SPARQLProtocol::parseGET(
    ParsedRequestBuilder parsedRequestBuilder) {
  const bool isQuery = parsedRequestBuilder.parametersContain("query");
  if (parsedRequestBuilder.parametersContain("update")) {
    throw std::runtime_error("SPARQL Update is not allowed as GET request.");
  }
  if (parsedRequestBuilder.isGraphStoreOperation()) {
    if (isQuery) {
      throw std::runtime_error(
          R"(Request contains parameters for both a SPARQL Query ("query") and a Graph Store Protocol operation ("graph" or "default").)");
    }
    // SPARQL Graph Store HTTP Protocol with indirect graph identification
    parsedRequestBuilder.extractGraphStoreOperation();
  } else if (isQuery) {
    // SPARQL Query
    parsedRequestBuilder.extractOperationIfSpecified(ti<Query>, "query");
    parsedRequestBuilder.extractDatasetClauses();
  }
  return std::move(parsedRequestBuilder).build();
}
