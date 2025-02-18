
// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/SPARQLProtocol.h"

using namespace ad_utility::use_type_identity;
namespace http = boost::beast::http;
using namespace ad_utility::url_parser::sparqlOperation;

// ____________________________________________________________________________
void ParsedRequestBuilder::extractDatasetClauses() {
  extractDatasetClauseIfOperationIs(ti<Query>, "default-graph-uri", false);
  extractDatasetClauseIfOperationIs(ti<Query>, "named-graph-uri", true);
  extractDatasetClauseIfOperationIs(ti<Update>, "using-graph-uri", false);
  extractDatasetClauseIfOperationIs(ti<Update>, "using-named-graph-uri", true);
}

// ____________________________________________________________________________
bool ParsedRequestBuilder::parameterIsContainedExactlyOnce(
    std::string_view key) const {
  return ad_utility::url_parser::getParameterCheckAtMostOnce(
             parsedRequest_.parameters_, key)
      .has_value();
}

// ____________________________________________________________________________
bool ParsedRequestBuilder::isGraphStoreOperation() const {
  return parameterIsContainedExactlyOnce("graph") ||
         parameterIsContainedExactlyOnce("default");
}

// ____________________________________________________________________________
void ParsedRequestBuilder::extractGraphStoreOperation() {
  // SPARQL Graph Store HTTP Protocol with indirect graph identification
  if (parameterIsContainedExactlyOnce("graph") &&
      parameterIsContainedExactlyOnce("default")) {
    throw std::runtime_error(
        R"(Parameters "graph" and "default" must not be set at the same time.)");
  }
  AD_CORRECTNESS_CHECK(std::holds_alternative<None>(parsedRequest_.operation_));
  // We only support passing the target graph as a query parameter
  // (`Indirect Graph Identification`). `Direct Graph Identification` (the
  // URL is the graph) is not supported. See also
  // https://www.w3.org/TR/2013/REC-sparql11-http-rdf-update-20130321/#graph-identification.
  parsedRequest_.operation_ =
      GraphStoreOperation{extractTargetGraph(parsedRequest_.parameters_)};
}

// ____________________________________________________________________________
bool ParsedRequestBuilder::parametersContain(std::string_view param) const {
  return parsedRequest_.parameters_.contains(param);
}

// ____________________________________________________________________________
ad_utility::url_parser::ParsedRequest ParsedRequestBuilder::build() && {
  return std::move(parsedRequest_);
}

// ____________________________________________________________________________
void ParsedRequestBuilder::reportUnsupportedContentTypeIfGraphStore(
    std::string_view contentType) const {
  if (isGraphStoreOperation()) {
    throw std::runtime_error(absl::StrCat("Unsupported Content type \"",
                                          contentType,
                                          "\" for Graph Store protocol."));
  }
}

// ____________________________________________________________________________
template <typename Operation>
void ParsedRequestBuilder::extractDatasetClauseIfOperationIs(
    TI<Operation>, const std::string& key, bool isNamed) {
  if (Operation* op = std::get_if<Operation>(&parsedRequest_.operation_)) {
    ad_utility::appendVector(op->datasetClauses_,
                             ad_utility::url_parser::parseDatasetClausesFrom(
                                 parsedRequest_.parameters_, key, isNamed));
  }
}

// ____________________________________________________________________________
template <typename Operation>
void ParsedRequestBuilder::extractOperationIfSpecified(TI<Operation>,
                                                       string_view paramName) {
  auto operation = ad_utility::url_parser::getParameterCheckAtMostOnce(
      parsedRequest_.parameters_, paramName);
  if (operation.has_value()) {
    AD_CORRECTNESS_CHECK(
        std::holds_alternative<None>(parsedRequest_.operation_));
    parsedRequest_.operation_ = Operation{operation.value(), {}};
    parsedRequest_.parameters_.erase(paramName);
  }
}

template void ParsedRequestBuilder::extractOperationIfSpecified(
    TI<Query>, string_view paramName);
template void ParsedRequestBuilder::extractOperationIfSpecified(
    TI<Update>, string_view paramName);

// ____________________________________________________________________________
GraphOrDefault ParsedRequestBuilder::extractTargetGraph(
    const ad_utility::url_parser::ParamValueMap& params) {
  const std::optional<std::string> graphIri =
      ad_utility::url_parser::checkParameter(params, "graph", std::nullopt);
  const bool isDefault =
      ad_utility::url_parser::checkParameter(params, "default", "").has_value();
  if (graphIri.has_value() == isDefault) {
    throw std::runtime_error(
        R"(Exactly one of the query parameters "default" or "graph" must be set to identify the graph for the graph store protocol request.)");
  }
  if (graphIri.has_value()) {
    return GraphRef::fromIrirefWithoutBrackets(graphIri.value());
  } else {
    AD_CORRECTNESS_CHECK(isDefault);
    return DEFAULT{};
  }
}

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
    parsedRequestBuilder.extractOperationIfSpecified(
        ad_utility::use_type_identity::ti<
            ad_utility::url_parser::sparqlOperation::Query>,
        "query");
    parsedRequestBuilder.extractDatasetClauses();
  }
  return std::move(parsedRequestBuilder).build();
}
