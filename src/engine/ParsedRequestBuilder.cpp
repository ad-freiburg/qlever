// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "ParsedRequestBuilder.h"

using namespace ad_utility::url_parser::sparqlOperation;

// ____________________________________________________________________________
ParsedRequestBuilder::ParsedRequestBuilder(const RequestType& request) {
  using namespace ad_utility::url_parser::sparqlOperation;
  // For an HTTP request, `request.target()` yields the HTTP Request-URI.
  // This is a concatenation of the URL path and the query strings.
  auto parsedUrl = ad_utility::url_parser::parseRequestTarget(request.target());
  parsedRequest_ = {std::move(parsedUrl.path_), std::nullopt,
                    std::move(parsedUrl.parameters_), None{}};
}

// ____________________________________________________________________________
void ParsedRequestBuilder::extractAccessToken(const RequestType& request) {
  parsedRequest_.accessToken_ =
      determineAccessToken(request, parsedRequest_.parameters_);
}

// ____________________________________________________________________________
void ParsedRequestBuilder::extractDatasetClauses() {
  extractDatasetClauseIfOperationIs<Query>("default-graph-uri", false);
  extractDatasetClauseIfOperationIs<Query>("named-graph-uri", true);
  extractDatasetClauseIfOperationIs<Update>("using-graph-uri", false);
  extractDatasetClauseIfOperationIs<Update>("using-named-graph-uri", true);
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
    const std::string& key, bool isNamed) {
  if (Operation* op = std::get_if<Operation>(&parsedRequest_.operation_)) {
    ad_utility::appendVector(op->datasetClauses_,
                             ad_utility::url_parser::parseDatasetClausesFrom(
                                 parsedRequest_.parameters_, key, isNamed));
  }
}

// ____________________________________________________________________________
template <typename Operation>
void ParsedRequestBuilder::extractOperationIfSpecified(
    std::string_view paramName) {
  auto operation = ad_utility::url_parser::getParameterCheckAtMostOnce(
      parsedRequest_.parameters_, paramName);
  if (operation.has_value()) {
    AD_CORRECTNESS_CHECK(
        std::holds_alternative<None>(parsedRequest_.operation_));
    parsedRequest_.operation_ = Operation{operation.value(), {}};
    parsedRequest_.parameters_.erase(paramName);
  }
}

template void ParsedRequestBuilder::extractOperationIfSpecified<Query>(
    std::string_view paramName);
template void ParsedRequestBuilder::extractOperationIfSpecified<Update>(
    std::string_view paramName);

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
std::optional<std::string> ParsedRequestBuilder::determineAccessToken(
    const RequestType& request,
    const ad_utility::url_parser::ParamValueMap& params) {
  namespace http = boost::beast::http;
  std::optional<std::string> tokenFromAuthorizationHeader;
  std::optional<std::string> tokenFromParameter;
  if (request.find(http::field::authorization) != request.end()) {
    std::string_view authorization = request[http::field::authorization];
    const std::string prefix = "Bearer ";
    if (!authorization.starts_with(prefix)) {
      throw std::runtime_error(absl::StrCat(
          "Authorization header doesn't start with \"", prefix, "\"."));
    }
    authorization.remove_prefix(prefix.length());
    tokenFromAuthorizationHeader = std::string(authorization);
  }
  if (params.contains("access-token")) {
    tokenFromParameter = ad_utility::url_parser::getParameterCheckAtMostOnce(
        params, "access-token");
  }
  // If both are specified, they must be equal. This way there is no hidden
  // precedence.
  if (tokenFromAuthorizationHeader && tokenFromParameter &&
      tokenFromAuthorizationHeader != tokenFromParameter) {
    throw std::runtime_error(
        "Access token is specified both in the `Authorization` header and by "
        "the `access-token` parameter, but they are not the same");
  }
  return tokenFromAuthorizationHeader ? std::move(tokenFromAuthorizationHeader)
                                      : std::move(tokenFromParameter);
}
