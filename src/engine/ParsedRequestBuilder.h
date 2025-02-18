// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include "util/TypeIdentity.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

// Helper for parsing `HttpRequest` into `ParsedRequest`. The parsing has many
// common patterns but the details are slightly different. This struct
// stores the partially parsed `ParsedRequest` and methods for common
// operations used while parsing.
struct ParsedRequestBuilder {
  FRIEND_TEST(ParsedRequestBuilderTest, extractTargetGraph);
  FRIEND_TEST(ParsedRequestBuilderTest, determineAccessToken);
  FRIEND_TEST(ParsedRequestBuilderTest, parameterIsContainedExactlyOnce);

  ad_utility::url_parser::ParsedRequest parsedRequest_;

  // Initialize a `ParsedRequestBuilder`, parsing the request target into the
  // `ParsedRequest`.
  explicit ParsedRequestBuilder(
      const ad_utility::httpUtils::HttpRequest auto& request) {
    using namespace ad_utility::url_parser::sparqlOperation;
    // For an HTTP request, `request.target()` yields the HTTP Request-URI.
    // This is a concatenation of the URL path and the query strings.
    auto parsedUrl =
        ad_utility::url_parser::parseRequestTarget(request.target());
    parsedRequest_ = {std::move(parsedUrl.path_), std::nullopt,
                      std::move(parsedUrl.parameters_), None{}};
  }

  // Extract the access token from the access-token parameter or the
  // Authorization header and set it for `ParsedRequest`. If both are given,
  // then they must be the same.
  void extractAccessToken(
      const ad_utility::httpUtils::HttpRequest auto& request) {
    parsedRequest_.accessToken_ =
        determineAccessToken(request, parsedRequest_.parameters_);
  }

  // If applicable extract the dataset clauses from the parameters and set them
  // on the Query or Update.
  void extractDatasetClauses();

  // If the parameter is set, set the operation with the parameter's value as
  // operation string and empty dataset clauses. Setting an operation when one
  // is already set is an error. Note: processed parameters are removed from the
  // parameter map.
  template <typename Operation>
  void extractOperationIfSpecified(ad_utility::use_type_identity::TI<Operation>,
                                   string_view paramName);

  // Returns whether the request is a Graph Store operation.
  bool isGraphStoreOperation() const;

  // Set the operation to the parsed Graph Store operation.
  void extractGraphStoreOperation();

  // Returns whether the parameters contain a parameter with the given key.
  bool parametersContain(std::string_view param) const;

  // Check that requests don't both have these content types and are Graph
  // Store operations.
  void reportUnsupportedContentTypeIfGraphStore(
      std::string_view contentType) const;

  // Move the `ParsedRequest` out when parsing is finished.
  ad_utility::url_parser::ParsedRequest build() &&;

 private:
  // Adds a dataset clause to the operation if it is of the given type. The
  // dataset clause's IRI is the value of parameter `key`. The `isNamed_` of the
  // dataset clause is as given.
  template <typename Operation>
  void extractDatasetClauseIfOperationIs(
      ad_utility::use_type_identity::TI<Operation>, const std::string& key,
      bool isNamed);

  // Check that a parameter is contained exactly once. An exception is thrown if
  // a parameter is contained more than once.
  bool parameterIsContainedExactlyOnce(std::string_view key) const;

  // Extract the graph to be acted upon using from the URL query parameters
  // (`Indirect Graph Identification`). See
  // https://www.w3.org/TR/2013/REC-sparql11-http-rdf-update-20130321/#indirect-graph-identification
  static GraphOrDefault extractTargetGraph(
      const ad_utility::url_parser::ParamValueMap& params);

  // Determine the access token from the parameters and the requests
  // Authorization header.
  static std::optional<std::string> determineAccessToken(
      const ad_utility::httpUtils::HttpRequest auto& request,
      const ad_utility::url_parser::ParamValueMap& params) {
    namespace http = boost::beast::http;
    std::optional<std::string> tokenFromAuthorizationHeader;
    std::optional<std::string> tokenFromParameter;
    if (request.find(http::field::authorization) != request.end()) {
      string_view authorization = request[http::field::authorization];
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
    return tokenFromAuthorizationHeader
               ? std::move(tokenFromAuthorizationHeader)
               : std::move(tokenFromParameter);
  }
};
