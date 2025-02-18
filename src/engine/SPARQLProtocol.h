// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include "util/TypeIdentity.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"
#include "util/http/beast.h"

// Helper for parsing `HttpRequest` into `ParsedRequest`. The parsing has many
// common patterns but the details are slightly different. This struct
// stores the partially parsed `ParsedRequest` and methods for common
// operations used while parsing.
struct ParsedRequestBuilder {
  FRIEND_TEST(SPARQLProtocolTest, extractTargetGraph);
  FRIEND_TEST(SPARQLProtocolTest, determineAccessToken);
  FRIEND_TEST(SPARQLProtocolTest, parameterIsContainedExactlyOnce);

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

// Parses HTTP requests to `ParsedRequests` (a representation of Query, Update,
// Graph Store and internal operations) according to the SPARQL specifications.
class SPARQLProtocol {
  FRIEND_TEST(SPARQLProtocolTest, parseGET);
  FRIEND_TEST(SPARQLProtocolTest, parseUrlencodedPOST);
  FRIEND_TEST(SPARQLProtocolTest, parseQueryPOST);
  FRIEND_TEST(SPARQLProtocolTest, parseUpdatePOST);
  FRIEND_TEST(SPARQLProtocolTest, parsePOST);

  static constexpr std::string_view contentTypeUrlEncoded =
      "application/x-www-form-urlencoded";
  static constexpr std::string_view contentTypeSparqlQuery =
      "application/sparql-query";
  static constexpr std::string_view contentTypeSparqlUpdate =
      "application/sparql-update";

  using RequestType =
      boost::beast::http::request<boost::beast::http::string_body>;

  // Parse an HTTP GET request into a `ParsedRequest`. The
  // `ParsedRequestBuilder` must have already extracted the access token.
  static ad_utility::url_parser::ParsedRequest parseGET(
      ParsedRequestBuilder parsedRequestBuilder);

  // Parse an HTTP POST request with content-type
  // `application/x-www-form-urlencoded` into a `ParsedRequest`.
  static ad_utility::url_parser::ParsedRequest parseUrlencodedPOST(
      const RequestType& request) {
    using namespace ad_utility::url_parser::sparqlOperation;
    using namespace ad_utility::use_type_identity;
    namespace http = boost::beast::http;
    auto parsedRequestBuilder = ParsedRequestBuilder(request);
    // All parameters must be included in the request body for URL-encoded
    // POST. The HTTP query string parameters must be empty. See SPARQL 1.1
    // Protocol Sections 2.1.2
    if (!parsedRequestBuilder.parsedRequest_.parameters_.empty()) {
      throw std::runtime_error(
          "URL-encoded POST requests must not contain query parameters in "
          "the URL.");
    }

    // Set the url-encoded parameters from the request body.
    // Note: previously we used `boost::urls::parse_query`, but that
    // function doesn't unescape the `+` which encodes a space character.
    // The following workaround of making the url-encoded parameters a
    // complete relative url and parsing this URL seems to work. Note: We
    // have to bind the result of `StrCat` to an explicit variable, as the
    // `boost::urls` parsing routines only give back a view, which otherwise
    // would be dangling.
    auto bodyAsQuery = absl::StrCat("/?", request.body());
    auto query = boost::urls::parse_origin_form(bodyAsQuery);
    if (!query) {
      throw std::runtime_error("Invalid URL-encoded POST request, body was: " +
                               request.body());
    }
    parsedRequestBuilder.parsedRequest_.parameters_ =
        ad_utility::url_parser::paramsToMap(query->params());
    parsedRequestBuilder.reportUnsupportedContentTypeIfGraphStore(
        contentTypeUrlEncoded);
    if (parsedRequestBuilder.parametersContain("query") &&
        parsedRequestBuilder.parametersContain("update")) {
      throw std::runtime_error(
          R"(Request must only contain one of "query" and "update".)");
    }
    parsedRequestBuilder.extractOperationIfSpecified(ti<Query>, "query");
    parsedRequestBuilder.extractOperationIfSpecified(ti<Update>, "update");
    parsedRequestBuilder.extractDatasetClauses();
    // We parse the access token from the url-encoded parameters in the
    // body. The URL parameters must be empty for URL-encoded POST (see
    // above).
    parsedRequestBuilder.extractAccessToken(request);

    return std::move(parsedRequestBuilder).build();
  }

  // Parse an HTTP POST request with a SPARQL operation in its body
  // into a `ParsedRequest`. This is used for the content types
  // `application/sparql-query` and `application/sparql-update`.
  template <typename Operation>
  static ad_utility::url_parser::ParsedRequest parseSPARQLPOST(
      const RequestType& request, std::string_view contentType) {
    using namespace ad_utility::url_parser::sparqlOperation;
    auto parsedRequestBuilder = ParsedRequestBuilder(request);
    parsedRequestBuilder.reportUnsupportedContentTypeIfGraphStore(contentType);
    parsedRequestBuilder.parsedRequest_.operation_ =
        Operation{request.body(), {}};
    parsedRequestBuilder.extractDatasetClauses();
    parsedRequestBuilder.extractAccessToken(request);
    return std::move(parsedRequestBuilder).build();
  }

  // Parse an HTTP POST request with content type `application/sparql-query`
  // into a `ParsedRequest`.
  static ad_utility::url_parser::ParsedRequest parseQueryPOST(
      const RequestType& request) {
    return parseSPARQLPOST<ad_utility::url_parser::sparqlOperation::Query>(
        request, contentTypeSparqlQuery);
  }

  // Parse an HTTP POST request with content type `application/sparql-update`
  // into a `ParsedRequest`.
  static ad_utility::url_parser::ParsedRequest parseUpdatePOST(
      const RequestType& request) {
    return parseSPARQLPOST<ad_utility::url_parser::sparqlOperation::Update>(
        request, contentTypeSparqlUpdate);
  }

  // Parse an HTTP POST request into a `ParsedRequest`.
  static ad_utility::url_parser::ParsedRequest parsePOST(
      const RequestType& request) {
    // For a POST request, the content type *must* be either
    // "application/x-www-form-urlencoded" (1), "application/sparql-query"
    // (2) or "application/sparql-update" (3).
    //
    // (1) Section 2.1.2: The body of the POST request contains *all*
    // parameters (including the query or update) in an encoded form (just
    // like in the part of a GET request after the "?").
    //
    // (2) Section 2.1.3: The body of the POST request contains *only* the
    // unencoded SPARQL query. There may be additional HTTP query parameters.
    //
    // (3) Section 2.2.2: The body of the POST request contains *only* the
    // unencoded SPARQL update. There may be additional HTTP query parameters.
    //
    // Reference: https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321
    std::string_view contentType =
        request.base()[boost::beast::http::field::content_type];
    LOG(DEBUG) << "Content-type: \"" << contentType << "\"" << std::endl;

    // Note: For simplicity we only check via `starts_with`. This ignores
    // additional parameters like `application/sparql-query;charset=utf8`. We
    // currently always expect UTF-8.
    // TODO<joka921> Implement more complete parsing that allows the checking
    // of these parameters.
    if (contentType.starts_with(contentTypeUrlEncoded)) {
      return parseUrlencodedPOST(request);
    }
    if (contentType.starts_with(contentTypeSparqlQuery)) {
      return parseQueryPOST(request);
    }
    if (contentType.starts_with(contentTypeSparqlUpdate)) {
      return parseUpdatePOST(request);
    }
    // Checking if the content type is supported by the Graph Store
    // HTTP Protocol implementation is done later.
    auto parsedRequestBuilder = ParsedRequestBuilder(request);
    if (parsedRequestBuilder.isGraphStoreOperation()) {
      parsedRequestBuilder.extractGraphStoreOperation();
      parsedRequestBuilder.extractAccessToken(request);
      return std::move(parsedRequestBuilder).build();
    }

    throw std::runtime_error(absl::StrCat(
        "POST request with content type \"", contentType,
        "\" not supported (must be \"", contentTypeUrlEncoded, "\", \"",
        contentTypeSparqlQuery, "\" or \"", contentTypeSparqlUpdate, "\")"));
  }

 public:
  // Parse a HTTP request.
  static ad_utility::url_parser::ParsedRequest parseHttpRequest(
      const RequestType& request) {
    namespace http = boost::beast::http;
    if (request.method() == http::verb::get) {
      auto parsedRequestBuilder = ParsedRequestBuilder(request);
      parsedRequestBuilder.extractAccessToken(request);
      return parseGET(std::move(parsedRequestBuilder));
    }
    if (request.method() == http::verb::post) {
      return parsePOST(request);
    }
    std::ostringstream requestMethodName;
    requestMethodName << request.method();
    throw std::runtime_error(
        absl::StrCat("Request method \"", requestMethodName.str(),
                     "\" not supported (has to be GET or POST)"));
  }
};
