// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include "util/TypeIdentity.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"
#include "util/http/beast.h"

struct ParsedRequestBuilder {
  FRIEND_TEST(SPARQLProtocolTest, extractTargetGraph);
  FRIEND_TEST(SPARQLProtocolTest, extractAccessTokenImpl);
  FRIEND_TEST(SPARQLProtocolTest, parameterIsContainedExactlyOnce);

  ad_utility::url_parser::ParsedRequest parsedRequest_;

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

  void extractAccessToken(
      const ad_utility::httpUtils::HttpRequest auto& request) {
    parsedRequest_.accessToken_ =
        extractAccessToken(request, parsedRequest_.parameters_);
  }

  void extractDatasetClauses();

  // Some valid requests (e.g. QLever's custom commands like retrieving index
  // statistics) don't have a query. An empty operation is not an error. Setting
  // multiple operations should not happen. Setting an operation when one is
  // already set is an error.
  template <typename Operation>
  void extractOperationIfSpecified(ad_utility::use_type_identity::TI<Operation>,
                                   string_view paramName);

  bool isGraphStoreOperation() const;

  void extractGraphStoreOperation();

  bool parametersContain(std::string_view param) const;

  // Check that requests don't both have these content types and are Graph
  // Store operations.
  void reportUnsupportedContentTypeIfGraphStore(
      std::string_view contentType) const;

  ad_utility::url_parser::ParsedRequest build() &&;

 private:
  // Adds a dataset clause to the operation if it has the given type. The
  // dataset clause's IRI is the value of parameter `key`. The `isNamed_` of the
  // dataset clause is as given.
  template <typename Operation>
  void extractDatasetClauseIfOperationIs(
      ad_utility::use_type_identity::TI<Operation>, const std::string& key,
      bool isNamed);

  // Check that a parameter is contained exactly once.
  bool parameterIsContainedExactlyOnce(std::string_view key) const;

  // Extract the graph to be acted upon using from the URL query parameters
  // (`Indirect Graph Identification`). See
  // https://www.w3.org/TR/2013/REC-sparql11-http-rdf-update-20130321/#indirect-graph-identification
  static GraphOrDefault extractTargetGraph(
      const ad_utility::url_parser::ParamValueMap& params);

  static std::optional<std::string> extractAccessToken(
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

  static ad_utility::url_parser::ParsedRequest parseGET(
      const ad_utility::httpUtils::HttpRequest auto& request) {
    using namespace ad_utility::url_parser::sparqlOperation;
    using namespace ad_utility::use_type_identity;
    auto parsedRequestBuilder = ParsedRequestBuilder(request);
    parsedRequestBuilder.extractAccessToken(request);

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

  static ad_utility::url_parser::ParsedRequest parseUrlencodedPOST(
      const ad_utility::httpUtils::HttpRequest auto& request) {
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

  template <typename Operation>
  static ad_utility::url_parser::ParsedRequest parseSPARQLPOST(
      const ad_utility::httpUtils::HttpRequest auto& request,
      std::string_view contentType) {
    using namespace ad_utility::url_parser::sparqlOperation;
    auto parsedRequestBuilder = ParsedRequestBuilder(request);
    parsedRequestBuilder.reportUnsupportedContentTypeIfGraphStore(contentType);
    parsedRequestBuilder.parsedRequest_.operation_ =
        Operation{request.body(), {}};
    parsedRequestBuilder.extractDatasetClauses();
    parsedRequestBuilder.extractAccessToken(request);
    return std::move(parsedRequestBuilder).build();
  }

  static ad_utility::url_parser::ParsedRequest parseQueryPOST(
      const ad_utility::httpUtils::HttpRequest auto& request) {
    return parseSPARQLPOST<ad_utility::url_parser::sparqlOperation::Query>(
        request, contentTypeSparqlQuery);
  }

  static ad_utility::url_parser::ParsedRequest parseUpdatePOST(
      const ad_utility::httpUtils::HttpRequest auto& request) {
    return parseSPARQLPOST<ad_utility::url_parser::sparqlOperation::Update>(
        request, contentTypeSparqlUpdate);
  }

  static ad_utility::url_parser::ParsedRequest parsePOST(
      const ad_utility::httpUtils::HttpRequest auto& request) {
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
  /// Parse the path and URL parameters from the given request. Supports both
  /// GET and POST request according to the SPARQL 1.1 standard.
  static ad_utility::url_parser::ParsedRequest parseHttpRequest(
      const ad_utility::httpUtils::HttpRequest auto& request) {
    namespace http = boost::beast::http;
    if (request.method() == http::verb::get) {
      return parseGET(request);
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
