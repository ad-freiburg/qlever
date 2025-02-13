// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include "util/Algorithm.h"
#include "util/TypeIdentity.h"
#include "util/TypeTraits.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"
#include "util/http/beast.h"

class SPARQLProtocol {
  FRIEND_TEST(SPARQLProtocolTest, extractAccessToken);

 public:
  /// Parse the path and URL parameters from the given request. Supports both
  /// GET and POST request according to the SPARQL 1.1 standard.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<
          RequestT>) static ad_utility::url_parser::ParsedRequest
      parseHttpRequest(const RequestT& request) {
    using namespace ad_utility::url_parser::sparqlOperation;
    using namespace ad_utility::use_type_identity;
    namespace http = boost::beast::http;
    // For an HTTP request, `request.target()` yields the HTTP Request-URI.
    // This is a concatenation of the URL path and the query strings.
    auto parsedUrl =
        ad_utility::url_parser::parseRequestTarget(request.target());
    ad_utility::url_parser::ParsedRequest parsedRequest{
        std::move(parsedUrl.path_), std::nullopt,
        std::move(parsedUrl.parameters_), None{}};

    // Some valid requests (e.g. QLever's custom commands like retrieving index
    // statistics) don't have a query. So an empty operation is not necessarily
    // an error.
    auto setOperationIfSpecifiedInParams = [&parsedRequest]<typename Operation>(
                                               TI<Operation>,
                                               string_view paramName) {
      auto operation = ad_utility::url_parser::getParameterCheckAtMostOnce(
          parsedRequest.parameters_, paramName);
      if (operation.has_value()) {
        parsedRequest.operation_ = Operation{operation.value(), {}};
        parsedRequest.parameters_.erase(paramName);
      }
    };
    auto addToDatasetClausesIfOperationIs =
        [&parsedRequest]<typename Operation>(
            TI<Operation>, const std::string& key, bool isNamed) {
          if (Operation* op =
                  std::get_if<Operation>(&parsedRequest.operation_)) {
            ad_utility::appendVector(
                op->datasetClauses_,
                ad_utility::url_parser::parseDatasetClausesFrom(
                    parsedRequest.parameters_, key, isNamed));
          }
        };
    auto addDatasetClauses = [&addToDatasetClausesIfOperationIs] {
      addToDatasetClausesIfOperationIs(ti<Query>, "default-graph-uri", false);
      addToDatasetClausesIfOperationIs(ti<Query>, "named-graph-uri", true);
      addToDatasetClausesIfOperationIs(ti<Update>, "using-graph-uri", false);
      addToDatasetClausesIfOperationIs(ti<Update>, "using-named-graph-uri",
                                       true);
    };
    auto extractAccessTokenFromRequest = [&parsedRequest, &request]() {
      parsedRequest.accessToken_ =
          extractAccessToken(request, parsedRequest.parameters_);
    };

    if (request.method() == http::verb::get) {
      setOperationIfSpecifiedInParams(ti<Query>, "query");
      addDatasetClauses();
      extractAccessTokenFromRequest();

      if (parsedRequest.parameters_.contains("update")) {
        throw std::runtime_error(
            "SPARQL Update is not allowed as GET request.");
      }
      return parsedRequest;
    }
    if (request.method() == http::verb::post) {
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
      std::string_view contentType = request.base()[http::field::content_type];
      LOG(DEBUG) << "Content-type: \"" << contentType << "\"" << std::endl;
      static constexpr std::string_view contentTypeUrlEncoded =
          "application/x-www-form-urlencoded";
      static constexpr std::string_view contentTypeSparqlQuery =
          "application/sparql-query";
      static constexpr std::string_view contentTypeSparqlUpdate =
          "application/sparql-update";

      // Note: For simplicity we only check via `starts_with`. This ignores
      // additional parameters like `application/sparql-query;charset=utf8`. We
      // currently always expect UTF-8.
      // TODO<joka921> Implement more complete parsing that allows the checking
      // of these parameters.
      if (contentType.starts_with(contentTypeUrlEncoded)) {
        // All parameters must be included in the request body for URL-encoded
        // POST. The HTTP query string parameters must be empty. See SPARQL 1.1
        // Protocol Sections 2.1.2
        if (!parsedRequest.parameters_.empty()) {
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
          throw std::runtime_error(
              "Invalid URL-encoded POST request, body was: " + request.body());
        }
        parsedRequest.parameters_ =
            ad_utility::url_parser::paramsToMap(query->params());

        if (parsedRequest.parameters_.contains("query") &&
            parsedRequest.parameters_.contains("update")) {
          throw std::runtime_error(
              R"(Request must only contain one of "query" and "update".)");
        }
        setOperationIfSpecifiedInParams(ti<Query>, "query");
        setOperationIfSpecifiedInParams(ti<Update>, "update");
        addDatasetClauses();
        // We parse the access token from the url-encoded parameters in the
        // body. The URL parameters must be empty for URL-encoded POST (see
        // above).
        extractAccessTokenFromRequest();

        return parsedRequest;
      }
      if (contentType.starts_with(contentTypeSparqlQuery)) {
        parsedRequest.operation_ = Query{request.body(), {}};
        addDatasetClauses();
        extractAccessTokenFromRequest();
        return parsedRequest;
      }
      if (contentType.starts_with(contentTypeSparqlUpdate)) {
        parsedRequest.operation_ = Update{request.body(), {}};
        addDatasetClauses();
        extractAccessTokenFromRequest();
        return parsedRequest;
      }
      throw std::runtime_error(absl::StrCat(
          "POST request with content type \"", contentType,
          "\" not supported (must be \"", contentTypeUrlEncoded, "\", \"",
          contentTypeSparqlQuery, "\" or \"", contentTypeSparqlUpdate, "\")"));
    }
    std::ostringstream requestMethodName;
    requestMethodName << request.method();
    throw std::runtime_error(
        absl::StrCat("Request method \"", requestMethodName.str(),
                     "\" not supported (has to be GET or POST)"));
  };

 private:
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static std::
      optional<std::string> extractAccessToken(
          const RequestT& request,
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
  };
};
