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

// ____________________________________________________________________________
ad_utility::url_parser::ParsedRequest SPARQLProtocol::parseUrlencodedPOST(
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

// ____________________________________________________________________________
ad_utility::url_parser::ParsedRequest SPARQLProtocol::parseQueryPOST(
    const RequestType& request) {
  return parseSPARQLPOST<Query>(request, contentTypeSparqlQuery);
}

// ____________________________________________________________________________
ad_utility::url_parser::ParsedRequest SPARQLProtocol::parseUpdatePOST(
    const RequestType& request) {
  return parseSPARQLPOST<Update>(request, contentTypeSparqlUpdate);
}

// ____________________________________________________________________________
ad_utility::url_parser::ParsedRequest SPARQLProtocol::parsePOST(
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
ad_utility::url_parser::ParsedRequest SPARQLProtocol::parseHttpRequest(
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
