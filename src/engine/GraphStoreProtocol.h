// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <gtest/gtest_prod.h>

#include "parser/ParsedQuery.h"
#include "parser/RdfParser.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

// The mediatype of a request could not be determined.
class UnknownMediatypeError : public std::runtime_error {
 public:
  explicit UnknownMediatypeError(std::string_view msg)
      : std::runtime_error{std::string{msg}} {}
};

// The mediatype of a request is not supported.
class UnsupportedMediatypeError : public std::runtime_error {
 public:
  explicit UnsupportedMediatypeError(std::string_view msg)
      : std::runtime_error{std::string{msg}} {}
};

// Transform SPARQL Graph Store Protocol requests to their equivalent
// ParsedQuery (SPARQL Query or Update).
class GraphStoreProtocol {
 private:
  // Extract the mediatype from a request.
  CPP_template_2(typename RequestT)(requires ad_utility::httpUtils::HttpRequest<
                                    RequestT>) static ad_utility::MediaType
      extractMediatype(const RequestT& rawRequest) {
    using namespace boost::beast::http;

    std::string_view contentTypeString;
    if (rawRequest.find(field::content_type) != rawRequest.end()) {
      contentTypeString = rawRequest.at(field::content_type);
    }
    if (contentTypeString.empty()) {
      // If the mediatype is not given, return an error.
      // Note: The specs also allow to try to determine the media type from the
      // content.
      throw UnknownMediatypeError("Mediatype empty or not set.");
    }
    const auto mediatype =
        ad_utility::getMediaTypeFromAcceptHeader(contentTypeString);
    // A media type is set but not one of the supported ones as per the QLever
    // MediaType code.
    if (!mediatype.has_value()) {
      throwUnsupportedMediatype(rawRequest.at(field::content_type));
    }
    return mediatype.value();
  }
  FRIEND_TEST(GraphStoreProtocolTest, extractMediatype);

  // Throws the error if a mediatype is not supported.
  [[noreturn]] static void throwUnsupportedMediatype(
      const std::string_view& mediatype);

  // Throws the error if an HTTP method is not supported.
  [[noreturn]] static void throwUnsupportedHTTPMethod(
      const std::string_view& method);

  // Parse the triples from the request body according to the content type.
  static std::vector<TurtleTriple> parseTriples(
      const std::string& body, const ad_utility::MediaType contentType);
  FRIEND_TEST(GraphStoreProtocolTest, parseTriples);

  // Transforms the triples from `TurtleTriple` to `SparqlTripleSimpleWithGraph`
  // and sets the correct graph.
  static std::vector<SparqlTripleSimpleWithGraph> convertTriples(
      const GraphOrDefault& graph, std::vector<TurtleTriple> triples);
  FRIEND_TEST(GraphStoreProtocolTest, convertTriples);

  // Transform a SPARQL Graph Store Protocol POST to an equivalent ParsedQuery
  // which is an SPARQL Update.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static ParsedQuery
      transformPost(const RequestT& rawRequest, const GraphOrDefault& graph) {
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    auto convertedTriples = convertTriples(graph, std::move(triples));
    updateClause::GraphUpdate up{std::move(convertedTriples), {}};
    ParsedQuery res;
    res._clause = parsedQuery::UpdateClause{std::move(up)};
    return res;
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformPost);

  // Transform a SPARQL Graph Store Protocol GET to an equivalent ParsedQuery
  // which is an SPARQL Query.
  static ParsedQuery transformGet(const GraphOrDefault& graph);
  FRIEND_TEST(GraphStoreProtocolTest, transformGet);

 public:
  // Every Graph Store Protocol request has equivalent SPARQL Query or Update.
  // Transform the Graph Store Protocol request into it's equivalent Query or
  // Update.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static ParsedQuery
      transformGraphStoreProtocol(const RequestT& rawRequest) {
    ad_utility::url_parser::ParsedUrl parsedUrl =
        ad_utility::url_parser::parseRequestTarget(rawRequest.target());
    // We only support passing the target graph as a query parameter (`Indirect
    // Graph Identification`). `Direct Graph Identification` (the URL is the
    // graph) is not supported. See also
    // https://www.w3.org/TR/2013/REC-sparql11-http-rdf-update-20130321/#graph-identification.
    GraphOrDefault graph = extractTargetGraph(parsedUrl.parameters_);

    using enum boost::beast::http::verb;
    auto method = rawRequest.method();
    if (method == get) {
      return transformGet(graph);
    } else if (method == put) {
      throwUnsupportedHTTPMethod("PUT");
    } else if (method == delete_) {
      throwUnsupportedHTTPMethod("DELETE");
    } else if (method == post) {
      return transformPost(rawRequest, graph);
    } else if (method == head) {
      throwUnsupportedHTTPMethod("HEAD");
    } else if (method == patch) {
      throwUnsupportedHTTPMethod("PATCH");
    } else {
      throw std::runtime_error(
          absl::StrCat("Unsupported HTTP method \"",
                       std::string_view{rawRequest.method_string()},
                       "\" for the SPARQL Graph Store HTTP Protocol."));
    }
  }

 private:
  // Extract the graph to be acted upon using from the URL query parameters
  // (`Indirect Graph Identification`). See
  // https://www.w3.org/TR/2013/REC-sparql11-http-rdf-update-20130321/#indirect-graph-identification
  static GraphOrDefault extractTargetGraph(
      const ad_utility::url_parser::ParamValueMap& params);
  FRIEND_TEST(GraphStoreProtocolTest, extractTargetGraph);
};
