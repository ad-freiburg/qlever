// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_GRAPHSTOREPROTOCOL_H
#define QLEVER_SRC_ENGINE_GRAPHSTOREPROTOCOL_H

#include <gtest/gtest_prod.h>

#include "engine/HttpError.h"
#include "parser/ParsedQuery.h"
#include "parser/Quads.h"
#include "parser/RdfParser.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

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
      throw HttpError(boost::beast::http::status::unsupported_media_type,
                      "Mediatype empty or not set.");
    }
    std::vector<ad_utility::MediaType> mediaTypes = [&contentTypeString]() {
      try {
        return ad_utility::getMediaTypesFromAcceptHeader(contentTypeString);
      } catch (const std::exception& e) {
        throw HttpError(boost::beast::http::status::unsupported_media_type,
                        e.what());
      }
    }();

    // A media type is set but not one of the supported ones as per the QLever
    // MediaType code. Content-Type is only allowed to return a single value, so
    // wildcards are also correctly discarded here.
    if (mediaTypes.size() != 1) {
      throwUnsupportedMediatype(rawRequest.at(field::content_type));
    }
    return mediaTypes.front();
  }
  FRIEND_TEST(GraphStoreProtocolTest, extractMediatype);

  // Throws the error if a mediatype is not supported.
  [[noreturn]] static void throwUnsupportedMediatype(
      const std::string_view& mediatype);

  // Throws the error if an HTTP method is not supported.
  [[noreturn]] static void throwNotYetImplementedHTTPMethod(
      const std::string_view& method);

  // Abort the request with an `HTTP 204 No Content` if the request body is
  // empty.
  static void throwIfRequestBodyEmpty(const auto& request) {
    if (request.body().empty()) {
      throw HttpError(boost::beast::http::status::no_content,
                      "Request body is empty.");
    }
  }

  // Return a truncated string with the graph store operation type and the
  // (possibly truncated) request body.
  static std::string truncatedStringRepresentation(std::string type,
                                                   const auto& request) {
    // Graph store protocol requests might have a very large body. Limit
    // the length used for the string representation.
    return absl::StrCat("Graph Store ", type, " Operation\n",
                        ad_utility::truncateOperationString(request.body()));
  }

  // Parse the triples from the request body according to the content type.
  static std::vector<TurtleTriple> parseTriples(
      const std::string& body, ad_utility::MediaType contentType);
  FRIEND_TEST(GraphStoreProtocolTest, parseTriples);

  // Transforms the triples from `TurtleTriple` to `SparqlTripleSimpleWithGraph`
  // and sets the correct graph.
  static updateClause::GraphUpdate::Triples convertTriples(
      const GraphOrDefault& graph, std::vector<TurtleTriple>&& triples,
      Quads::BlankNodeAdder& blankNodeAdder);
  FRIEND_TEST(GraphStoreProtocolTest, convertTriples);

  // Transform a SPARQL Graph Store Protocol POST to an equivalent ParsedQuery
  // which is an SPARQL Update.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static ParsedQuery
      transformPost(const RequestT& rawRequest, const GraphOrDefault& graph,
                    const Index& index) {
    throwIfRequestBodyEmpty(rawRequest);
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    Quads::BlankNodeAdder bn{{}, {}, index.getBlankNodeManager()};
    auto convertedTriples = convertTriples(graph, std::move(triples), bn);
    updateClause::GraphUpdate up{std::move(convertedTriples), {}};
    ParsedQuery res;
    res._clause = parsedQuery::UpdateClause{std::move(up)};
    // Graph store protocol POST requests might have a very large body. Limit
    // the length used for the string representation.
    res._originalString = truncatedStringRepresentation("POST", rawRequest);
    return res;
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformPost);
  FRIEND_TEST(GraphStoreProtocolTest, EncodedIriManagerUsage);

  // Transform a SPARQL Graph Store Protocol GET to an equivalent ParsedQuery
  // which is an SPARQL Query.
  static ParsedQuery transformGet(const GraphOrDefault& graph,
                                  const EncodedIriManager* encodedIriManager);
  FRIEND_TEST(GraphStoreProtocolTest, transformGet);

 public:
  // Every Graph Store Protocol request has equivalent SPARQL Query or Update.
  // Transform the Graph Store Protocol request into it's equivalent Query or
  // Update.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static std::
      vector<ParsedQuery> transformGraphStoreProtocol(
          ad_utility::url_parser::sparqlOperation::GraphStoreOperation
              operation,
          const RequestT& rawRequest, const Index& index) {
    ad_utility::url_parser::ParsedUrl parsedUrl =
        ad_utility::url_parser::parseRequestTarget(rawRequest.target());
    using enum boost::beast::http::verb;
    auto method = rawRequest.method();
    if (method == get) {
      return {transformGet(operation.graph_, &index.encodedIriManager())};
    } else if (method == put) {
      throwNotYetImplementedHTTPMethod("PUT");
    } else if (method == delete_) {
      throwNotYetImplementedHTTPMethod("DELETE");
    } else if (method == post) {
      return {transformPost(rawRequest, operation.graph_, index)};
    } else if (method == head) {
      throwNotYetImplementedHTTPMethod("HEAD");
    } else if (method == patch) {
      throwNotYetImplementedHTTPMethod("PATCH");
    } else {
      throw std::runtime_error(
          absl::StrCat("Unsupported HTTP method \"",
                       std::string_view{rawRequest.method_string()},
                       "\" for the SPARQL Graph Store HTTP Protocol."));
    }
  }
};

#endif  // QLEVER_SRC_ENGINE_GRAPHSTOREPROTOCOL_H
