// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_GRAPHSTOREPROTOCOL_H
#define QLEVER_SRC_ENGINE_GRAPHSTOREPROTOCOL_H

#include <gtest/gtest_prod.h>

#include "engine/HttpError.h"
#include "parser/ParsedQuery.h"
#include "parser/RdfParser.h"
#include "parser/SparqlParser.h"
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
      throw HttpError(boost::beast::http::status::unsupported_media_type,
                      "Mediatype empty or not set.");
    }
    std::optional<ad_utility::MediaType> mediatype;
    try {
      mediatype = ad_utility::getMediaTypeFromAcceptHeader(contentTypeString);
    } catch (const std::exception& e) {
      throw HttpError(boost::beast::http::status::unsupported_media_type,
                      e.what());
    }
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
  [[noreturn]] static void throwNotYetImplementedHTTPMethod(
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
    if (rawRequest.body().empty()) {
      throw HttpError(boost::beast::http::status::no_content);
    }
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    auto convertedTriples = convertTriples(graph, std::move(triples));
    updateClause::GraphUpdate up{std::move(convertedTriples), {}};
    ParsedQuery res;
    res._clause = parsedQuery::UpdateClause{std::move(up)};
    // Graph store protocol POST requests might have a very large body. Limit
    // the length used for the string representation.
    res._originalString =
        absl::StrCat("Graph Store POST Operation\n",
                     ad_utility::truncateOperationString(rawRequest.body()));
    return res;
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformPostAndTsop);

  // Transform a SPARQL Graph Store Protocol TSOP to an equivalent ParsedQuery
  // which is an SPARQL Update.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static ParsedQuery
      transformTsop(const RequestT& rawRequest, const GraphOrDefault& graph) {
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    auto convertedTriples = convertTriples(graph, std::move(triples));
    updateClause::GraphUpdate up{{}, std::move(convertedTriples)};
    ParsedQuery res;
    res._clause = parsedQuery::UpdateClause{std::move(up)};
    // Graph store protocol TSOP requests might have a very large body. Limit
    // the length used for the string representation.
    res._originalString =
        absl::StrCat("Graph Store TSOP Operation\n",
                     ad_utility::truncateOperationString(rawRequest.body()));
    return res;
  }

  // Transform a SPARQL Graph Store Protocol GET to an equivalent ParsedQuery
  // which is an SPARQL Query.
  static ParsedQuery transformGet(const GraphOrDefault& graph);
  FRIEND_TEST(GraphStoreProtocolTest, transformGet);

  // Transform a SPARQL Graph Store Protocol PUT to equivalent ParsedQueries
  // which are SPARQL Queries.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static std::
      vector<ParsedQuery> transformPut(const RequestT& rawRequest,
                                       const GraphOrDefault& graph) {
    // TODO: The response codes are not conform to the specs. "If new RDF graph
    //  content is created", then the status must be `201 Created`. "If
    //  existing graph content is modified", then the status must be `200 OK`
    //  or `204 No Content`.
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    auto convertedTriples = convertTriples(graph, std::move(triples));
    updateClause::GraphUpdate up{std::move(convertedTriples), {}};
    ParsedQuery res;
    res._clause = parsedQuery::UpdateClause{std::move(up)};
    // Graph store protocol POST requests might have a very large body. Limit
    // the length used for the string representation.
    std::string stringRepresentation =
        absl::StrCat("Graph Store PUT Operation\n",
                     ad_utility::truncateOperationString(rawRequest.body()));
    res._originalString = stringRepresentation;
    std::string dropUpdate;
    if (const auto* iri =
            std::get_if<ad_utility::triple_component::Iri>(&graph)) {
      dropUpdate =
          absl::StrCat("DROP SILENT GRAPH ", iri->toStringRepresentation());
    } else {
      dropUpdate = "DROP SILENT DEFAULT";
    }
    std::vector<ParsedQuery> drop = SparqlParser::parseUpdate(dropUpdate);
    AD_CORRECTNESS_CHECK(drop.size() == 1);
    drop[0]._originalString = stringRepresentation;
    return {std::move(drop[0]), std::move(res)};
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformPut);

  // Transform a SPARQL Graph Store Protocol DELETE to equivalent ParsedQueries
  // which are SPARQL Queries.
  static ParsedQuery transformDelete(const GraphOrDefault& graph);
  FRIEND_TEST(GraphStoreProtocolTest, transformDelete);

 public:
  // Every Graph Store Protocol request has equivalent SPARQL Query or Update.
  // Transform the Graph Store Protocol request into it's equivalent Query or
  // Update.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static std::
      vector<ParsedQuery> transformGraphStoreProtocol(
          ad_utility::url_parser::sparqlOperation::GraphStoreOperation
              operation,
          const RequestT& rawRequest) {
    ad_utility::url_parser::ParsedUrl parsedUrl =
        ad_utility::url_parser::parseRequestTarget(rawRequest.target());
    using enum boost::beast::http::verb;
    std::string_view method = rawRequest.method_string();
    if (method == "GET") {
      return {transformGet(operation.graph_)};
    } else if (method == "PUT") {
      return transformPut(rawRequest, operation.graph_);
    } else if (method == "DELETE") {
      return {transformDelete(operation.graph_)};
    } else if (method == "POST") {
      return {transformPost(rawRequest, operation.graph_)};
    } else if (method == "TSOP") {
      return {transformTsop(rawRequest, operation.graph_)};
    } else if (method == "HEAD") {
      throwNotYetImplementedHTTPMethod("HEAD");
    } else if (method == "PATCH") {
      throwNotYetImplementedHTTPMethod("PATCH");
    } else {
      throw std::runtime_error(
          absl::StrCat("Unsupported HTTP method \"", method,
                       "\" for the SPARQL Graph Store HTTP Protocol."));
    }
  }
};

#endif  // QLEVER_SRC_ENGINE_GRAPHSTOREPROTOCOL_H
