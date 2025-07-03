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
    auto mediaTypes =
        ad_utility::getMediaTypesFromAcceptHeader(contentTypeString);

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

  // Aborts the request with an HTTP 204 No Content if the request body is
  // empty.
  static void throwIfRequestBodyEmpty(const auto& request) {
    if (request.body().empty()) {
      throw HttpError(boost::beast::http::status::no_content,
                      "Request body is empty.");
    }
  }

  static std::string truncatedStringRepresentation(std::string type,
                                                   const auto& request) {
    // Graph store protocol requests might have a very large body. Limit
    // the length used for the string representation.
    return absl::StrCat("Graph Store ", type, " Operation\n",
                        ad_utility::truncateOperationString(request.body()));
  }

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
    throwIfRequestBodyEmpty(rawRequest);
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    auto convertedTriples = convertTriples(graph, std::move(triples));
    updateClause::GraphUpdate up{std::move(convertedTriples), {}};
    ParsedQuery res;
    res._clause = parsedQuery::UpdateClause{std::move(up)};
    res._originalString = truncatedStringRepresentation("POST", rawRequest);
    return res;
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformPostAndTsop);

  // `TSOP` (`POST` backwards) does a `DELETE DATA` of the payload. It is an
  // extension to the Graph Store Protocol.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static ParsedQuery
      transformTsop(const RequestT& rawRequest, const GraphOrDefault& graph) {
    throwIfRequestBodyEmpty(rawRequest);
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    auto convertedTriples = convertTriples(graph, std::move(triples));
    updateClause::GraphUpdate up{{}, std::move(convertedTriples)};
    ParsedQuery res;
    res._clause = parsedQuery::UpdateClause{std::move(up)};
    res._originalString = truncatedStringRepresentation("TSOP", rawRequest);
    return res;
  }

  // Transform a SPARQL Graph Store Protocol GET to an equivalent ParsedQuery
  // which is an SPARQL Query.
  static ParsedQuery transformGet(const GraphOrDefault& graph);
  FRIEND_TEST(GraphStoreProtocolTest, transformGet);

  // Transform a SPARQL Graph Store Protocol PUT to equivalent ParsedQueries
  // which are SPARQL Updates.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static std::
      vector<ParsedQuery> transformPut(const RequestT& rawRequest,
                                       const GraphOrDefault& graph) {
    // TODO: The response codes are not conform to the specs. "If new RDF graph
    //  content is created", then the status must be `201 Created`. "If
    //  existing graph content is modified", then the status must be `200 OK`
    //  or `204 No Content`.
    std::string stringRepresentation =
        truncatedStringRepresentation("PUT", rawRequest);

    // The request is transformed in the following equivalent SPARQL:
    // `DROP SILENT GRAPH <graph> ; INSERT DATA { GRAPH <graph> { ...body... }
    // }`
    auto dropUpdate = [&graph]() -> std::string {
      if (const auto* iri =
              std::get_if<ad_utility::triple_component::Iri>(&graph)) {
        return absl::StrCat("DROP SILENT GRAPH ",
                            iri->toStringRepresentation());
      } else {
        return "DROP SILENT DEFAULT";
      }
    }();
    ParsedQuery drop = [&dropUpdate]() {
      auto drops = SparqlParser::parseUpdate(dropUpdate);
      AD_CORRECTNESS_CHECK(drops.size() == 1);
      return drops.at(0);
    }();
    drop._originalString = stringRepresentation;

    AD_LOG_INFO << "parsing triples from the following raw PUT REQUEST:\n"
                << rawRequest.body() << std::endl;
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    AD_LOG_INFO << "parsed " << triples.size() << "triples\n";
    auto convertedTriples = convertTriples(graph, std::move(triples));
    updateClause::GraphUpdate up{std::move(convertedTriples), {}};
    ParsedQuery res;
    res._clause = parsedQuery::UpdateClause{std::move(up)};
    res._originalString = stringRepresentation;
    return {std::move(drop), std::move(res)};
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformPut);

  // Transform a SPARQL Graph Store Protocol DELETE to equivalent ParsedQueries
  // which are SPARQL Updates.
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
      // TSOP (`POST` backwards) does the inverse of `POST`. It does a `DELETE
      // DATA` of the payload.
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
