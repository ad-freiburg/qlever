// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_GRAPHSTOREPROTOCOL_H
#define QLEVER_SRC_ENGINE_GRAPHSTOREPROTOCOL_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <gtest/gtest_prod.h>

#include "engine/GraphManager.h"
#include "engine/HttpError.h"
#include "parser/ParsedQuery.h"
#include "parser/Quads.h"
#include "parser/RdfParser.h"
#include "parser/SparqlParser.h"
#include "util/http/HttpUtils.h"
#include "util/http/ResponseMiddleware.h"
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
      // HTTP requires the response body to be empty for this status code.
      throw HttpError(boost::beast::http::status::no_content, "");
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

  static ResponseMiddleware makePostNewGraphMiddleware(
      const ad_utility::triple_component::Iri& newGraph);

  // Generates a random graph IRI. The IRI is generated randomly with an
  // internal prefix. NOTE: It is not guaranteed that the IRI does not exist.
  static ad_utility::triple_component::Iri generateGraphIri();

  // Transform a SPARQL Graph Store Protocol POST to an equivalent ParsedQuery
  // which is an SPARQL Update.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static ParsedQuery
      transformPost(GraphManager& graphManager, const RequestT& rawRequest,
                    const GraphOrDefault& graph, const Index& index) {
    throwIfRequestBodyEmpty(rawRequest);
    // For a `POST` when the graph identifies the QLever instance itself then
    // the data must be stored in a newly generated graph which is returned in
    // the response.
    // TODO: test this with jena
    bool generateNewGraph = [&rawRequest, &graph]() {
      if (std::holds_alternative<GraphRef>(graph) &&
          rawRequest.find(boost::beast::http::field::host) !=
              rawRequest.end()) {
        return std::get<GraphRef>(graph) ==
               ad_utility::triple_component::Iri::fromIriref(
                   "<http://" +
                   std::string(rawRequest[boost::beast::http::field::host]) +
                   "/" + GSP_DIRECT_GRAPH_IDENTIFICATION_PREFIX + ">");
      }
      return false;
    }();
    const GraphOrDefault effectiveGraph =
        generateNewGraph ? graphManager.getNewInternalGraph() : graph;
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    Quads::BlankNodeAdder bn{{}, {}, index.getBlankNodeManager()};
    auto convertedTriples =
        convertTriples(effectiveGraph, std::move(triples), bn);
    updateClause::GraphUpdate up{std::move(convertedTriples), {}};
    ParsedQuery res;
    parsedQuery::UpdateClause clause{std::move(up)};
    if (generateNewGraph) {
      res.responseMiddleware_ = makePostNewGraphMiddleware(
          std::get<ad_utility::triple_component::Iri>(effectiveGraph));
    }
    res._clause = std::move(clause);
    // Graph store protocol POST requests might have a very large body. Limit
    // the length used for the string representation.
    res._originalString = truncatedStringRepresentation("POST", rawRequest);
    return res;
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformPostAndTsop);
  FRIEND_TEST(GraphStoreProtocolTest, EncodedIriManagerUsage);

  // `TSOP` (`POST` backwards) does a `DELETE DATA` of the payload. It is an
  // extension to the Graph Store Protocol.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static ParsedQuery
      transformTsop(const RequestT& rawRequest, const GraphOrDefault& graph,
                    const Index& index) {
    throwIfRequestBodyEmpty(rawRequest);
    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    Quads::BlankNodeAdder bn{{}, {}, index.getBlankNodeManager()};
    auto convertedTriples = convertTriples(graph, std::move(triples), bn);
    updateClause::GraphUpdate up{{}, std::move(convertedTriples)};
    ParsedQuery res;
    res._clause = parsedQuery::UpdateClause{std::move(up)};
    res._originalString = truncatedStringRepresentation("TSOP", rawRequest);
    return res;
  }

  // Transform a SPARQL Graph Store Protocol GET to an equivalent ParsedQuery
  // which is a SPARQL Query.
  static ParsedQuery transformGet(const GraphOrDefault& graph,
                                  const EncodedIriManager* encodedIriManager);
  FRIEND_TEST(GraphStoreProtocolTest, transformGet);

  // Transform a SPARQL Graph Store Protocol HEAD to an equivalent ParsedQuery.
  // The response is the same as for GET but without the body.
  static ParsedQuery transformHead(const GraphOrDefault& graph,
                                   const EncodedIriManager* encodedIriManager);

  // Transform a SPARQL Graph Store Protocol PUT to equivalent ParsedQueries
  // which are SPARQL Updates.
  CPP_template_2(typename RequestT)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) static std::
      vector<ParsedQuery> transformPut(const RequestT& rawRequest,
                                       const GraphOrDefault& graph,
                                       const Index& index) {
    std::string stringRepresentation =
        truncatedStringRepresentation("PUT", rawRequest);

    // The request is transformed in the following equivalent SPARQL:
    // `DROP SILENT GRAPH <graph> ; INSERT DATA { GRAPH <graph> { ...body... }
    // }`
    auto getDrop = [&graph]() -> std::string {
      if (const auto* iri =
              std::get_if<ad_utility::triple_component::Iri>(&graph)) {
        return absl::StrCat("DROP SILENT GRAPH ",
                            iri->toStringRepresentation());
      } else {
        return "DROP SILENT DEFAULT";
      }
    };

    ParsedQuery drop = ad_utility::getSingleElement(SparqlParser::parseUpdate(
        index.getBlankNodeManager(), &index.encodedIriManager(), getDrop()));
    drop._originalString = stringRepresentation;

    auto triples =
        parseTriples(rawRequest.body(), extractMediatype(rawRequest));
    Quads::BlankNodeAdder bn{{}, {}, index.getBlankNodeManager()};
    auto convertedTriples = convertTriples(graph, std::move(triples), bn);
    updateClause::GraphUpdate up{std::move(convertedTriples), {}};
    ParsedQuery insertData;
    // Interpretation of the very vague GSP 5.3:
    // - 201 Created if a new graph is created
    // - 200 Ok or 204 No Content if an existing graph is modified
    // When the drop (first operation) deletes triples then the graph has
    // existed before in our model of implicit graph existence.
    drop.responseMiddleware_ =
        ResponseMiddleware([](ResponseMiddleware::ResponseT response,
                              std::vector<UpdateMetadata> updateMetadata) {
          AD_CORRECTNESS_CHECK(updateMetadata.size() == 2 &&
                               updateMetadata.at(0).inUpdate_.has_value());
          if (updateMetadata.at(0).inUpdate_.value().triplesDeleted_ > 0) {
            response.result(boost::beast::http::status::ok);
          } else {
            response.result(boost::beast::http::status::created);
          }
          return response;
        });
    insertData._clause = parsedQuery::UpdateClause{std::move(up)};
    insertData._originalString = stringRepresentation;
    return {std::move(drop), std::move(insertData)};
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformPut);

  // Transform a SPARQL Graph Store Protocol DELETE to equivalent ParsedQueries
  // which are SPARQL Updates.
  static ParsedQuery transformDelete(const GraphOrDefault& graph,
                                     const Index& index);
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
          GraphManager& graphManager, const RequestT& rawRequest,
          const Index& index) {
    ad_utility::url_parser::ParsedUrl parsedUrl =
        ad_utility::url_parser::parseRequestTarget(rawRequest.target());
    using enum boost::beast::http::verb;
    std::string_view method = rawRequest.method_string();
    if (method == "GET") {
      return {transformGet(operation.graph_, &index.encodedIriManager())};
    } else if (method == "PUT") {
      return transformPut(rawRequest, operation.graph_, index);
    } else if (method == "DELETE") {
      return {transformDelete(operation.graph_, index)};
    } else if (method == "POST") {
      return {transformPost(graphManager, rawRequest, operation.graph_, index)};
    } else if (method == "TSOP") {
      // TSOP (`POST` backwards) does the inverse of `POST`. It does a `DELETE
      // DATA` of the payload.
      return {transformTsop(rawRequest, operation.graph_, index)};
    } else if (method == "HEAD") {
      return {transformHead(operation.graph_, &index.encodedIriManager())};
    } else if (method == "PATCH") {
      throwNotYetImplementedHTTPMethod("PATCH");
    } else {
      throw std::runtime_error(
          absl::StrCat("Unsupported HTTP method \"", method,
                       "\" for the SPARQL Graph Store HTTP Protocol."));
    }
  }
};
#endif

#endif  // QLEVER_SRC_ENGINE_GRAPHSTOREPROTOCOL_H
