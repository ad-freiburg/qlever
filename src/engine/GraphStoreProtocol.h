// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <gtest/gtest_prod.h>

#include "parser/ParsedQuery.h"
#include "parser/RdfParser.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

class GraphStoreProtocol {
 private:
  static ParsedQuery transformPost(
      const ad_utility::httpUtils::HttpRequest auto& rawRequest,
      const GraphOrDefault& graph) {
    using namespace boost::beast::http;
    using Re2Parser = RdfStringParser<TurtleParser<Tokenizer>>;
    std::string contentTypeString;
    if (rawRequest.find(field::content_type) != rawRequest.end()) {
      contentTypeString = rawRequest.at(field::content_type);
    }
    if (contentTypeString.empty()) {
      // ContentType not set or empty; we don't try to guess -> 400 Bad Request
    }
    const auto contentType =
        ad_utility::getMediaTypeFromAcceptHeader(contentTypeString);
    std::vector<TurtleTriple> triples;
    switch (contentType.value()) {
      case ad_utility::MediaType::turtle:
      case ad_utility::MediaType::ntriples: {
        auto parser = Re2Parser();
        parser.setInputStream(rawRequest.body());
        triples = parser.parseAndReturnAllTriples();
        break;
      }
      default: {
        // Unsupported media type -> 415 Unsupported Media Type
        throw std::runtime_error(absl::StrCat(
            "Mediatype \"", ad_utility::toString(contentType.value()),
            "\" is not supported for SPARQL Graph Store HTTP "
            "Protocol in QLever."));
      }
    }
    ParsedQuery res;
    auto transformTurtleTriple = [&graph](const TurtleTriple& triple) {
      AD_CORRECTNESS_CHECK(triple.graphIri_.isId() &&
                           triple.graphIri_.getId() ==
                               qlever::specialIds().at(DEFAULT_GRAPH_IRI));
      SparqlTripleSimpleWithGraph::Graph g{std::monostate{}};
      if (std::holds_alternative<GraphRef>(graph)) {
        g = Iri(std::get<GraphRef>(graph).toStringRepresentation());
      }
      return SparqlTripleSimpleWithGraph(triple.subject_, triple.predicate_,
                                         triple.object_, g);
    };
    updateClause::GraphUpdate up{
        ad_utility::transform(triples, transformTurtleTriple), {}};
    res._clause = parsedQuery::UpdateClause{up};
    return res;
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformPost);

  static ParsedQuery transformGet(const GraphOrDefault& graph) {
    ParsedQuery res;
    res._clause = parsedQuery::ConstructClause(
        {{Variable("?s"), Variable("?p"), Variable("?o")}});
    res._rootGraphPattern = {};
    parsedQuery::GraphPattern selectSPO;
    selectSPO._graphPatterns.emplace_back(parsedQuery::BasicGraphPattern{
        {SparqlTriple(Variable("?s"), "?p", Variable("?o"))}});
    if (std::holds_alternative<ad_utility::triple_component::Iri>(graph)) {
      parsedQuery::GroupGraphPattern selectSPOWithGraph{
          std::move(selectSPO),
          std::get<ad_utility::triple_component::Iri>(graph)};
      res._rootGraphPattern._graphPatterns.emplace_back(
          std::move(selectSPOWithGraph));
    } else {
      AD_CORRECTNESS_CHECK(std::holds_alternative<DEFAULT>(graph));
      res._rootGraphPattern = std::move(selectSPO);
    }
    return res;
  }
  FRIEND_TEST(GraphStoreProtocolTest, transformGet);

 public:
  // Every Graph Store Protocol requests has equivalent SPARQL Query or Update.
  // Transform the Graph Store Protocol request into it's equivalent Query or
  // Update.
  static ParsedQuery transformGraphStoreProtocol(
      const ad_utility::httpUtils::HttpRequest auto& rawRequest) {
    ad_utility::url_parser::ParsedUrl parsedUrl =
        ad_utility::url_parser::parseRequestTarget(rawRequest.target());
    GraphOrDefault graph = extractTargetGraph(parsedUrl.parameters_);

    using enum boost::beast::http::verb;
    auto method = rawRequest.method();
    if (method == get) {
      return transformGet(graph);
    } else if (method == put) {
      throw std::runtime_error(
          "PUT in the SPARQL Graph Store HTTP Protocol is not yet implemented "
          "in QLever.");
    } else if (method == delete_) {
      throw std::runtime_error(
          "DELETE in the SPARQL Graph Store HTTP Protocol is not yet "
          "implemented in QLever.");
    } else if (method == post) {
      return transformPost(rawRequest, graph);
    } else if (method == head) {
      throw std::runtime_error(
          "HEAD in the SPARQL Graph Store HTTP Protocol is not yet implemented "
          "in QLever.");
    } else if (method == patch) {
      throw std::runtime_error(
          "PATCH in the SPARQL Graph Store HTTP Protocol is not yet "
          "implemented in QLever.");
    } else {
      throw std::runtime_error(
          absl::StrCat("Unsupported HTTP method \"",
                       std::string_view{rawRequest.method_string()},
                       "\" for the SPARQL Graph Store HTTP Protocol."));
    }
  }

 private:
  // Extract the graph to be acted upon using `Indirect Graph
  // Identification`.
  static GraphOrDefault extractTargetGraph(
      const ad_utility::url_parser::ParamValueMap& params);
  FRIEND_TEST(GraphStoreProtocolTest, extractTargetGraph);
};
