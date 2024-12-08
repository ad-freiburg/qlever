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
 public:
  // Every Graph Store Protocol requests has equivalent SPARQL Query or Update.
  // Transform the Graph Store Protocol request into it's equivalent Query or
  // Update.
  static ParsedQuery transformGraphStoreProtocol(
      const ad_utility::httpUtils::HttpRequest auto& rawRequest) {
    ad_utility::url_parser::ParsedUrl parsedUrl =
        ad_utility::url_parser::parseRequestTarget(rawRequest.target());
    GraphOrDefault graph = extractTargetGraph(parsedUrl.parameters_);
    auto visitGet = [&graph]() -> ParsedQuery {
      ParsedQuery res;
      res._clause = parsedQuery::ConstructClause(
          {{Variable("?s"), Variable("?p"), Variable("?o")}});
      res._rootGraphPattern = {};
      parsedQuery::GraphPattern selectSPO;
      selectSPO._graphPatterns.emplace_back(parsedQuery::BasicGraphPattern{
          {SparqlTriple(Variable("?s"), "?p", Variable("?o"))}});
      // TODO: extract this wrapping into a lambda
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
    };

    using namespace boost::beast::http;
    using Re2Parser = RdfStringParser<TurtleParser<Tokenizer>>;
    using NQuadRe2Parser = RdfStringParser<NQuadParser<Tokenizer>>;
    auto visitPost = [&graph, &rawRequest]() -> ParsedQuery {
      auto contentType = rawRequest.at(field::content_type);
      auto type = ad_utility::getMediaTypeFromAcceptHeader(contentType);
      std::vector<TurtleTriple> triples;
      switch (type.value()) {
        case ad_utility::MediaType::turtle:
        case ad_utility::MediaType::ntriples: {
          Re2Parser parser = Re2Parser();
          parser.setInputStream(rawRequest.body());
          triples = parser.parseAndReturnAllTriples();
          break;
        }
        case ad_utility::MediaType::nquads: {
          NQuadRe2Parser parser = NQuadRe2Parser();
          parser.setInputStream(rawRequest.body());
          triples = parser.parseAndReturnAllTriples();
          break;
        }
        default: {
          throw std::runtime_error(
              absl::StrCat("Mediatype \"", ad_utility::toString(type.value()),
                           "\" is not supported for SPARQL Graph Store HTTP "
                           "Protocol in QLever."));
          break;
        }
      }
      ParsedQuery res;
      auto transformTurtleTriple =
          [&graph](const TurtleTriple& triple) -> SparqlTripleSimpleWithGraph {
        auto triplesGraph =
            [&triple]() -> std::variant<std::monostate, Iri, Variable> {
          if (triple.graphIri_.isIri()) {
            return Iri(triple.graphIri_.getIri().toStringRepresentation());
          } else if (triple.graphIri_.isVariable()) {
            return triple.graphIri_.getVariable();
          } else {
            AD_CORRECTNESS_CHECK(triple.graphIri_.isId());
            AD_CORRECTNESS_CHECK(triple.graphIri_.getId() ==
                                 qlever::specialIds().at(DEFAULT_GRAPH_IRI));
            return std::monostate{};
          }
        }();
        if (std::holds_alternative<GraphRef>(graph)) {
          triplesGraph =
              Iri(std::get<GraphRef>(graph).toStringRepresentation());
        }
        return SparqlTripleSimpleWithGraph(triple.subject_, triple.predicate_,
                                           triple.object_, triplesGraph);
      };
      updateClause::GraphUpdate up{
          ad_utility::transform(triples, transformTurtleTriple), {}};
      res._clause = parsedQuery::UpdateClause{up};
      return res;
    };

    auto method = rawRequest.method();
    if (method == verb::get) {
      return visitGet();
    } else if (method == verb::put) {
      throw std::runtime_error(
          "PUT in the SPARQL Graph Store HTTP Protocol is not yet implemented "
          "in QLever.");
    } else if (method == verb::delete_) {
      throw std::runtime_error(
          "DELETE in the SPARQL Graph Store HTTP Protocol is not yet "
          "implemented in QLever.");
    } else if (method == verb::post) {
      return visitPost();
    } else if (method == verb::head) {
      throw std::runtime_error(
          "HEAD in the SPARQL Graph Store HTTP Protocol is not yet implemented "
          "in QLever.");
    } else if (method == verb::patch) {
      throw std::runtime_error(
          "PATCH in the SPARQL Graph Store HTTP Protocol is not yet "
          "implemented in QLever.");
    } else {
      throw std::runtime_error(
          absl::StrCat("Unsupported HTTP method \"", "",
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
