// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/GraphStoreProtocol.h"

#include <boost/beast.hpp>

#include "parser/RdfParser.h"

// ____________________________________________________________________________
GraphOrDefault GraphStoreProtocol::extractTargetGraph(
    const ad_utility::url_parser::ParamValueMap& params) {
  // Extract the graph to be acted upon using `Indirect Graph
  // Identification`.
  const auto graphIri =
      ad_utility::url_parser::checkParameter(params, "graph", std::nullopt);
  const auto isDefault =
      ad_utility::url_parser::checkParameter(params, "default", "");
  if (!(graphIri || isDefault)) {
    throw std::runtime_error("No graph IRI specified in the request.");
  }
  if (graphIri && isDefault) {
    throw std::runtime_error(
        "Only one of `default` and `graph` may be used for graph "
        "identification.");
  }
  if (graphIri) {
    return GraphRef::fromIriref(graphIri.value());
  } else {
    AD_CORRECTNESS_CHECK(isDefault);
    return DEFAULT{};
  }
}

// ____________________________________________________________________________
ParsedQuery GraphStoreProtocol::transformGraphStoreProtocol(
    const ad_utility::httpUtils::HttpRequest auto& rawRequest,
    const ad_utility::url_parser::ParsedRequest& request) {
  // TODO: only use the raw request. It makes no sense to use `ParsedRequest`
  // here - the interface just doesn't fit.
  GraphOrDefault graph = extractTargetGraph(request.parameters_);
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
    ParsedQuery res;
    updateClause::GraphUpdate up{{}, {}};
    if (std::holds_alternative<ad_utility::triple_component::Iri>(graph)) {
      up.with_ = std::get<ad_utility::triple_component::Iri>(graph);
    }
    res._clause = parsedQuery::UpdateClause{up};
    return res;
  };

  auto method = rawRequest.method();
  if (method == verb::get) {
    return visitGet();
  } else if (method == verb::put) {
    throw std::runtime_error(
        "PUT in the SPARQL Graph Store HTTP Protocol is not yet implemented in "
        "QLever.");
  } else if (method == verb::delete_) {
    throw std::runtime_error(
        "DELETE in the SPARQL Graph Store HTTP Protocol is not yet implemented "
        "in QLever.");
  } else if (method == verb::post) {
    return visitPost();
  } else if (method == verb::head) {
    throw std::runtime_error(
        "HEAD in the SPARQL Graph Store HTTP Protocol is not yet implemented "
        "in QLever.");
  } else if (method == verb::patch) {
    throw std::runtime_error(
        "PATCH in the SPARQL Graph Store HTTP Protocol is not yet implemented "
        "in QLever.");
  } else {
    throw std::runtime_error(
        absl::StrCat("Unsupported HTTP method \"", "",
                     "\" for the SPARQL Graph Store HTTP Protocol."));
  }
}
