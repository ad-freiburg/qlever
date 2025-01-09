// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/GraphStoreProtocol.h"

#include <boost/beast.hpp>

// ____________________________________________________________________________
GraphOrDefault GraphStoreProtocol::extractTargetGraph(
    const ad_utility::url_parser::ParamValueMap& params) {
  const std::optional<std::string> graphIri =
      ad_utility::url_parser::checkParameter(params, "graph", std::nullopt);
  const bool isDefault =
      ad_utility::url_parser::checkParameter(params, "default", "").has_value();
  if (!(graphIri.has_value() || isDefault)) {
    throw std::runtime_error(
        "No graph IRI specified in the request. Specify one using either the "
        "query parameter `default` or `graph=<iri>`.");
  }
  if (graphIri.has_value() && isDefault) {
    throw std::runtime_error(
        "Only one of `default` and `graph` may be used for graph "
        "identification.");
  }
  if (graphIri.has_value()) {
    return GraphRef::fromIrirefWithoutBrackets(graphIri.value());
  } else {
    AD_CORRECTNESS_CHECK(isDefault);
    return DEFAULT{};
  }
}

// ____________________________________________________________________________
void GraphStoreProtocol::throwUnsupportedMediatype(const string& mediatype) {
  throw UnsupportedMediatypeError(absl::StrCat(
      "Mediatype \"", mediatype,
      "\" is not supported for SPARQL Graph Store HTTP Protocol in QLever. "
      "Supported: ",
      toString(ad_utility::MediaType::turtle), ", ",
      toString(ad_utility::MediaType::ntriples), "."));
}

// ____________________________________________________________________________
std::vector<TurtleTriple> GraphStoreProtocol::parseTriples(
    const string& body, const ad_utility::MediaType contentType) {
  using Re2Parser = RdfStringParser<TurtleParser<Tokenizer>>;
  std::vector<TurtleTriple> triples;
  switch (contentType) {
    case ad_utility::MediaType::turtle:
    case ad_utility::MediaType::ntriples: {
      auto parser = Re2Parser();
      parser.setInputStream(body);
      triples = parser.parseAndReturnAllTriples();
      break;
    }
    default: {
      throwUnsupportedMediatype(toString(contentType));
    }
  }
  return triples;
}

// ____________________________________________________________________________
std::vector<SparqlTripleSimpleWithGraph> GraphStoreProtocol::convertTriples(
    const GraphOrDefault& graph, std::vector<TurtleTriple> triples) {
  SparqlTripleSimpleWithGraph::Graph tripleGraph{std::monostate{}};
  if (std::holds_alternative<GraphRef>(graph)) {
    tripleGraph = Iri(std::get<GraphRef>(graph).toStringRepresentation());
  }
  auto transformTurtleTriple = [&tripleGraph](TurtleTriple triple) {
    AD_CORRECTNESS_CHECK(triple.graphIri_.isId() &&
                         triple.graphIri_.getId() ==
                             qlever::specialIds().at(DEFAULT_GRAPH_IRI));

    return SparqlTripleSimpleWithGraph(std::move(triple.subject_),
                                       std::move(triple.predicate_),
                                       std::move(triple.object_), tripleGraph);
  };
  return ad_utility::transform(triples, transformTurtleTriple);
}

// ____________________________________________________________________________
ParsedQuery GraphStoreProtocol::transformGet(const GraphOrDefault& graph) {
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
