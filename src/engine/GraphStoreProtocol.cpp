// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/GraphStoreProtocol.h"

#include "util/http/beast.h"

// ____________________________________________________________________________
GraphOrDefault GraphStoreProtocol::extractTargetGraph(
    const ad_utility::url_parser::ParamValueMap& params) {
  const std::optional<std::string> graphIri =
      ad_utility::url_parser::checkParameter(params, "graph", std::nullopt);
  const bool isDefault =
      ad_utility::url_parser::checkParameter(params, "default", "").has_value();
  if (graphIri.has_value() == isDefault) {
    throw std::runtime_error(
        "Exactly one of the query parameters default or graph must be set to "
        "identify the graph for the graph store protocol request.");
  }
  if (graphIri.has_value()) {
    return GraphRef::fromIrirefWithoutBrackets(graphIri.value());
  } else {
    AD_CORRECTNESS_CHECK(isDefault);
    return DEFAULT{};
  }
}

// ____________________________________________________________________________
void GraphStoreProtocol::throwUnsupportedMediatype(
    const string_view& mediatype) {
  throw UnsupportedMediatypeError(absl::StrCat(
      "Mediatype \"", mediatype,
      "\" is not supported for SPARQL Graph Store HTTP Protocol in QLever. "
      "Supported: ",
      toString(ad_utility::MediaType::turtle), ", ",
      toString(ad_utility::MediaType::ntriples), "."));
}

// ____________________________________________________________________________
void GraphStoreProtocol::throwUnsupportedHTTPMethod(
    const std::string_view& method) {
  throw std::runtime_error(absl::StrCat(
      method,
      " in the SPARQL Graph Store HTTP Protocol is not yet implemented "
      "in QLever."));
}

// ____________________________________________________________________________
std::vector<TurtleTriple> GraphStoreProtocol::parseTriples(
    const string& body, const ad_utility::MediaType contentType) {
  using Re2Parser = RdfStringParser<TurtleParser<Tokenizer>>;
  switch (contentType) {
    case ad_utility::MediaType::turtle:
    case ad_utility::MediaType::ntriples: {
      auto parser = Re2Parser();
      parser.setInputStream(body);
      return parser.parseAndReturnAllTriples();
    }
    default: {
      throwUnsupportedMediatype(toString(contentType));
    }
  }
}

// ____________________________________________________________________________
std::vector<SparqlTripleSimpleWithGraph> GraphStoreProtocol::convertTriples(
    const GraphOrDefault& graph, std::vector<TurtleTriple> triples) {
  SparqlTripleSimpleWithGraph::Graph tripleGraph{std::monostate{}};
  if (std::holds_alternative<GraphRef>(graph)) {
    tripleGraph = Iri(std::get<GraphRef>(graph).toStringRepresentation());
  }
  auto transformTurtleTriple = [&tripleGraph](TurtleTriple&& triple) {
    AD_CORRECTNESS_CHECK(triple.graphIri_.isId() &&
                         triple.graphIri_.getId() ==
                             qlever::specialIds().at(DEFAULT_GRAPH_IRI));

    return SparqlTripleSimpleWithGraph(std::move(triple.subject_),
                                       std::move(triple.predicate_),
                                       std::move(triple.object_), tripleGraph);
  };
  return ad_utility::transform(std::move(triples), transformTurtleTriple);
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
  if (const auto* iri =
          std::get_if<ad_utility::triple_component::Iri>(&graph)) {
    res.datasetClauses_ =
        parsedQuery::DatasetClauses::fromClauses({DatasetClause{*iri, false}});
  }
  res._rootGraphPattern = std::move(selectSPO);
  return res;
}
