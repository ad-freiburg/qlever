// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/GraphStoreProtocol.h"

#include "parser/Tokenizer.h"
#include "util/http/beast.h"

// ____________________________________________________________________________
void GraphStoreProtocol::throwUnsupportedMediatype(
    const string_view& mediatype) {
  throw HttpError(
      boost::beast::http::status::unsupported_media_type,
      absl::StrCat(
          "Mediatype \"", mediatype,
          "\" is not supported for SPARQL Graph Store HTTP Protocol in QLever. "
          "Supported: ",
          toString(ad_utility::MediaType::turtle), ", ",
          toString(ad_utility::MediaType::ntriples), "."));
}

// ____________________________________________________________________________
void GraphStoreProtocol::throwNotYetImplementedHTTPMethod(
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
    tripleGraph = std::get<GraphRef>(graph);
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
  // Construct the parsed query from its short equivalent SPARQL Update string.
  // This is easier and also provides e.g. the `_originalString` field.
  auto query = [&graph]() -> std::string {
    if (const auto* iri =
            std::get_if<ad_utility::triple_component::Iri>(&graph)) {
      return absl::StrCat("CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ",
                          iri->toStringRepresentation(), " { ?s ?p ?o } }");
    } else {
      return "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
    }
  }();
  return SparqlParser::parseQuery(query);
}

// ____________________________________________________________________________
ParsedQuery GraphStoreProtocol::transformDelete(const GraphOrDefault& graph) {
  // Construct the parsed update from its short equivalent SPARQL Update string.
  // This is easier and also provides e.g. the `_originalString` field.
  auto update = [&graph]() -> std::string {
    if (const auto* iri =
            std::get_if<ad_utility::triple_component::Iri>(&graph)) {
      return absl::StrCat("DROP GRAPH ", iri->toStringRepresentation());
    } else {
      return "DROP DEFAULT";
    }
  }();
  auto updates = SparqlParser::parseUpdate(update);
  AD_CORRECTNESS_CHECK(updates.size() == 1);
  return std::move(updates.at(0));
}
