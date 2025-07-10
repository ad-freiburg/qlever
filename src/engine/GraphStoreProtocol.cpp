// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/GraphStoreProtocol.h"

#include "parser/SparqlParser.h"
#include "parser/Tokenizer.h"
#include "util/http/beast.h"

// ____________________________________________________________________________
void GraphStoreProtocol::throwUnsupportedMediatype(
    const std::string_view& mediatype) {
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
    const std::string& body, const ad_utility::MediaType contentType) {
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
  std::string query;
  if (const auto* iri =
          std::get_if<ad_utility::triple_component::Iri>(&graph)) {
    query = absl::StrCat("CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ",
                         iri->toStringRepresentation(), " { ?s ?p ?o } }");
  } else {
    query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
  }
  return SparqlParser::parseQuery(query);
}
