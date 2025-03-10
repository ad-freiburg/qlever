// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "Quads.h"

#include "RdfParser.h"

// _____________________________________________________________________________
// TODO: deduplicate this
TripleComponent visitGraphTerm(const GraphTerm& graphTerm) {
  return graphTerm.visit([]<typename T>(const T& element) -> TripleComponent {
    if constexpr (std::is_same_v<T, Variable>) {
      return element;
    } else if constexpr (std::is_same_v<T, Literal> || std::is_same_v<T, Iri>) {
      return RdfStringParser<TurtleParser<TokenizerCtre>>::parseTripleObject(
          element.toSparql());
    } else {
      return element.toSparql();
    }
  });
}

// ____________________________________________________________________________________
// Transform the triples and sets the graph on all triples.
vector<SparqlTripleSimpleWithGraph> transformTriplesTemplate(
    ad_utility::sparql_types::Triples triples,
    const SparqlTripleSimpleWithGraph::Graph& graph) {
  auto convertTriple = [&graph](const std::array<GraphTerm, 3>& triple)
      -> SparqlTripleSimpleWithGraph {
    return {visitGraphTerm(triple[0]), visitGraphTerm(triple[1]),
            visitGraphTerm(triple[2]), graph};
  };

  return ad_utility::transform(triples, convertTriple);
}

// Re-wraps the value into a variant `T` which has additional values.
template <typename T>
T expandVariant(const Quads::IriOrVariable& graph) {
  return std::visit([](const auto& graph) -> T { return graph; }, graph);
};

// ____________________________________________________________________________________
std::vector<SparqlTripleSimpleWithGraph> Quads::getQuads() const {
  std::vector<SparqlTripleSimpleWithGraph> quads;
  ad_utility::appendVector(
      quads, transformTriplesTemplate(freeTriples_, std::monostate{}));
  for (const auto& [graph, triples] : graphTriples_) {
    ad_utility::appendVector(
        quads,
        transformTriplesTemplate(
            triples, expandVariant<SparqlTripleSimpleWithGraph::Graph>(graph)));
  }
  return quads;
}

// ____________________________________________________________________________________
std::vector<parsedQuery::GraphPatternOperation> Quads::getOperations() const {
  auto toSparqlTriple = [](const std::array<GraphTerm, 3>& triple) {
    return SparqlTriple::fromSimple(
        SparqlTripleSimple(visitGraphTerm(triple[0]), visitGraphTerm(triple[1]),
                           visitGraphTerm(triple[2])));
  };

  std::vector<parsedQuery::GraphPatternOperation> operations;
  operations.emplace_back(parsedQuery::BasicGraphPattern{
      ad_utility::transform(freeTriples_, toSparqlTriple)});

  using GraphSpec = parsedQuery::GroupGraphPattern::GraphSpec;
  for (const auto& [graph, triples] : graphTriples_) {
    // We need a `GroupGraphPattern` where the graph is set. This contains the
    // triples inside another `GraphPattern`.
    parsedQuery::GraphPattern tripleSubPattern;
    tripleSubPattern._graphPatterns.emplace_back(parsedQuery::BasicGraphPattern{
        ad_utility::transform(triples, toSparqlTriple)});
    operations.emplace_back(parsedQuery::GroupGraphPattern{
        std::move(tripleSubPattern), expandVariant<GraphSpec>(graph)});
  }
  return operations;
}
