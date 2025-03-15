// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "Quads.h"

// ____________________________________________________________________________________
// Transform the triples and sets the graph on all triples.
vector<SparqlTripleSimpleWithGraph> transformTriplesTemplate(
    ad_utility::sparql_types::Triples triples,
    const SparqlTripleSimpleWithGraph::Graph& graph) {
  auto convertTriple = [&graph](const std::array<GraphTerm, 3>& triple)
      -> SparqlTripleSimpleWithGraph {
    return {triple[0].toTripleComponent(), triple[1].toTripleComponent(),
            triple[2].toTripleComponent(), graph};
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
    return SparqlTriple::fromSimple(SparqlTripleSimple(
        triple[0].toTripleComponent(), triple[1].toTripleComponent(),
        triple[2].toTripleComponent()));
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
