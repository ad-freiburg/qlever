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
// Transform the triples and sets the graph.
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

// ____________________________________________________________________________________
std::vector<SparqlTripleSimpleWithGraph> Quads::getQuads() const {
  // Re-wraps value into the `SparqlTripleSimpleWithGraph::Graph` variant which
  // also has the monostate member.
  auto expandVariant = [](const IriOrVariable& graph) {
    return std::visit(
        [](const auto& graph) -> SparqlTripleSimpleWithGraph::Graph {
          return graph;
        },
        graph);
  };

  std::vector<SparqlTripleSimpleWithGraph> quads;
  ad_utility::appendVector(
      quads, transformTriplesTemplate(freeTriples_, std::monostate{}));
  for (const auto& [graph, triples] : graphTriples_) {
    ad_utility::appendVector(
        quads, transformTriplesTemplate(triples, expandVariant(graph)));
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
    // TODO: this is stupd. make these the same types
    GraphSpec graphTC = std::visit(
        ad_utility::OverloadCallOperator{
            [](const Iri& graph) -> GraphSpec {
              return TripleComponent::Iri::fromIriref(graph.toSparql());
            },
            [](const Variable& graph) -> GraphSpec { return graph; }},
        graph);
    operations.emplace_back(
        parsedQuery::GroupGraphPattern{std::move(tripleSubPattern), graphTC});
  }
  return operations;
}
