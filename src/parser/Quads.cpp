// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "Quads.h"

#include "RdfParser.h"

// ____________________________________________________________________________________
void Quads::addFreeTriples(ad_utility::sparql_types::Triples triples) {
  ad_utility::appendVector(freeTriples_, std::move(triples));
}

// ____________________________________________________________________________________
void Quads::addGraphTriples(IriOrVariable graph,
                            ad_utility::sparql_types::Triples triples) {
  graphTriples_.emplace_back(std::move(graph), std::move(triples));
}

// _____________________________________________________________________________
// TODO: deduplicate these two
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
  std::vector<SparqlTripleSimpleWithGraph> quads;
  ad_utility::appendVector(
      quads, transformTriplesTemplate(freeTriples_,
                                      Iri(std::string{DEFAULT_GRAPH_IRI})));
  for (const auto& [graph, triples] : graphTriples_) {
    ad_utility::appendVector(
        quads,
        transformTriplesTemplate(
            triples,
            std::visit(
                [](const auto& graph) -> SparqlTripleSimpleWithGraph::Graph {
                  return graph;
                },
                graph)));
  }
  return quads;
}

// ____________________________________________________________________________________
std::vector<parsedQuery::GraphPatternOperation> Quads::getOperations() const {
  std::vector<parsedQuery::GraphPatternOperation> operations;
  // TODO: shorten these. function into simple and then add graph?
  operations.emplace_back(parsedQuery::BasicGraphPattern{ad_utility::transform(
      freeTriples_, [](const std::array<GraphTerm, 3>& triple) {
        return SparqlTriple::fromSimple(SparqlTripleSimple(
            visitGraphTerm(triple[0]), visitGraphTerm(triple[1]),
            visitGraphTerm(triple[2])));
      })});

  using GraphSpec = parsedQuery::GroupGraphPattern::GraphSpec;
  for (const auto& [graph, triples] : graphTriples_) {
    parsedQuery::GraphPattern child;
    child._graphPatterns.emplace_back(
        parsedQuery::BasicGraphPattern{ad_utility::transform(
            triples, [](const std::array<GraphTerm, 3>& triple) {
              return SparqlTriple::fromSimple(SparqlTripleSimple(
                  visitGraphTerm(triple[0]), visitGraphTerm(triple[1]),
                  visitGraphTerm(triple[2])));
            })});
    parsedQuery::GroupGraphPattern::GraphSpec graphTC = std::visit(
        ad_utility::OverloadCallOperator{
            [](const Iri& graph) -> GraphSpec {
              return TripleComponent::Iri::fromIriref(graph.toSparql());
            },
            [](const Variable& graph) -> GraphSpec { return graph; }},
        graph);
    operations.emplace_back(
        parsedQuery::GroupGraphPattern{std::move(child), graphTC});
  }
  return operations;
}
