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
static T expandVariant(const ad_utility::sparql_types::VarOrIri& graph) {
  return std::visit([](const auto& graph) -> T { return graph; }, graph);
};

// ____________________________________________________________________________________
std::vector<SparqlTripleSimpleWithGraph> Quads::toTriplesWithGraph() const {
  std::vector<SparqlTripleSimpleWithGraph> quads;
  size_t numTriplesInGraphs = std::accumulate(
      graphTriples_.begin(), graphTriples_.end(), 0,
      [](size_t acc, const GraphBlock& block) {
        return acc + get<ad_utility::sparql_types::Triples>(block).size();
      });
  quads.reserve(numTriplesInGraphs + freeTriples_.size());
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
std::vector<parsedQuery::GraphPatternOperation>
Quads::toGraphPatternOperations() const {
  auto toSparqlTriple = [](const std::array<GraphTerm, 3>& triple) {
    return SparqlTriple::fromSimple(SparqlTripleSimple(
        triple[0].toTripleComponent(), triple[1].toTripleComponent(),
        triple[2].toTripleComponent()));
  };

  using namespace parsedQuery;
  std::vector<GraphPatternOperation> operations;
  operations.emplace_back(
      BasicGraphPattern{ad_utility::transform(freeTriples_, toSparqlTriple)});

  for (const auto& [graph, triples] : graphTriples_) {
    // We need a `GroupGraphPattern` where the graph is set. This contains the
    // triples inside another `GraphPattern`.
    GraphPattern tripleSubPattern;
    tripleSubPattern._graphPatterns.emplace_back(
        BasicGraphPattern{ad_utility::transform(triples, toSparqlTriple)});
    operations.emplace_back(
        GroupGraphPattern{std::move(tripleSubPattern),
                          expandVariant<GroupGraphPattern::GraphSpec>(graph)});
  }
  return operations;
}

// ____________________________________________________________________________________
void Quads::forAllVariables(absl::FunctionRef<void(const Variable&)> f) {
  auto visitGraphTerm = [&f](const GraphTerm& t) {
    if (std::holds_alternative<Variable>(t)) {
      f(std::get<Variable>(t));
    }
  };
  auto visitTriple = [&visitGraphTerm](const std::array<GraphTerm, 3>& triple) {
    const auto& [s, p, o] = triple;
    visitGraphTerm(s);
    visitGraphTerm(p);
    visitGraphTerm(o);
  };
  auto visitGraphBlock = [&visitTriple, &f](const GraphBlock& block) {
    const auto& [graph, triples] = block;
    if (holds_alternative<Variable>(graph)) {
      f(get<Variable>(graph));
    }
    ql::ranges::for_each(triples, visitTriple);
  };
  ql::ranges::for_each(graphTriples_, visitGraphBlock);
  ql::ranges::for_each(freeTriples_, visitTriple);
}
