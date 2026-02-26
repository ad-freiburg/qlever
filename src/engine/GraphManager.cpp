// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "GraphManager.h"

#include "../../test/util/RuntimeParametersTestHelpers.h"
#include "QueryPlanner.h"
#include "global/RuntimeParameters.h"
#include "parser/SparqlParser.h"

// _____________________________________________________________________________
GraphManager::GraphManager(std::vector<std::string> graphs,
                           uint64_t allocatedGraphs)
    : graphs_(std::move(graphs)), allocatedGraphs_(allocatedGraphs) {
  AD_LOG_INFO << "GraphManager initialized with " << graphs_.size()
              << " graphs and " << allocatedGraphs_
              << " already allocated graphs." << std::endl;
}

// _____________________________________________________________________________
GraphManager GraphManager::fillFromIndex(
    const EncodedIriManager* encodedIriManager, QueryExecutionContext& qec) {
  // TODO: set and restore the runtime parameters differently or move it from
  // testing folder
  auto groupByHashmap =
      setRuntimeParameterForTest<&RuntimeParameters::groupByHashMapEnabled_>(
          true);
  auto defaultIsNamed = setRuntimeParameterForTest<
      &RuntimeParameters::treatDefaultGraphAsNamedGraph_>(true);

  auto query = SparqlParser::parseQuery(encodedIriManager,
                                        "SELECT ?g WHERE { GRAPH "
                                        "?g { ?s ?p ?o } } GROUP BY ?g");

  QueryPlanner qp(&qec, std::make_shared<ad_utility::CancellationHandle<>>());
  auto executionTree = qp.createExecutionTree(query);
  auto result = executionTree.getResult(false);
  std::vector<std::string> existingGraphs;
  for (const auto& row : result->idTable()) {
    AD_CORRECTNESS_CHECK(row[0].getDatatype() == Datatype::VocabIndex);
    auto graph = qec.getIndex().indexToString(row[0].getVocabIndex());
    existingGraphs.push_back(std::move(graph));
  }

  return fromExistingGraphs(std::move(existingGraphs));
}

// _____________________________________________________________________________
GraphManager GraphManager::fromExistingGraphs(std::vector<std::string> graphs) {
  auto alreadyCreatedGraphs =
      graphs | ql::views::filter([](const auto& graph) {
        return graph.starts_with(QLEVER_NEW_GRAPH_PREFIX);
      }) |
      ql::views::transform([](const auto& graph) {
        return graph.substr(QLEVER_NEW_GRAPH_PREFIX.size(),
                            graph.size() - QLEVER_NEW_GRAPH_PREFIX.size() - 1);
      }) |
      ql::views::transform([](const auto& suffix) -> uint64_t {
        try {
          return std::stoul(suffix);
        } catch (const std::invalid_argument&) {
          AD_LOG_WARN << "Internal graph with invalid suffix " << suffix
                      << std::endl;
          return 0;
        }
      });
  auto allocatedGraphs =
      alreadyCreatedGraphs.begin() == alreadyCreatedGraphs.end()
          ? 0
          : ql::ranges::max(alreadyCreatedGraphs) + 1;

  return GraphManager(std::move(graphs), allocatedGraphs);
}

// _____________________________________________________________________________
void GraphManager::addGraphs(std::vector<std::string> graphs) {
  auto internalGraphSuffixes =
      graphs | ql::views::filter([](const auto& graph) {
        return graph.starts_with(QLEVER_NEW_GRAPH_PREFIX);
      }) |
      ql::views::transform([](const auto& graph) {
        return graph.substr(QLEVER_NEW_GRAPH_PREFIX.size(),
                            graph.size() - QLEVER_NEW_GRAPH_PREFIX.size() - 1);
      });
  ql::ranges::for_each(internalGraphSuffixes, [this](const auto& suffix) {
    try {
      auto graphId = std::stoul(suffix);
      AD_CORRECTNESS_CHECK(graphId < allocatedGraphs_);
    } catch (const std::invalid_argument&) {
      AD_LOG_WARN << "Invalid graph suffix " << suffix
                  << " from internal namespace being inserted." << std::endl;
    }
  });
  ql::ranges::move(std::move(graphs), std::back_inserter(graphs_));
  ql::ranges::sort(graphs_);
  graphs_.erase(std::ranges::unique(graphs_).begin(), graphs_.end());
  AD_LOG_INFO << "We now have " << graphs_.size() << " unique graphs."
              << std::endl;
}

// _____________________________________________________________________________
bool GraphManager::graphExists(const std::string& graph) const {
  return ql::ranges::binary_search(graphs_, graph);
}

// _____________________________________________________________________________
ad_utility::triple_component::Iri GraphManager::getNewInternalGraph() {
  auto graphId = allocatedGraphs_++;
  return ad_utility::triple_component::Iri::fromIriref(
      QLEVER_NEW_GRAPH_PREFIX + std::to_string(graphId) + ">");
}
