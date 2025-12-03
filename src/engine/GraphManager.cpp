// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "GraphManager.h"

#include "../../test/util/RuntimeParametersTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "global/Constants.h"
#include "parser/SparqlParser.h"

using namespace ad_utility;

// ____________________________________________________________________________
void GraphManager::initializeFromIndex(
    const EncodedIriManager* encodedIriManager, QueryExecutionContext qec) {
  AD_LOG_INFO << "Calculating graph sizes from dataset ..." << std::endl;
  // TODO: set and restore the runtime parameters differently or move it from
  // testing folder
  auto groupByHashmap =
      setRuntimeParameterForTest<&RuntimeParameters::groupByHashMapEnabled_>(
          true);
  auto defaultIsNamed = setRuntimeParameterForTest<
      &RuntimeParameters::treatDefaultGraphAsNamedGraph_>(false);
  HashMap<std::string, uint64_t> sizes =
      calculateGraphSizes(encodedIriManager, qec);

  auto alreadyCreatedGraphs =
      sizes | ql::views::filter([](const auto& elem) {
        return elem.first.starts_with(QLEVER_NEW_GRAPH_PREFIX);
      }) |
      ql::views::keys | ql::views::transform([](const auto& graphIri) {
        return graphIri.substr(
            QLEVER_NEW_GRAPH_PREFIX.size(),
            graphIri.size() - QLEVER_NEW_GRAPH_PREFIX.size() - 1);
      }) |
      ql::views::transform([](const auto& suffix) -> long {
        try {
          return std::stol(suffix);
        } catch (const std::invalid_argument&) {
          AD_LOG_WARN << "Internal graph with invalid suffix " << suffix
                      << std::endl;
          return -1l;
        }
      });
  auto largestGraph = alreadyCreatedGraphs.begin() == alreadyCreatedGraphs.end()
                          ? -1
                          : ql::ranges::max(alreadyCreatedGraphs);
  AD_LOG_INFO << "Largest found graph: " << largestGraph << std::endl;

  *allocatedGraphs_.wlock() = largestGraph + 1;
  *isInitialized_.wlock() = true;
}

// ____________________________________________________________________________
void GraphManager::initializeForTesting(uint64_t size) {
  *allocatedGraphs_.wlock() = size;
  *isInitialized_.wlock() = true;
}

// ____________________________________________________________________________
HashMap<std::string, uint64_t> GraphManager::calculateGraphSizes(
    const EncodedIriManager* encodedIriManager, QueryExecutionContext qec) {
  auto query =
      SparqlParser::parseQuery(encodedIriManager,
                               "SELECT ?g (COUNT(?g) AS ?count) WHERE { GRAPH "
                               "?g { ?s ?p ?o } } GROUP BY ?g");

  QueryPlanner qp(&qec, std::make_shared<CancellationHandle<>>());
  auto executionTree = qp.createExecutionTree(query);
  auto result = executionTree.getResult(false);
  HashMap<std::string, uint64_t> sizes;
  for (const auto& row : result->idTable()) {
    AD_CORRECTNESS_CHECK(row[0].getDatatype() == Datatype::VocabIndex);
    auto graph = qec.getIndex().indexToString(row[0].getVocabIndex());
    AD_CORRECTNESS_CHECK(row[1].getDatatype() == Datatype::Int);
    auto size = row[1].getInt();
    auto [_, wasInserted] = sizes.insert({graph, size});
    AD_CORRECTNESS_CHECK(wasInserted);
  }
  return sizes;
}

// ____________________________________________________________________________
triple_component::Iri GraphManager::getNewInternalGraph() {
  auto graphId =
      allocatedGraphs_.withWriteLock([](auto& counter) { return counter++; });
  return triple_component::Iri::fromIriref(QLEVER_NEW_GRAPH_PREFIX +
                                           std::to_string(graphId) + ">");
}
