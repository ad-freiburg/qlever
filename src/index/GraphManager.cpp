// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "GraphManager.h"

#include "../../test/util/RuntimeParametersTestHelpers.h"
#include "../engine/QueryPlanner.h"
#include "global/RuntimeParameters.h"
#include "parser/SparqlParser.h"

// _____________________________________________________________________________
GraphManager::GraphManager(ad_utility::HashSet<Id> graphs)
    : graphs_(std::move(graphs)) {}

// _____________________________________________________________________________
GraphManager GraphManager::fromExistingGraphs(ad_utility::HashSet<Id> graphs) {
  // TODO: might want to check here that all are LocalVocab
  return GraphManager(std::move(graphs));
}

// _____________________________________________________________________________
void GraphManager::addGraphs(ad_utility::HashSet<Id> graphs) {
  // The IDs may be temporary LVIs. Rewrite them to our own LocalVOc
  auto localGraphs =
      graphs | ql::views::transform([this](const auto& graph) {
        if (graph.getDatatype() == Datatype::LocalVocabIndex) {
          return Id::makeFromLocalVocabIndex(
              graphLocalVocab_.getIndexAndAddIfNotContained(
                  *graph.getLocalVocabIndex()));
        }
        AD_CORRECTNESS_CHECK(graph.getDatatype() == Datatype::VocabIndex);
        return graph;
      });
  graphs_.wlock()->insert(localGraphs.begin(), localGraphs.end());
}

// _____________________________________________________________________________
bool GraphManager::graphExists(const Id& graph) const {
  auto graphs = graphs_.rlock();
  return graphs->find(graph) != graphs->end();
}

// _____________________________________________________________________________
GraphManager::GraphNamespaceManager& GraphManager::getNamespaceManager() {
  AD_CORRECTNESS_CHECK(namespaceManager_.has_value());
  return namespaceManager_.value();
}

// _____________________________________________________________________________
void GraphManager::initializeNamespaceManager(std::string prefix,
                                              const GraphManager& graphManager,
                                              const Index::Vocab& vocab) {
  namespaceManager_ = GraphNamespaceManager::fromGraphManager(
      std::move(prefix), graphManager, vocab);
}

// _____________________________________________________________________________
GraphManager::GraphNamespaceManager::GraphNamespaceManager(
    std::string prefix, uint64_t allocatedGraphs)
    : prefix_(std::move(prefix)), allocatedGraphs_(allocatedGraphs) {}

// _____________________________________________________________________________
GraphManager::GraphNamespaceManager
GraphManager::GraphNamespaceManager::fromGraphManager(
    std::string prefix, const GraphManager& graphManager,
    const Index::Vocab& vocab) {
  auto graphs = graphManager.getGraphs();
  auto alreadyCreatedGraphs =
      *graphs | ql::views::transform([&vocab](const auto& graphId) {
        if (graphId.getDatatype() == Datatype::VocabIndex) {
          return vocab[graphId.getVocabIndex()];
        }
        AD_CORRECTNESS_CHECK(graphId.getDatatype() ==
                             Datatype::LocalVocabIndex);
        AD_CORRECTNESS_CHECK(graphId.getLocalVocabIndex()->isIri());
        return graphId.getLocalVocabIndex()->toStringRepresentation();
      }) |
      ql::views::filter([](const auto& graph) {
        return graph.starts_with(QLEVER_NEW_GRAPH_PREFIX);
      }) |
      ql::views::transform([](const auto& graph) {
        return graph.substr(QLEVER_NEW_GRAPH_PREFIX.size(),
                            graph.size() - QLEVER_NEW_GRAPH_PREFIX.size() - 1);
      }) |
      ql::views::transform([](const auto& suffix) -> uint64_t {
        try {
          return std::stoull(suffix);
        } catch (const std::invalid_argument&) {
          AD_LOG_WARN << "Internal graph with invalid suffix " << suffix
                      << std::endl;
          // This wastes 1 graph in the case that all graphs have non-numeric
          // (considered invalid) IDs. This is negligible.
          return 0;
        }
      });
  auto allocatedGraphs =
      alreadyCreatedGraphs.begin() == alreadyCreatedGraphs.end()
          ? 0
          : ql::ranges::max(alreadyCreatedGraphs) + 1;

  return GraphNamespaceManager(std::move(prefix), allocatedGraphs);
}

// _____________________________________________________________________________
ad_utility::triple_component::Iri
GraphManager::GraphNamespaceManager::allocateNewGraph() {
  auto graphId = allocatedGraphs_.withWriteLock(
      [](auto& allocatedGraphs) { return allocatedGraphs++; });
  return ad_utility::triple_component::Iri::fromIriref(
      prefix_ + std::to_string(graphId) + ">");
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const GraphManager& graphManager) {
  auto graphs = graphManager.graphs_.rlock();
  j["graphs"] = *graphs |
                ql::views::transform([](const auto& id) -> std::string {
                  return std::to_string(id.getBits());
                }) |
                ::ranges::to<std::vector>();
  j["namespaces"]["new-graphs"] = graphManager.namespaceManager_.value();
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j, GraphManager& graphManager) {
  auto graphs = static_cast<std::vector<std::string>>(j["graphs"]) |
                ql::views::transform([](const auto& idStr) -> Id {
                  return Id::fromBits(std::stoull(idStr));
                });
  graphManager.graphs_.withWriteLock([&graphs](auto& graphsMember) {
    graphsMember = ad_utility::HashSet<Id>(graphs.begin(), graphs.end());
  });
  graphManager.namespaceManager_ =
      j["namespaces"]["new-graphs"].get<GraphManager::GraphNamespaceManager>();
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const GraphManager& graphManager) {
  os << "GraphManager(graphs=[";
  auto graphs = graphManager.graphs_.rlock();
  os << absl::StrJoin(*graphs | ql::views::transform([](const auto& id) {
    std::ostringstream os;
    os << id;
    return os.str();
  }),
                      ", ");
  os << "], namespaceManager=";
  if (graphManager.namespaceManager_.has_value()) {
    os << graphManager.namespaceManager_.value();
  } else {
    os << "<Not Initialized>";
  }
  os << ")";
  return os;
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j,
             const GraphManager::GraphNamespaceManager& namespaceManager) {
  j["prefix"] = namespaceManager.prefix_;
  j["allocatedGraphs"] = *namespaceManager.allocatedGraphs_.rlock();
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j,
               GraphManager::GraphNamespaceManager& namespaceManager) {
  namespaceManager.prefix_ = j["prefix"].get<std::string>();
  namespaceManager.allocatedGraphs_.withWriteLock([&j](auto& allocatedGraphs) {
    allocatedGraphs = j["allocatedGraphs"].get<uint64_t>();
  });
}

// _____________________________________________________________________________
std::ostream& operator<<(
    std::ostream& os,
    const GraphManager::GraphNamespaceManager& namespaceManager) {
  os << "GraphNamespaceManager(prefix=\"" << namespaceManager.prefix_
     << "\", allocatedGraphs=" << *namespaceManager.allocatedGraphs_.rlock()
     << ")";
  return os;
}
