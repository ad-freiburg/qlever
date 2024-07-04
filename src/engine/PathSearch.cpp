// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "PathSearch.h"

#include <boost/graph/detail/adjacency_list.hpp>
#include <memory>
#include <sstream>
#include <variant>

#include "engine/CallFixedSize.h"
#include "engine/PathSearchVisitors.h"
#include "engine/VariableToColumnMap.h"
#include "util/Exception.h"

// _____________________________________________________________________________
PathSearch::PathSearch(QueryExecutionContext* qec,
                       std::shared_ptr<QueryExecutionTree> subtree,
                       PathSearchConfiguration config)
    : Operation(qec),
      subtree_(std::move(subtree)),
      config_(std::move(config)),
      idToIndex_(allocator()) {
  AD_CORRECTNESS_CHECK(qec != nullptr);
  resultWidth_ = 4 + config_.edgeProperties_.size();

  size_t colIndex = 0;

  variableColumns_[config_.start_] = makeAlwaysDefinedColumn(colIndex++);
  variableColumns_[config_.end_] = makeAlwaysDefinedColumn(colIndex++);
  variableColumns_[config_.pathColumn_] = makeAlwaysDefinedColumn(colIndex++);
  variableColumns_[config_.edgeColumn_] = makeAlwaysDefinedColumn(colIndex++);

  if (std::holds_alternative<Variable>(config_.sources_)) {
    resultWidth_++;
    const auto& sourceColumn = std::get<Variable>(config_.sources_);
    variableColumns_[sourceColumn] = makeAlwaysDefinedColumn(colIndex++);
  }

  if (std::holds_alternative<Variable>(config_.targets_)) {
    resultWidth_++;
    const auto& targetColumn = std::get<Variable>(config_.targets_);
    variableColumns_[targetColumn] = makeAlwaysDefinedColumn(colIndex++);
  }

  for (auto edgeProperty : config_.edgeProperties_) {
    variableColumns_[edgeProperty] = makeAlwaysDefinedColumn(colIndex++);
  }
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> PathSearch::getChildren() {
  std::vector<QueryExecutionTree*> res;
  res.push_back(subtree_.get());
  return res;
};

// _____________________________________________________________________________
std::string PathSearch::getCacheKeyImpl() const {
  std::ostringstream os;
  os << config_.toString();

  AD_CORRECTNESS_CHECK(subtree_);
  os << "Subtree:\n" << subtree_->getCacheKey() << '\n';
  return std::move(os).str();
};

// _____________________________________________________________________________
string PathSearch::getDescriptor() const {
  std::ostringstream os;
  os << "PathSearch";
  return std::move(os).str();
};

// _____________________________________________________________________________
size_t PathSearch::getResultWidth() const { return resultWidth_; };

// _____________________________________________________________________________
size_t PathSearch::getCostEstimate() {
  // TODO: Figure out a smart way to estimate cost
  return 1000;
};

// _____________________________________________________________________________
uint64_t PathSearch::getSizeEstimateBeforeLimit() {
  // TODO: Figure out a smart way to estimate size
  return 1000;
};

// _____________________________________________________________________________
float PathSearch::getMultiplicity(size_t col) {
  (void)col;
  return 1;
};

// _____________________________________________________________________________
bool PathSearch::knownEmptyResult() { return subtree_->knownEmptyResult(); };

// _____________________________________________________________________________
vector<ColumnIndex> PathSearch::resultSortedOn() const { return {}; };

// _____________________________________________________________________________
void PathSearch::bindSourceSide(std::shared_ptr<QueryExecutionTree> sourcesOp,
                                size_t inputCol) {
  boundSources_ = {sourcesOp, inputCol};
}

// _____________________________________________________________________________
void PathSearch::bindTargetSide(std::shared_ptr<QueryExecutionTree> targetsOp,
                                size_t inputCol) {
  boundTargets_ = {targetsOp, inputCol};
}

// _____________________________________________________________________________
Result PathSearch::computeResult([[maybe_unused]] bool requestLaziness) {
  std::shared_ptr<const Result> subRes = subtree_->getResult();
  IdTable idTable{allocator()};
  idTable.setNumColumns(getResultWidth());

  const IdTable& dynSub = subRes->idTable();
  if (!dynSub.empty()) {
    auto timer = ad_utility::Timer(ad_utility::Timer::Stopped);
    timer.start();

    std::vector<std::span<const Id>> edgePropertyLists;
    for (const auto& edgeProperty : config_.edgeProperties_) {
      auto edgePropertyIndex = subtree_->getVariableColumn(edgeProperty);
      edgePropertyLists.push_back(dynSub.getColumn(edgePropertyIndex));
    }

    auto subStartColumn = subtree_->getVariableColumn(config_.start_);
    auto subEndColumn = subtree_->getVariableColumn(config_.end_);

    timer.stop();
    auto prepTime = timer.msecs();
    timer.start();

    buildGraph(dynSub.getColumn(subStartColumn), dynSub.getColumn(subEndColumn),
               edgePropertyLists);

    timer.stop();
    auto buildingTime = timer.msecs();
    timer.start();

    std::span<const Id> sources =
        handleSearchSide(config_.sources_, boundSources_);
    std::span<const Id> targets =
        handleSearchSide(config_.targets_, boundTargets_);

    timer.stop();
    auto sideTime = timer.msecs();
    timer.start();

    auto paths = findPaths(sources, targets);


    timer.stop();
    auto searchTime = timer.msecs();
    timer.start();

    CALL_FIXED_SIZE(std::array{getResultWidth()},
                    &PathSearch::pathsToResultTable, this, idTable, paths);

    timer.stop();
    auto fillTime = timer.msecs();
    timer.start();

    auto& info = runtimeInfo();
    info.addDetail("Time to read subcols", prepTime.count());
    info.addDetail("Time to build graph & mapping", buildingTime.count());
    info.addDetail("Time to prepare search sides", sideTime.count());
    info.addDetail("Time to search paths", searchTime.count());
    info.addDetail("Time to fill result table", fillTime.count());
  }

  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
};

// _____________________________________________________________________________
VariableToColumnMap PathSearch::computeVariableToColumnMap() const {
  return variableColumns_;
};

// _____________________________________________________________________________
void PathSearch::buildMapping(std::span<const Id> startNodes,
                              std::span<const Id> endNodes) {
  auto addNode = [this](const Id node) {
    if (!idToIndex_.contains(node)) {
      idToIndex_[node] = indexToId_.size();
      indexToId_.push_back(node);
    }
  };
  for (size_t i = 0; i < startNodes.size(); i++) {
    checkCancellation();
    addNode(startNodes[i]);
    addNode(endNodes[i]);
  }
}

std::span<const Id> PathSearch::handleSearchSide(
    const SearchSide& side, const std::optional<TreeAndCol>& binding) const {
  std::span<const Id> ids;
  bool isVariable = std::holds_alternative<Variable>(side);
  if (isVariable && binding.has_value()) {
    ids = binding->first->getResult()->idTable().getColumn(binding->second);
  } else if (isVariable || std::get<std::vector<Id>>(side).empty()) {
    std::vector<Id> idVec;
    for (auto id : indexToId_) {
      idVec.push_back(id);
    }
    ids = idVec;
  } else {
    ids = std::get<std::vector<Id>>(side);
  }
  return ids;
}

// _____________________________________________________________________________
void PathSearch::buildGraph(std::span<const Id> startNodes,
                            std::span<const Id> endNodes,
                            std::span<std::span<const Id>> edgePropertyLists) {
  AD_CORRECTNESS_CHECK(startNodes.size() == endNodes.size());
  buildMapping(startNodes, endNodes);

  while (boost::num_vertices(graph_) < indexToId_.size()) {
    boost::add_vertex(graph_);
  }

  for (size_t i = 0; i < startNodes.size(); i++) {
    checkCancellation();
    auto startIndex = idToIndex_[startNodes[i]];
    auto endIndex = idToIndex_[endNodes[i]];

    std::vector<Id> edgeProperties;
    for (size_t j = 0; j < edgePropertyLists.size(); j++) {
      edgeProperties.push_back(edgePropertyLists[j][i]);
    }

    Edge edge{startNodes[i].getBits(), endNodes[i].getBits(), edgeProperties};
    boost::add_edge(startIndex, endIndex, edge, graph_);
  }
}

// _____________________________________________________________________________
std::vector<Path> PathSearch::findPaths(std::span<const Id> sources,
                                        std::span<const Id> targets) const {
  switch (config_.algorithm_) {
    case ALL_PATHS:
      return allPaths(sources, targets);
    case SHORTEST_PATHS:
      return shortestPaths(sources, targets);
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
std::vector<Path> PathSearch::allPaths(std::span<const Id> sources,
                                       std::span<const Id> targets) const {
  std::vector<Path> paths;
  Path path;

  for (auto source : sources) {
    auto startIndex = idToIndex_.at(source);

    std::vector<uint64_t> targetIndices;
    for (auto target : targets) {
      targetIndices.push_back(target.getBits());
    }

    PredecessorMap predecessors;

    AllPathsVisitor vis(startIndex, predecessors);
    try {
      boost::depth_first_search(graph_,
                                boost::visitor(vis).root_vertex(startIndex));
    } catch (const StopSearchException& e) {
    }

    for (auto target : targetIndices) {
      auto pathsToTarget =
          reconstructPaths(source.getBits(), target, predecessors);
      for (auto path : pathsToTarget) {
        paths.push_back(std::move(path));
      }
    }
  }
  return paths;
}

// _____________________________________________________________________________
std::vector<Path> PathSearch::shortestPaths(std::span<const Id> sources,
                                            std::span<const Id> targets) const {
  std::vector<Path> paths;
  Path path;
  for (auto source : sources) {
    auto startIndex = idToIndex_.at(source);

    std::unordered_set<uint64_t> targetIndices;
    for (auto target : targets) {
      targetIndices.insert(target.getBits());
    }
    std::vector<VertexDescriptor> predecessors(indexToId_.size());
    std::vector<double> distances(indexToId_.size(),
                                  std::numeric_limits<double>::max());

    DijkstraAllPathsVisitor vis(startIndex, targetIndices, path, paths,
                                predecessors, distances);

    auto weightMap = get(&Edge::weight_, graph_);

    boost::dijkstra_shortest_paths(
        graph_, startIndex,
        boost::visitor(vis)
            .weight_map(weightMap)
            .predecessor_map(predecessors.data())
            .distance_map(distances.data())
            .distance_compare(std::less_equal<double>()));
  }
  return paths;
}

// _____________________________________________________________________________
std::vector<Path> PathSearch::reconstructPaths(
    uint64_t source, uint64_t target, PredecessorMap predecessors) const {
  const auto& edges = predecessors[target];
  std::vector<Path> paths;

  for (const auto& edge : edges) {
    std::vector<Path> subPaths;
    if (edge.start_ == source) {
      subPaths = {Path()};
    } else {
      subPaths = reconstructPaths(source, edge.start_, predecessors);
    }

    for (auto path : subPaths) {
      path.push_back(edge);
      paths.push_back(path);
    }
  }
  return paths;
}

// _____________________________________________________________________________
template <size_t WIDTH>
void PathSearch::pathsToResultTable(IdTable& tableDyn,
                                    std::vector<Path>& paths) const {
  IdTableStatic<WIDTH> table = std::move(tableDyn).toStatic<WIDTH>();

  size_t rowIndex = 0;
  for (size_t pathIndex = 0; pathIndex < paths.size(); pathIndex++) {
    auto path = paths[pathIndex];
    for (size_t edgeIndex = 0; edgeIndex < path.size(); edgeIndex++) {
      auto edge = path.edges_[edgeIndex];
      auto [start, end] = edge.toIds();
      table.emplace_back();
      table(rowIndex, getStartIndex()) = start;
      table(rowIndex, getEndIndex()) = end;
      table(rowIndex, getPathIndex()) = Id::makeFromInt(pathIndex);
      table(rowIndex, getEdgeIndex()) = Id::makeFromInt(edgeIndex);

      for (size_t edgePropertyIndex = 0;
           edgePropertyIndex < edge.edgeProperties_.size();
           edgePropertyIndex++) {
        table(rowIndex, 4 + edgePropertyIndex) =
            edge.edgeProperties_[edgePropertyIndex];
      }

      rowIndex++;
    }
  }

  tableDyn = std::move(table).toDynamic();
}
