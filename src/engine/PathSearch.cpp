// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "PathSearch.h"

#include "engine/CallFixedSize.h"
#include "engine/PathSearchVisitors.h"
#include "engine/QueryExecutionTree.h"
#include "engine/VariableToColumnMap.h"

// _____________________________________________________________________________
BinSearchWrapper::BinSearchWrapper(const IdTable& table, size_t startCol,
                                   size_t endCol, std::vector<size_t> edgeCols)
    : table_(table),
      startCol_(startCol),
      endCol_(endCol),
      edgeCols_(std::move(edgeCols)) {}

// _____________________________________________________________________________
std::vector<Edge> BinSearchWrapper::outgoingEdes(const Id node) const {
  auto startIds = table_.getColumn(startCol_);
  auto range = std::ranges::equal_range(startIds, node);
  auto startIndex = std::distance(startIds.begin(), range.begin());

  std::vector<Edge> edges;
  for (size_t i = 0; i < range.size(); i++) {
    auto row = startIndex + i;
    auto edge = makeEdgeFromRow(row);
    edges.push_back(edge);
  }
  return edges;
}

std::vector<Path> BinSearchWrapper::findPaths(
    const Id& source, const std::unordered_set<uint64_t>& targets) {
  if (pathCache_.contains(source.getBits())) {
    return pathCache_[source.getBits()];
  }
  pathCache_[source.getBits()] = {};
  std::vector<Path> paths;

  auto edges = outgoingEdes(source);
  for (auto edge : edges) {
    if (targets.contains(edge.end_) || targets.empty()) {
      Path path;
      path.push_back(edge);
      paths.push_back(std::move(path));
    }
    auto partialPaths = findPaths(Id::fromBits(edge.end_), targets);
    for (auto path : partialPaths) {
      path.push_back(edge);
      paths.push_back(std::move(path));
    }
  }

  pathCache_[source.getBits()].insert(pathCache_[source.getBits()].end(),
                                      paths.begin(), paths.end());
  return paths;
}

// _____________________________________________________________________________
const Edge BinSearchWrapper::makeEdgeFromRow(size_t row) const {
  Edge edge;
  edge.start_ = table_(row, startCol_).getBits();
  edge.end_ = table_(row, endCol_).getBits();

  for (auto edgeCol : edgeCols_) {
    edge.edgeProperties_.push_back(table_(row, edgeCol));
  }
  return edge;
}

// _____________________________________________________________________________
PathSearch::PathSearch(QueryExecutionContext* qec,
                       std::shared_ptr<QueryExecutionTree> subtree,
                       PathSearchConfiguration config)
    : Operation(qec),
      subtree_(std::move(subtree)),
      config_(std::move(config)),
      idToIndex_(allocator()) {
  AD_CORRECTNESS_CHECK(qec != nullptr);

  auto startCol = subtree_->getVariableColumn(config_.start_);
  auto endCol = subtree_->getVariableColumn(config_.end_);
  subtree_ = QueryExecutionTree::createSortedTree(subtree_, {startCol, endCol});

  resultWidth_ = 4 + config_.edgeProperties_.size();

  size_t colIndex = 0;

  variableColumns_[config_.start_] = makeAlwaysDefinedColumn(colIndex);
  colIndex++;
  variableColumns_[config_.end_] = makeAlwaysDefinedColumn(colIndex);
  colIndex++;
  variableColumns_[config_.pathColumn_] = makeAlwaysDefinedColumn(colIndex);
  colIndex++;
  variableColumns_[config_.edgeColumn_] = makeAlwaysDefinedColumn(colIndex);
  colIndex++;

  if (std::holds_alternative<Variable>(config_.sources_)) {
    resultWidth_++;
    const auto& sourceColumn = std::get<Variable>(config_.sources_);
    variableColumns_[sourceColumn] = makeAlwaysDefinedColumn(colIndex);
    colIndex++;
  }

  if (std::holds_alternative<Variable>(config_.targets_)) {
    resultWidth_++;
    const auto& targetColumn = std::get<Variable>(config_.targets_);
    variableColumns_[targetColumn] = makeAlwaysDefinedColumn(colIndex);
    colIndex++;
  }

  for (const auto& edgeProperty : config_.edgeProperties_) {
    variableColumns_[edgeProperty] = makeAlwaysDefinedColumn(colIndex);
    colIndex++;
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

    if (config_.algorithm_ == PathSearchAlgorithm::SHORTEST_PATHS) {
      std::vector<std::span<const Id>> edgePropertyLists;
      for (const auto& edgeProperty : config_.edgeProperties_) {
        auto edgePropertyIndex = subtree_->getVariableColumn(edgeProperty);
        edgePropertyLists.push_back(dynSub.getColumn(edgePropertyIndex));
      }

      auto subStartColumn = subtree_->getVariableColumn(config_.start_);
      auto subEndColumn = subtree_->getVariableColumn(config_.end_);

      buildGraph(dynSub.getColumn(subStartColumn),
                 dynSub.getColumn(subEndColumn), edgePropertyLists);
    }

    auto subStartColumn = subtree_->getVariableColumn(config_.start_);
    auto subEndColumn = subtree_->getVariableColumn(config_.end_);
    std::vector<size_t> edgeColumns;
    for (auto edgeProp : config_.edgeProperties_) {
      edgeColumns.push_back(subtree_->getVariableColumn(edgeProp));
    }
    BinSearchWrapper binSearch{dynSub, subStartColumn, subEndColumn,
                               std::move(edgeColumns)};

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

    auto paths = findPaths(sources, targets, binSearch);

    timer.stop();
    auto searchTime = timer.msecs();
    timer.start();

    CALL_FIXED_SIZE(std::array{getResultWidth()},
                    &PathSearch::pathsToResultTable, this, idTable, paths);

    timer.stop();
    auto fillTime = timer.msecs();
    timer.start();

    auto& info = runtimeInfo();
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
  } else if (isVariable) {
    return {};
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
                                        std::span<const Id> targets,
                                        BinSearchWrapper& binSearch) const {
  switch (config_.algorithm_) {
    case PathSearchAlgorithm::ALL_PATHS:
      return allPaths(sources, targets, binSearch);
    case PathSearchAlgorithm::SHORTEST_PATHS:
      return shortestPaths(sources, targets);
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
std::vector<Path> PathSearch::allPaths(std::span<const Id> sources,
                                       std::span<const Id> targets,
                                       BinSearchWrapper& binSearch) const {
  std::vector<Path> paths;
  Path path;

  std::unordered_set<uint64_t> targetSet;
  for (auto target : targets) {
    targetSet.insert(target.getBits());
  }

  if (sources.empty()) {
    sources = indexToId_;
  }
  for (auto source : sources) {
    for (auto path : binSearch.findPaths(source, targetSet)) {
      std::ranges::reverse(path.edges_);
      paths.push_back(path);
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
