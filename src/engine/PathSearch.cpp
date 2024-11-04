// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "PathSearch.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <variant>
#include <vector>

#include "engine/CallFixedSize.h"
#include "engine/QueryExecutionTree.h"
#include "engine/VariableToColumnMap.h"
#include "util/AllocatorWithLimit.h"

using namespace pathSearch;

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

// _____________________________________________________________________________
std::vector<Id> BinSearchWrapper::getSources() const {
  auto startIds = table_.getColumn(startCol_);
  std::vector<Id> sources;
  std::ranges::unique_copy(startIds, std::back_inserter(sources));

  return sources;
}

// _____________________________________________________________________________
std::vector<Id> BinSearchWrapper::getEdgeProperties(const Edge& edge) const {
  std::vector<Id> edgeProperties;
  for (auto edgeCol : edgeCols_) {
    edgeProperties.push_back(table_(edge.edgeRow_, edgeCol));
  }
  return edgeProperties;
}

// _____________________________________________________________________________
Edge BinSearchWrapper::makeEdgeFromRow(size_t row) const {
  Edge edge;
  edge.start_ = table_(row, startCol_);
  edge.end_ = table_(row, endCol_);
  edge.edgeRow_ = row;

  return edge;
}

// _____________________________________________________________________________
PathSearch::PathSearch(QueryExecutionContext* qec,
                       std::shared_ptr<QueryExecutionTree> subtree,
                       PathSearchConfiguration config)
    : Operation(qec), subtree_(std::move(subtree)), config_(std::move(config)) {
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
    auto subVarCols = subtree_->getVariableColumns();
    auto colInfo = subVarCols[edgeProperty];
    variableColumns_[edgeProperty] = {colIndex, colInfo.mightContainUndef_};
    colIndex++;
  }
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> PathSearch::getChildren() {
  std::vector<QueryExecutionTree*> res;
  res.push_back(subtree_.get());

  if (sourceAndTargetTree_.has_value()) {
    res.push_back(sourceAndTargetTree_.value().get());
  } else {
    if (sourceTree_.has_value()) {
      res.push_back(sourceTree_.value().get());
    }

    if (targetTree_.has_value()) {
      res.push_back(targetTree_.value().get());
    }
  }

  return res;
};

// _____________________________________________________________________________
std::string PathSearch::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "PathSearch:\n";
  os << config_.toString();

  AD_CORRECTNESS_CHECK(subtree_);
  os << "Subtree:\n" << subtree_->getCacheKey() << '\n';

  if (sourceTree_.has_value()) {
    os << "Source Side subtree:\n"
       << sourceTree_.value()->getCacheKey() << '\n';
  }

  if (targetTree_.has_value()) {
    os << "Target Side subtree:\n"
       << targetTree_.value()->getCacheKey() << '\n';
  }

  if (sourceAndTargetTree_.has_value()) {
    os << "Source And Target Side subtree:\n"
       << sourceAndTargetTree_.value()->getCacheKey() << '\n';
  }

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
bool PathSearch::knownEmptyResult() {
  for (auto child : getChildren()) {
    if (child->knownEmptyResult()) {
      return true;
    }
  }
  return false;
};

// _____________________________________________________________________________
vector<ColumnIndex> PathSearch::resultSortedOn() const { return {}; };

// _____________________________________________________________________________
void PathSearch::bindSourceSide(std::shared_ptr<QueryExecutionTree> sourcesOp,
                                size_t inputCol) {
  sourceTree_ = sourcesOp;
  sourceCol_ = inputCol;
}

// _____________________________________________________________________________
void PathSearch::bindTargetSide(std::shared_ptr<QueryExecutionTree> targetsOp,
                                size_t inputCol) {
  targetTree_ = targetsOp;
  targetCol_ = inputCol;
}

// _____________________________________________________________________________
void PathSearch::bindSourceAndTargetSide(
    std::shared_ptr<QueryExecutionTree> sourceAndTargetOp, size_t sourceCol,
    size_t targetCol) {
  sourceAndTargetTree_ = sourceAndTargetOp;
  sourceCol_ = sourceCol;
  targetCol_ = targetCol;
}

// _____________________________________________________________________________
Result PathSearch::computeResult([[maybe_unused]] bool requestLaziness) {
  std::shared_ptr<const Result> subRes = subtree_->getResult();
  IdTable idTable{allocator()};
  idTable.setNumColumns(getResultWidth());

  const IdTable& dynSub = subRes->idTable();
  if (!dynSub.empty()) {
    auto timer = ad_utility::Timer(ad_utility::Timer::Started);

    auto subStartColumn = subtree_->getVariableColumn(config_.start_);
    auto subEndColumn = subtree_->getVariableColumn(config_.end_);
    std::vector<size_t> edgeColumns;
    for (const auto& edgeProp : config_.edgeProperties_) {
      edgeColumns.push_back(subtree_->getVariableColumn(edgeProp));
    }
    BinSearchWrapper binSearch{dynSub, subStartColumn, subEndColumn,
                               std::move(edgeColumns)};

    timer.stop();
    auto buildingTime = timer.msecs();
    timer.start();

    auto [sources, targets] = handleSearchSides();

    timer.stop();
    auto sideTime = timer.msecs();
    timer.start();

    PathsLimited paths{allocator()};
    std::vector<Id> allSources;
    if (sources.empty()) {
      allSources = binSearch.getSources();
      sources = allSources;
    }
    paths = allPaths(sources, targets, binSearch, config_.cartesian_,
                     config_.numPathsPerTarget_);

    timer.stop();
    auto searchTime = timer.msecs();
    timer.start();

    CALL_FIXED_SIZE(std::array{getResultWidth()},
                    &PathSearch::pathsToResultTable, this, idTable, paths,
                    binSearch);

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
std::pair<std::span<const Id>, std::span<const Id>>
PathSearch::handleSearchSides() const {
  std::span<const Id> sourceIds;
  std::span<const Id> targetIds;

  if (sourceAndTargetTree_.has_value()) {
    auto resultTable = sourceAndTargetTree_.value()->getResult();
    sourceIds = resultTable->idTable().getColumn(sourceCol_.value());
    targetIds = resultTable->idTable().getColumn(targetCol_.value());
    return {sourceIds, targetIds};
  }

  if (sourceTree_.has_value()) {
    sourceIds = sourceTree_.value()->getResult()->idTable().getColumn(
        sourceCol_.value());
  } else if (config_.sourceIsVariable()) {
    sourceIds = {};
  } else {
    sourceIds = std::get<std::vector<Id>>(config_.sources_);
  }

  if (targetTree_.has_value()) {
    targetIds = targetTree_.value()->getResult()->idTable().getColumn(
        targetCol_.value());
  } else if (config_.targetIsVariable()) {
    targetIds = {};
  } else {
    targetIds = std::get<std::vector<Id>>(config_.targets_);
  }

  return {sourceIds, targetIds};
}

// _____________________________________________________________________________
PathsLimited PathSearch::findPaths(
    const Id& source, const std::unordered_set<uint64_t>& targets,
    const BinSearchWrapper& binSearch,
    std::optional<uint64_t> numPathsPerTarget) const {
  std::vector<Edge> edgeStack;
  Path currentPath{EdgesLimited(allocator())};
  std::unordered_map<
      uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>,
      ad_utility::AllocatorWithLimit<std::pair<const uint64_t, uint64_t>>>
      numPathsPerNode{allocator()};
  PathsLimited result{allocator()};
  std::unordered_set<uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>,
                     ad_utility::AllocatorWithLimit<uint64_t>>
      visited{allocator()};

  visited.insert(source.getBits());
  for (auto edge : binSearch.outgoingEdes(source)) {
    edgeStack.push_back(std::move(edge));
  }

  while (!edgeStack.empty()) {
    checkCancellation();
    auto edge = edgeStack.back();
    edgeStack.pop_back();

    visited.insert(edge.end_.getBits());

    while (!currentPath.empty() && edge.start_ != currentPath.end()) {
      visited.erase(currentPath.end().getBits());
      currentPath.pop_back();
    }

    auto edgeEnd = edge.end_.getBits();
    if (numPathsPerTarget) {
      auto numPaths = ++numPathsPerNode[edgeEnd];

      if (numPaths > numPathsPerTarget) {
        continue;
      }
    }

    currentPath.push_back(edge);

    if (targets.empty() || targets.contains(edgeEnd)) {
      result.push_back(currentPath);
    }

    for (const auto& outgoingEdge : binSearch.outgoingEdes(edge.end_)) {
      if (!visited.contains(outgoingEdge.end_.getBits())) {
        edgeStack.push_back(outgoingEdge);
      }
    }
  }

  return result;
}

// _____________________________________________________________________________
PathsLimited PathSearch::allPaths(
    std::span<const Id> sources, std::span<const Id> targets,
    const BinSearchWrapper& binSearch, bool cartesian,
    std::optional<uint64_t> numPathsPerTarget) const {
  PathsLimited paths{allocator()};
  Path path{EdgesLimited(allocator())};

  if (cartesian || sources.size() != targets.size()) {
    std::unordered_set<uint64_t> targetSet;
    for (auto target : targets) {
      targetSet.insert(target.getBits());
    }
    for (auto source : sources) {
      for (const auto& path :
           findPaths(source, targetSet, binSearch, numPathsPerTarget)) {
        paths.push_back(path);
      }
    }
  } else {
    for (size_t i = 0; i < sources.size(); i++) {
      for (const auto& path : findPaths(sources[i], {targets[i].getBits()},
                                        binSearch, numPathsPerTarget)) {
        paths.push_back(path);
      }
    }
  }
  return paths;
}

// _____________________________________________________________________________
template <size_t WIDTH>
void PathSearch::pathsToResultTable(IdTable& tableDyn, PathsLimited& paths,
                                    const BinSearchWrapper& binSearch) const {
  IdTableStatic<WIDTH> table = std::move(tableDyn).toStatic<WIDTH>();

  std::vector<size_t> edgePropertyCols;
  for (const auto& edgeVar : config_.edgeProperties_) {
    auto edgePropertyCol = variableColumns_.at(edgeVar).columnIndex_;
    edgePropertyCols.push_back(edgePropertyCol);
  }

  size_t rowIndex = 0;
  for (size_t pathIndex = 0; pathIndex < paths.size(); pathIndex++) {
    auto path = paths[pathIndex];

    std::optional<Id> sourceId = std::nullopt;
    if (config_.sourceIsVariable()) {
      sourceId = path.edges_.front().start_;
    }

    std::optional<Id> targetId = std::nullopt;
    if (config_.targetIsVariable()) {
      targetId = path.edges_.back().end_;
    }

    for (size_t edgeIndex = 0; edgeIndex < path.size(); edgeIndex++) {
      checkCancellation();
      auto edge = path.edges_[edgeIndex];
      table.emplace_back();
      table(rowIndex, getStartIndex()) = edge.start_;
      table(rowIndex, getEndIndex()) = edge.end_;
      table(rowIndex, getPathIndex()) = Id::makeFromInt(pathIndex);
      table(rowIndex, getEdgeIndex()) = Id::makeFromInt(edgeIndex);

      if (sourceId) {
        table(rowIndex, getSourceIndex().value()) = sourceId.value();
      }

      if (targetId) {
        table(rowIndex, getTargetIndex().value()) = targetId.value();
      }

      auto edgeProperties = binSearch.getEdgeProperties(edge);
      for (size_t edgePropertyIndex = 0;
           edgePropertyIndex < edgeProperties.size(); edgePropertyIndex++) {
        table(rowIndex, edgePropertyCols[edgePropertyIndex]) =
            edgeProperties[edgePropertyIndex];
      }

      rowIndex++;
    }
  }

  tableDyn = std::move(table).toDynamic();
}
