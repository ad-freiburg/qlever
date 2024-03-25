// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "TransitivePathFallback.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/TransitivePathBase.h"
#include "util/Exception.h"
#include "util/Timer.h"

// _____________________________________________________________________________
TransitivePathFallback::TransitivePathFallback(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    const TransitivePathSide& leftSide, const TransitivePathSide& rightSide,
    size_t minDist, size_t maxDist)
    : TransitivePathBase(qec, child, leftSide, rightSide, minDist, maxDist) {}

// _____________________________________________________________________________
template <size_t RES_WIDTH, size_t SUB_WIDTH, size_t SIDE_WIDTH>
void TransitivePathFallback::computeTransitivePathBound(
    IdTable* dynRes, const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide, const IdTable& startSideTable) const {
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  auto timer = ad_utility::Timer(ad_utility::Timer::Stopped);
  timer.start();

  auto [edges, nodes] = setupMapAndNodes<SUB_WIDTH, SIDE_WIDTH>(
      dynSub, startSide, targetSide, startSideTable);

  timer.stop();
  auto initTime = timer.msecs();
  timer.start();

  Map hull(allocator());
  if (!targetSide.isVariable()) {
    hull = transitiveHull(edges, nodes, std::get<Id>(targetSide.value_));
  } else {
    hull = transitiveHull(edges, nodes, std::nullopt);
  }

  timer.stop();
  auto hullTime = timer.msecs();
  timer.start();

  TransitivePathFallback::fillTableWithHull<RES_WIDTH, SIDE_WIDTH>(
      res, hull, nodes, startSide.outputCol_, targetSide.outputCol_,
      startSideTable, startSide.treeAndCol_.value().second);

  timer.stop();
  auto fillTime = timer.msecs();

  auto& info = runtimeInfo();
  info.addDetail("Initialization time", initTime.count());
  info.addDetail("Hull time", hullTime.count());
  info.addDetail("IdTable fill time", fillTime.count());

  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
template <size_t RES_WIDTH, size_t SUB_WIDTH>
void TransitivePathFallback::computeTransitivePath(
    IdTable* dynRes, const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  auto timer = ad_utility::Timer(ad_utility::Timer::Stopped);
  timer.start();

  auto [edges, nodes] =
      setupMapAndNodes<SUB_WIDTH>(dynSub, startSide, targetSide);

  timer.stop();
  auto initTime = timer.msecs();
  timer.start();

  Map hull{allocator()};
  if (!targetSide.isVariable()) {
    hull = transitiveHull(edges, nodes, std::get<Id>(targetSide.value_));
  } else {
    hull = transitiveHull(edges, nodes, std::nullopt);
  }

  timer.stop();
  auto hullTime = timer.msecs();
  timer.start();

  TransitivePathFallback::fillTableWithHull<RES_WIDTH>(
      res, hull, startSide.outputCol_, targetSide.outputCol_);

  timer.stop();
  auto fillTime = timer.msecs();

  auto& info = runtimeInfo();
  info.addDetail("Initialization time", initTime.count());
  info.addDetail("Hull time", hullTime.count());
  info.addDetail("IdTable fill time", fillTime.count());

  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
std::pair<TransitivePathSide&, TransitivePathSide&>
TransitivePathFallback::decideDirection() {
  if (lhs_.isBoundVariable()) {
    LOG(DEBUG) << "Computing TransitivePath left to right" << std::endl;
    return {lhs_, rhs_};
  } else if (rhs_.isBoundVariable() || !rhs_.isVariable()) {
    LOG(DEBUG) << "Computing TransitivePath right to left" << std::endl;
    return {rhs_, lhs_};
  }
  LOG(DEBUG) << "Computing TransitivePath left to right" << std::endl;
  return {lhs_, rhs_};
}

// _____________________________________________________________________________
ResultTable TransitivePathFallback::computeResult() {
  if (minDist_ == 0 && !isBoundOrId() && lhs_.isVariable() &&
      rhs_.isVariable()) {
    AD_THROW(
        "This query might have to evalute the empty path, which is currently "
        "not supported");
  }
  auto [startSide, targetSide] = decideDirection();
  shared_ptr<const ResultTable> subRes = subtree_->getResult();

  IdTable idTable{allocator()};

  idTable.setNumColumns(getResultWidth());

  size_t subWidth = subRes->idTable().numColumns();

  if (startSide.isBoundVariable()) {
    shared_ptr<const ResultTable> sideRes =
        startSide.treeAndCol_.value().first->getResult();
    size_t sideWidth = sideRes->idTable().numColumns();

    CALL_FIXED_SIZE((std::array{resultWidth_, subWidth, sideWidth}),
                    &TransitivePathFallback::computeTransitivePathBound, this,
                    &idTable, subRes->idTable(), startSide, targetSide,
                    sideRes->idTable());

    return {std::move(idTable), resultSortedOn(),
            ResultTable::getSharedLocalVocabFromNonEmptyOf(*sideRes, *subRes)};
  }
  CALL_FIXED_SIZE((std::array{resultWidth_, subWidth}),
                  &TransitivePathFallback::computeTransitivePath, this,
                  &idTable, subRes->idTable(), startSide, targetSide);

  // NOTE: The only place, where the input to a transitive path operation is not
  // an index scan (which has an empty local vocabulary by default) is the
  // `LocalVocabTest`. But it doesn't harm to propagate the local vocab here
  // either.
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
TransitivePathFallback::Map TransitivePathFallback::transitiveHull(
    const Map& edges, const std::vector<Id>& startNodes,
    std::optional<Id> target) const {
  using MapIt = Map::const_iterator;
  // For every node do a dfs on the graph
  Map hull{allocator()};

  // Stores nodes we already have a path to. This avoids cycles.
  ad_utility::HashSetWithMemoryLimit<Id> marks{
      getExecutionContext()->getAllocator()};

  // The stack used to store the dfs' progress
  std::vector<Set::const_iterator> positions;

  // Used to store all edges leading away from a node for every level.
  // Reduces access to the hashmap, and is safe as the map will not
  // be modified after this point.
  std::vector<const Set*> edgeCache;

  for (Id currentStartNode : startNodes) {
    if (hull.contains(currentStartNode)) {
      // We have already computed the hull for this node
      continue;
    }

    // Reset for this iteration
    marks.clear();

    MapIt rootEdges = edges.find(currentStartNode);
    if (rootEdges != edges.end()) {
      positions.push_back(rootEdges->second.begin());
      edgeCache.push_back(&rootEdges->second);
    }
    if (minDist_ == 0 &&
        (!target.has_value() || currentStartNode == target.value())) {
      insertIntoMap(hull, currentStartNode, currentStartNode);
    }

    // While we have not found the entire transitive hull and have not reached
    // the max step limit
    while (!positions.empty()) {
      checkCancellation();
      size_t stackIndex = positions.size() - 1;
      // Process the next child of the node at the top of the stack
      Set::const_iterator& pos = positions[stackIndex];
      const Set* nodeEdges = edgeCache.back();

      if (pos == nodeEdges->end()) {
        // We finished processing this node
        positions.pop_back();
        edgeCache.pop_back();
        continue;
      }

      Id child = *pos;
      ++pos;
      size_t childDepth = positions.size();
      if (childDepth <= maxDist_ && marks.count(child) == 0) {
        // process the child
        if (childDepth >= minDist_) {
          marks.insert(child);
          if (!target.has_value() || child == target.value()) {
            insertIntoMap(hull, currentStartNode, child);
          }
        }
        // Add the child to the stack
        MapIt it = edges.find(child);
        if (it != edges.end()) {
          positions.push_back(it->second.begin());
          edgeCache.push_back(&it->second);
        }
      }
    }
  }
  return hull;
}

// _____________________________________________________________________________
template <size_t WIDTH, size_t START_WIDTH>
void TransitivePathFallback::fillTableWithHull(
    IdTableStatic<WIDTH>& table, const Map& hull, std::vector<Id>& nodes,
    size_t startSideCol, size_t targetSideCol, const IdTable& startSideTable,
    size_t skipCol) {
  IdTableView<START_WIDTH> startView =
      startSideTable.asStaticView<START_WIDTH>();

  size_t rowIndex = 0;
  for (size_t i = 0; i < nodes.size(); i++) {
    Id node = nodes[i];
    auto it = hull.find(node);
    if (it == hull.end()) {
      continue;
    }

    for (Id otherNode : it->second) {
      table.emplace_back();
      table(rowIndex, startSideCol) = node;
      table(rowIndex, targetSideCol) = otherNode;

      TransitivePathFallback::copyColumns<START_WIDTH, WIDTH>(
          startView, table, i, rowIndex, skipCol);

      rowIndex++;
    }
  }
}

// _____________________________________________________________________________
template <size_t WIDTH>
void TransitivePathFallback::fillTableWithHull(IdTableStatic<WIDTH>& table,
                                               const Map& hull,
                                               size_t startSideCol,
                                               size_t targetSideCol) {
  size_t rowIndex = 0;
  for (auto const& [node, linkedNodes] : hull) {
    for (Id linkedNode : linkedNodes) {
      table.emplace_back();
      table(rowIndex, startSideCol) = node;
      table(rowIndex, targetSideCol) = linkedNode;

      rowIndex++;
    }
  }
}

// _____________________________________________________________________________
template <size_t SUB_WIDTH, size_t SIDE_WIDTH>
std::pair<TransitivePathFallback::Map, std::vector<Id>>
TransitivePathFallback::setupMapAndNodes(const IdTable& sub,
                                         const TransitivePathSide& startSide,
                                         const TransitivePathSide& targetSide,
                                         const IdTable& startSideTable) const {
  std::vector<Id> nodes;
  Map edges = setupEdgesMap<SUB_WIDTH>(sub, startSide, targetSide);

  // Bound -> var|id
  std::span<const Id> startNodes = setupNodes<SIDE_WIDTH>(
      startSideTable, startSide.treeAndCol_.value().second);
  nodes.insert(nodes.end(), startNodes.begin(), startNodes.end());

  return {std::move(edges), std::move(nodes)};
}

// _____________________________________________________________________________
template <size_t SUB_WIDTH>
std::pair<TransitivePathFallback::Map, std::vector<Id>>
TransitivePathFallback::setupMapAndNodes(
    const IdTable& sub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  std::vector<Id> nodes;
  Map edges = setupEdgesMap<SUB_WIDTH>(sub, startSide, targetSide);

  // id -> var|id
  if (!startSide.isVariable()) {
    nodes.push_back(std::get<Id>(startSide.value_));
    // var -> var
  } else {
    std::span<const Id> startNodes =
        setupNodes<SUB_WIDTH>(sub, startSide.subCol_);
    nodes.insert(nodes.end(), startNodes.begin(), startNodes.end());
    if (minDist_ == 0) {
      std::span<const Id> targetNodes =
          setupNodes<SUB_WIDTH>(sub, targetSide.subCol_);
      nodes.insert(nodes.end(), targetNodes.begin(), targetNodes.end());
    }
  }

  return {std::move(edges), std::move(nodes)};
}

// _____________________________________________________________________________
template <size_t SUB_WIDTH>
TransitivePathFallback::Map TransitivePathFallback::setupEdgesMap(
    const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  Map edges{allocator()};
  decltype(auto) startCol = sub.getColumn(startSide.subCol_);
  decltype(auto) targetCol = sub.getColumn(targetSide.subCol_);

  for (size_t i = 0; i < sub.size(); i++) {
    checkCancellation();
    insertIntoMap(edges, startCol[i], targetCol[i]);
  }
  return edges;
}

// _____________________________________________________________________________
template <size_t WIDTH>
std::span<const Id> TransitivePathFallback::setupNodes(const IdTable& table,
                                                       size_t col) {
  return table.getColumn(col);
}

// _____________________________________________________________________________
template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
void TransitivePathFallback::copyColumns(
    const IdTableView<INPUT_WIDTH>& inputTable,
    IdTableStatic<OUTPUT_WIDTH>& outputTable, size_t inputRow, size_t outputRow,
    size_t skipCol) {
  size_t inCol = 0;
  size_t outCol = 2;
  while (inCol < inputTable.numColumns() && outCol < outputTable.numColumns()) {
    if (skipCol == inCol) {
      inCol++;
      continue;
    }

    outputTable(outputRow, outCol) = inputTable(inputRow, inCol);
    inCol++;
    outCol++;
  }
}

// _____________________________________________________________________________
void TransitivePathFallback::insertIntoMap(Map& map, Id key, Id value) const {
  auto [it, success] = map.try_emplace(key, allocator());
  it->second.insert(value);
}
