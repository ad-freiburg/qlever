// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "TransitivePathBinSearch.h"

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/TransitivePathBase.h"
#include "util/Exception.h"
#include "util/Timer.h"

// _____________________________________________________________________________
TransitivePathBinSearch::TransitivePathBinSearch(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    const TransitivePathSide& leftSide, const TransitivePathSide& rightSide,
    size_t minDist, size_t maxDist)
    : TransitivePathBase(qec, child, leftSide, rightSide, minDist, maxDist) {
  auto [startSide, targetSide] = decideDirection();
  subtree_ = QueryExecutionTree::createSortedTree(
      subtree_, {startSide.subCol_, targetSide.subCol_});
}

// _____________________________________________________________________________
template <size_t RES_WIDTH, size_t SUB_WIDTH, size_t SIDE_WIDTH>
void TransitivePathBinSearch::computeTransitivePathBound(
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

  TransitivePathBinSearch::fillTableWithHull<RES_WIDTH, SIDE_WIDTH>(
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
void TransitivePathBinSearch::computeTransitivePath(
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

  TransitivePathBinSearch::fillTableWithHull<RES_WIDTH>(
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
TransitivePathBinSearch::decideDirection() {
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
ResultTable TransitivePathBinSearch::computeResult() {
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
                    &TransitivePathBinSearch::computeTransitivePathBound, this,
                    &idTable, subRes->idTable(), startSide, targetSide,
                    sideRes->idTable());

    return {std::move(idTable), resultSortedOn(),
            ResultTable::getSharedLocalVocabFromNonEmptyOf(*sideRes, *subRes)};
  }
  CALL_FIXED_SIZE((std::array{resultWidth_, subWidth}),
                  &TransitivePathBinSearch::computeTransitivePath, this,
                  &idTable, subRes->idTable(), startSide, targetSide);

  // NOTE: The only place, where the input to a transitive path operation is not
  // an index scan (which has an empty local vocabulary by default) is the
  // `LocalVocabTest`. But it doesn't harm to propagate the local vocab here
  // either.
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
TransitivePathBinSearch::Map TransitivePathBinSearch::transitiveHull(
    const BinSearchMap& edges, const std::vector<Id>& startNodes,
    std::optional<Id> target) const {
  // For every node do a dfs on the graph
  Map hull{allocator()};

  std::vector<std::pair<Id, size_t>> stack;
  ad_utility::HashSetWithMemoryLimit<Id> marks{
      getExecutionContext()->getAllocator()};
  for (auto startNode : startNodes) {
    if (hull.contains(startNode)) {
      // We have already computed the hull for this node
      continue;
    }

    marks.clear();
    stack.clear();
    stack.push_back({startNode, 0});

    if (minDist_ == 0 && (!target.has_value() || startNode == target.value())) {
      insertIntoMap(hull, startNode, startNode);
    }

    while (stack.size() > 0) {
      checkCancellation();
      auto [node, steps] = stack.back();
      stack.pop_back();

      if (steps <= maxDist_ && marks.count(node) == 0) {
        if (steps >= minDist_) {
          marks.insert(node);
          if (!target.has_value() || node == target.value()) {
            insertIntoMap(hull, startNode, node);
          }
        }

        auto successors = edges.successors(node);
        for (auto successor : successors) {
          stack.push_back({successor, steps + 1});
        }
      }
    }
  }
  return hull;
}

// _____________________________________________________________________________
template <size_t WIDTH, size_t START_WIDTH>
void TransitivePathBinSearch::fillTableWithHull(
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

      TransitivePathBinSearch::copyColumns<START_WIDTH, WIDTH>(
          startView, table, i, rowIndex, skipCol);

      rowIndex++;
    }
  }
}

// _____________________________________________________________________________
template <size_t WIDTH>
void TransitivePathBinSearch::fillTableWithHull(IdTableStatic<WIDTH>& table,
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
std::pair<BinSearchMap, std::vector<Id>>
TransitivePathBinSearch::setupMapAndNodes(const IdTable& sub,
                                          const TransitivePathSide& startSide,
                                          const TransitivePathSide& targetSide,
                                          const IdTable& startSideTable) const {
  std::vector<Id> nodes;
  auto edges = setupEdgesMap<SUB_WIDTH>(sub, startSide, targetSide);

  // Bound -> var|id
  std::span<const Id> startNodes = setupNodes<SIDE_WIDTH>(
      startSideTable, startSide.treeAndCol_.value().second);
  nodes.insert(nodes.end(), startNodes.begin(), startNodes.end());

  return {std::move(edges), std::move(nodes)};
}

// _____________________________________________________________________________
template <size_t SUB_WIDTH>
std::pair<BinSearchMap, std::vector<Id>>
TransitivePathBinSearch::setupMapAndNodes(
    const IdTable& sub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  std::vector<Id> nodes;
  auto edges = setupEdgesMap<SUB_WIDTH>(sub, startSide, targetSide);

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
BinSearchMap TransitivePathBinSearch::setupEdgesMap(
    const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  decltype(auto) startCol = sub.getColumn(startSide.subCol_);
  decltype(auto) targetCol = sub.getColumn(targetSide.subCol_);
  BinSearchMap edges{startCol, targetCol};

  return edges;
}

// _____________________________________________________________________________
template <size_t WIDTH>
std::span<const Id> TransitivePathBinSearch::setupNodes(const IdTable& table,
                                                        size_t col) {
  return table.getColumn(col);
}

// _____________________________________________________________________________
template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
void TransitivePathBinSearch::copyColumns(
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
void TransitivePathBinSearch::insertIntoMap(Map& map, Id key, Id value) const {
  auto [it, success] = map.try_emplace(key, allocator());
  it->second.insert(value);
}
