// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "TransitivePathGraphblas.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/TransitivePathBase.h"
#include "util/Exception.h"
#include "util/Timer.h"

// _____________________________________________________________________________
TransitivePathGraphblas::TransitivePathGraphblas(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist)
    : TransitivePathBase(qec, child, leftSide, rightSide, minDist, maxDist) {}

// _____________________________________________________________________________
template <size_t RES_WIDTH, size_t SUB_WIDTH, size_t SIDE_WIDTH>
void TransitivePathGraphblas::computeTransitivePathBound(
    IdTable* dynRes, const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide, const IdTable& startSideTable) const {
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  decltype(auto) startCol = sub.getColumn(startSide.subCol_);
  decltype(auto) targetCol = sub.getColumn(targetSide.subCol_);

  auto timer = ad_utility::Timer(ad_utility::Timer::Stopped);
  timer.start();

  GrbMatrix::initialize();
  auto [graph, mapping] = setupMatrix(startCol, targetCol, sub.size());

  std::span<const Id> startNodes =
      startSideTable.getColumn(startSide.treeAndCol_->second);
  GrbMatrix startNodeMatrix =
      setupStartNodeMatrix(startNodes, graph.numRows(), mapping);

  timer.stop();
  auto initTime = timer.msecs();
  timer.start();

  auto hull = transitiveHull(graph, std::move(startNodeMatrix));
  if (!targetSide.isVariable()) {
    Id target = std::get<Id>(targetSide.value_);
    size_t targetIndex = mapping.getIndex(target);
    hull = getTargetRow(hull, targetIndex);
  }

  timer.stop();
  auto hullTime = timer.msecs();
  timer.start();

  TransitivePathGraphblas::fillTableWithHull<RES_WIDTH, SIDE_WIDTH>(
      res, hull, mapping, startSideTable, startNodes, startSide.outputCol_,
      targetSide.outputCol_, startSide.treeAndCol_.value().second);

  timer.stop();
  auto fillTime = timer.msecs();

  LOG(DEBUG) << "GraphBLAS Timing measurements:" << std::endl;
  LOG(DEBUG) << "Initialization time: " << std::to_string(initTime.count())
             << "ms" << std::endl;
  LOG(DEBUG) << "Hull computation time: " << std::to_string(hullTime.count())
             << "ms" << std::endl;
  LOG(DEBUG) << "IdTable fill time: " << std::to_string(fillTime.count())
             << "ms" << std::endl;

  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
template <size_t RES_WIDTH, size_t SUB_WIDTH>
void TransitivePathGraphblas::computeTransitivePath(
    IdTable* dynRes, const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  decltype(auto) startCol = sub.getColumn(startSide.subCol_);
  decltype(auto) targetCol = sub.getColumn(targetSide.subCol_);

  auto timer = ad_utility::Timer(ad_utility::Timer::Stopped);
  timer.start();

  GrbMatrix::initialize();
  auto [graph, mapping] = setupMatrix(startCol, targetCol, sub.size());

  timer.stop();
  auto initTime = timer.msecs();
  timer.start();

  GrbMatrix hull;
  if (!startSide.isVariable()) {
    std::vector<Id> startNode{std::get<Id>(startSide.value_)};
    GrbMatrix startMatrix =
        setupStartNodeMatrix(startNode, graph.numRows(), mapping);
    hull = transitiveHull(graph, std::move(startMatrix));
  } else {
    hull = transitiveHull(graph, std::nullopt);
  }

  timer.stop();
  auto hullTime = timer.msecs();
  timer.start();

  if (!targetSide.isVariable()) {
    Id target = std::get<Id>(targetSide.value_);
    size_t targetIndex = mapping.getIndex(target);
    hull = getTargetRow(hull, targetIndex);
  }

  if (!startSide.isVariable()) {
    std::vector<Id> startNode{std::get<Id>(startSide.value_)};
    TransitivePathGraphblas::fillTableWithHull<RES_WIDTH>(
        res, hull, mapping, startNode, startSide.outputCol_,
        targetSide.outputCol_);
  } else {
    TransitivePathGraphblas::fillTableWithHull<RES_WIDTH>(
        res, hull, mapping, startSide.outputCol_, targetSide.outputCol_);
  }

  timer.stop();
  auto fillTime = timer.msecs();

  LOG(DEBUG) << "GraphBLAS Timing measurements:" << std::endl;
  LOG(DEBUG) << "Initialization time: " << std::to_string(initTime.count())
             << "ms" << std::endl;
  LOG(DEBUG) << "Hull computation time: " << std::to_string(hullTime.count())
             << "ms" << std::endl;
  LOG(DEBUG) << "IdTable fill time: " << std::to_string(fillTime.count())
             << "ms" << std::endl;

  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
ResultTable TransitivePathGraphblas::computeResult() {
  if (minDist_ == 0 && !isBoundOrId() && lhs_.isVariable() &&
      rhs_.isVariable()) {
    AD_THROW(
        "This query might have to evalute the empty path, which is currently "
        "not supported");
  }
  shared_ptr<const ResultTable> subRes = subtree_->getResult();

  IdTable idTable{allocator()};

  idTable.setNumColumns(getResultWidth());

  size_t subWidth = subRes->idTable().numColumns();

  auto computeForOneSide = [this, &idTable, subRes, subWidth](
                               auto& boundSide,
                               auto& otherSide) -> ResultTable {
    shared_ptr<const ResultTable> sideRes =
        boundSide.treeAndCol_.value().first->getResult();
    size_t sideWidth = sideRes->idTable().numColumns();

    CALL_FIXED_SIZE((std::array{resultWidth_, subWidth, sideWidth}),
                    &TransitivePathGraphblas::computeTransitivePathBound, this,
                    &idTable, subRes->idTable(), boundSide, otherSide,
                    sideRes->idTable());

    return {std::move(idTable), resultSortedOn(),
            ResultTable::getSharedLocalVocabFromNonEmptyOf(*sideRes, *subRes)};
  };

  if (lhs_.isBoundVariable()) {
    return computeForOneSide(lhs_, rhs_);
  } else if (rhs_.isBoundVariable()) {
    return computeForOneSide(rhs_, lhs_);
    // Right side is an Id
  } else if (!rhs_.isVariable()) {
    CALL_FIXED_SIZE((std::array{resultWidth_, subWidth}),
                    &TransitivePathGraphblas::computeTransitivePath, this,
                    &idTable, subRes->idTable(), rhs_, lhs_);
    // No side is a bound variable, the right side is an unbound variable
    // and the left side is either an unbound Variable or an ID.
  } else {
    CALL_FIXED_SIZE((std::array{resultWidth_, subWidth}),
                    &TransitivePathGraphblas::computeTransitivePath, this,
                    &idTable, subRes->idTable(), lhs_, rhs_);
  }

  // NOTE: The only place, where the input to a transitive path operation is not
  // an index scan (which has an empty local vocabulary by default) is the
  // `LocalVocabTest`. But it doesn't harm to propagate the local vocab here
  // either.
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
GrbMatrix TransitivePathGraphblas::transitiveHull(
    const GrbMatrix& graph, std::optional<GrbMatrix> startNodes) const {
  size_t pathLength = 0;
  GrbMatrix result;

  if (startNodes) {
    result = std::move(startNodes.value());
  } else {
    result = GrbMatrix::diag(graph.numRows());
  }

  if (minDist_ > 0) {
    result = result.multiply(graph);
    pathLength++;
  }

  size_t previousNvals = 0;
  size_t nvals = result.numNonZero();
  while (nvals > previousNvals && pathLength < maxDist_) {
    previousNvals = result.numNonZero();
    // TODO: Check effect of matrix orientation (Row major, Column major) on
    // performance.
    result.accumulateMultiply(graph);
    checkCancellation();
    nvals = result.numNonZero();
    pathLength++;
  }
  return result;
}

// _____________________________________________________________________________
template <size_t WIDTH>
void TransitivePathGraphblas::fillTableWithHull(IdTableStatic<WIDTH>& table,
                                                const GrbMatrix& hull,
                                                const IdMapping& mapping,
                                                size_t startSideCol,
                                                size_t targetSideCol) {
  auto [rowIndices, colIndices] = hull.extractTuples();

  for (size_t i = 0; i < rowIndices.size(); i++) {
    table.emplace_back();
    auto startIndex = rowIndices[i];
    auto targetIndex = colIndices[i];
    Id startId = mapping.getId(startIndex);
    Id targetId = mapping.getId(targetIndex);
    table(i, startSideCol) = startId;
    table(i, targetSideCol) = targetId;
  }
}

// _____________________________________________________________________________
template <size_t WIDTH>
void TransitivePathGraphblas::fillTableWithHull(IdTableStatic<WIDTH>& table,
                                                const GrbMatrix& hull,
                                                const IdMapping& mapping,
                                                std::span<const Id> startNodes,
                                                size_t startSideCol,
                                                size_t targetSideCol) {
  size_t resultRowIndex = 0;
  size_t rowIndex = 0;

  for (auto startNode : startNodes) {
    std::vector<size_t> indices = hull.extractRow(rowIndex);
    for (size_t index : indices) {
      Id targetNode = mapping.getId(index);
      table.emplace_back();
      table(resultRowIndex, startSideCol) = startNode;
      table(resultRowIndex, targetSideCol) = targetNode;
      resultRowIndex++;
    }
    rowIndex++;
  }
}

// _____________________________________________________________________________
template <size_t WIDTH, size_t START_WIDTH>
void TransitivePathGraphblas::fillTableWithHull(
    IdTableStatic<WIDTH>& table, const GrbMatrix& hull,
    const IdMapping& mapping, const IdTable& startSideTable,
    std::span<const Id> startNodes, size_t startSideCol, size_t targetSideCol,
    size_t skipCol) {
  IdTableView<START_WIDTH> startView =
      startSideTable.asStaticView<START_WIDTH>();

  size_t resultRowIndex = 0;
  size_t rowIndex = 0;
  for (auto startNode : startNodes) {
    std::vector<size_t> indices = hull.extractRow(rowIndex);
    for (size_t index : indices) {
      Id targetNode = mapping.getId(index);
      table.emplace_back();
      table(resultRowIndex, startSideCol) = startNode;
      table(resultRowIndex, targetSideCol) = targetNode;

      TransitivePathGraphblas::copyColumns<START_WIDTH, WIDTH>(
          startView, table, rowIndex, resultRowIndex, skipCol);
      resultRowIndex++;
    }
    rowIndex++;
  }
}

// _____________________________________________________________________________
GrbMatrix TransitivePathGraphblas::getTargetRow(GrbMatrix& hull,
                                                size_t targetIndex) const {
  GrbMatrix transformer = GrbMatrix(hull.numCols(), hull.numCols());
  transformer.setElement(targetIndex, targetIndex, true);
  return hull.multiply(transformer);
}

// _____________________________________________________________________________
std::tuple<GrbMatrix, IdMapping> TransitivePathGraphblas::setupMatrix(
    std::span<const Id> startCol, std::span<const Id> targetCol,
    size_t numRows) const {
  std::vector<size_t> rowIndices;
  std::vector<size_t> colIndices;
  IdMapping mapping;

  for (size_t i = 0; i < numRows; i++) {
    auto startId = startCol[i];
    auto targetId = targetCol[i];
    auto startIndex = mapping.addId(startId);
    auto targetIndex = mapping.addId(targetId);

    rowIndices.push_back(startIndex);
    colIndices.push_back(targetIndex);
  }

  auto matrix =
      GrbMatrix::build(rowIndices, colIndices, mapping.size(), mapping.size());
  return {std::move(matrix), std::move(mapping)};
}

// _____________________________________________________________________________
GrbMatrix TransitivePathGraphblas::setupStartNodeMatrix(
    std::span<const Id> startIds, size_t numCols, IdMapping mapping) const {
  // stardIds.size() is the maximum possible number of columns for the
  // startMatrix, but if some start node does not have a link in the graph it
  // will be skipped, resulting in a zero column at the end of the startMatrix
  GrbMatrix startMatrix = GrbMatrix(startIds.size(), numCols);
  size_t rowIndex = 0;
  for (Id id : startIds) {
    if (!mapping.contains(id)) {
      continue;
    }
    size_t colIndex = mapping.getIndex(id);
    startMatrix.setElement(rowIndex, colIndex, true);
    rowIndex++;
  }
  return startMatrix;
}

// _____________________________________________________________________________
template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
void TransitivePathGraphblas::copyColumns(
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
