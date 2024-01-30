// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "TransitivePath.h"

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "util/Exception.h"

// _____________________________________________________________________________
TransitivePath::TransitivePath(QueryExecutionContext* qec,
                               std::shared_ptr<QueryExecutionTree> child,
                               TransitivePathSide leftSide,
                               TransitivePathSide rightSide, size_t minDist,
                               size_t maxDist)
    : Operation(qec),
      subtree_(child
                   ? QueryExecutionTree::createSortedTree(std::move(child), {0})
                   : nullptr),
      lhs_(std::move(leftSide)),
      rhs_(std::move(rightSide)),
      minDist_(minDist),
      maxDist_(maxDist) {
  AD_CORRECTNESS_CHECK(qec != nullptr);
  if (lhs_.isVariable()) {
    variableColumns_[std::get<Variable>(lhs_.value_)] =
        makeAlwaysDefinedColumn(0);
  }
  if (rhs_.isVariable()) {
    variableColumns_[std::get<Variable>(rhs_.value_)] =
        makeAlwaysDefinedColumn(1);
  }

  lhs_.outputCol_ = 0;
  rhs_.outputCol_ = 1;
}

// _____________________________________________________________________________
std::string TransitivePath::getCacheKeyImpl() const {
  std::ostringstream os;
  os << " minDist " << minDist_ << " maxDist " << maxDist_ << "\n";

  os << "Left side:\n";
  os << lhs_.getCacheKey();

  os << "Right side:\n";
  os << rhs_.getCacheKey();

  AD_CORRECTNESS_CHECK(subtree_);
  os << "Subtree:\n" << subtree_->getCacheKey() << '\n';

  return std::move(os).str();
}

// _____________________________________________________________________________
std::string TransitivePath::getDescriptor() const {
  std::ostringstream os;
  os << "TransitivePath ";
  // If not full transitive hull, show interval as [min, max].
  if (minDist_ > 1 || maxDist_ < std::numeric_limits<size_t>::max()) {
    os << "[" << minDist_ << ", " << maxDist_ << "] ";
  }
  auto getName = [this](ValueId id) {
    auto optStringAndType =
        ExportQueryExecutionTrees::idToStringAndType(getIndex(), id, {});
    if (optStringAndType.has_value()) {
      return optStringAndType.value().first;
    } else {
      return absl::StrCat("#", id.getBits());
    }
  };
  // Left variable or entity name.
  if (lhs_.isVariable()) {
    os << std::get<Variable>(lhs_.value_).name();
  } else {
    os << getName(std::get<Id>(lhs_.value_));
  }
  // The predicate.
  auto scanOperation =
      std::dynamic_pointer_cast<IndexScan>(subtree_->getRootOperation());
  if (scanOperation != nullptr) {
    os << " " << scanOperation->getPredicate() << " ";
  } else {
    // Escaped the question marks to avoid a warning about ignored trigraphs.
    os << R"( <???> )";
  }
  // Right variable or entity name.
  if (rhs_.isVariable()) {
    os << std::get<Variable>(rhs_.value_).name();
  } else {
    os << getName(std::get<Id>(rhs_.value_));
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t TransitivePath::getResultWidth() const { return resultWidth_; }

// _____________________________________________________________________________
vector<ColumnIndex> TransitivePath::resultSortedOn() const {
  if (lhs_.isSortedOnInputCol()) {
    return {0};
  }
  if (rhs_.isSortedOnInputCol()) {
    return {1};
  }

  return {};
}

// _____________________________________________________________________________
VariableToColumnMap TransitivePath::computeVariableToColumnMap() const {
  return variableColumns_;
}

// _____________________________________________________________________________
void TransitivePath::setTextLimit(size_t limit) {
  for (auto child : getChildren()) {
    child->setTextLimit(limit);
  }
}

// _____________________________________________________________________________
bool TransitivePath::knownEmptyResult() { return subtree_->knownEmptyResult(); }

// _____________________________________________________________________________
float TransitivePath::getMultiplicity(size_t col) {
  (void)col;
  // The multiplicities are not known.
  return 1;
}

// _____________________________________________________________________________
uint64_t TransitivePath::getSizeEstimateBeforeLimit() {
  if (std::holds_alternative<Id>(lhs_.value_) ||
      std::holds_alternative<Id>(rhs_.value_)) {
    // If the subject or object is fixed, assume that the number of matching
    // triples is 1000. This will usually be an overestimate, but it will do the
    // job of avoiding query plans that first generate large intermediate
    // results and only then merge them with a triple such as this. In the
    // lhs_.isVar && rhs_.isVar case below, we assume a worst-case blowup of
    // 10000; see the comment there.
    return 1000;
  }
  if (lhs_.treeAndCol_.has_value()) {
    return lhs_.treeAndCol_.value().first->getSizeEstimate();
  }
  if (rhs_.treeAndCol_.has_value()) {
    return rhs_.treeAndCol_.value().first->getSizeEstimate();
  }
  // Set costs to something very large, so that we never compute the complete
  // transitive hull (unless the variables on both sides are not bound in any
  // other way, so that the only possible query plan is to compute the complete
  // transitive hull).
  //
  // NOTE: _subtree->getSizeEstimateBeforeLimit() is the number of triples of
  // the predicate, for which the transitive hull operator (+) is specified. On
  // Wikidata, the predicate with the largest blowup when taking the
  // transitive hull is wdt:P2789 (connects with). The blowup is then from 90K
  // (without +) to 110M (with +), so about 1000 times larger.
  AD_CORRECTNESS_CHECK(lhs_.isVariable() && rhs_.isVariable());
  return subtree_->getSizeEstimate() * 10000;
}

// _____________________________________________________________________________
size_t TransitivePath::getCostEstimate() {
  // We assume that the cost of computing the transitive path is proportional to
  // the result size.
  auto costEstimate = getSizeEstimateBeforeLimit();
  // Add the cost for the index scan of the predicate involved.
  for (auto* ptr : getChildren()) {
    if (ptr) {
      costEstimate += ptr->getCostEstimate();
    }
  }
  return costEstimate;
}

// _____________________________________________________________________________
template <size_t RES_WIDTH, size_t SUB_WIDTH, size_t SIDE_WIDTH>
void TransitivePath::computeTransitivePathBound(
    IdTable* dynRes, const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide, const IdTable& startSideTable) const {
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  decltype(auto) startCol = sub.getColumn(startSide.subCol_);
  decltype(auto) targetCol = sub.getColumn(targetSide.subCol_);

  GrbMatrix::initialize();
  auto [graph, mapping] = setupMatrix(startCol, targetCol, sub.size());

  std::span<const Id> startNodes =
      startSideTable.getColumn(startSide.treeAndCol_->second);
  GrbMatrix startNodeMatrix =
      setupStartNodeMatrix(startNodes, graph.nrows(), mapping);

  auto hull = std::make_unique<GrbMatrix>(
      transitiveHull(graph, std::make_optional(std::move(startNodeMatrix))));
  if (!targetSide.isVariable()) {
    Id target = std::get<Id>(targetSide.value_);
    size_t targetIndex = mapping.getIndex(target);
    hull = std::make_unique<GrbMatrix>(getTargetRow(*hull, targetIndex));
  }

  TransitivePath::fillTableWithHull<RES_WIDTH, SIDE_WIDTH>(
      res, *hull, mapping, startSideTable, startNodes, startSide.outputCol_,
      targetSide.outputCol_, startSide.treeAndCol_.value().second);
  GrbMatrix::finalize();

  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
template <size_t RES_WIDTH, size_t SUB_WIDTH>
void TransitivePath::computeTransitivePath(
    IdTable* dynRes, const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  decltype(auto) startCol = sub.getColumn(startSide.subCol_);
  decltype(auto) targetCol = sub.getColumn(targetSide.subCol_);

  GrbMatrix::initialize();
  auto [graph, mapping] = setupMatrix(startCol, targetCol, sub.size());

  std::unique_ptr<GrbMatrix> hull;
  if (!startSide.isVariable()) {
    const Id startNode[]{std::get<Id>(startSide.value_)};
    GrbMatrix startMatrix =
        setupStartNodeMatrix(startNode, graph.nrows(), mapping);
    hull = std::make_unique<GrbMatrix>(
        transitiveHull(graph, std::make_optional(std::move(startMatrix))));
  } else {
    hull = std::make_unique<GrbMatrix>(transitiveHull(graph, std::nullopt));
  }

  if (!targetSide.isVariable()) {
    Id target = std::get<Id>(targetSide.value_);
    size_t targetIndex = mapping.getIndex(target);
    hull = std::make_unique<GrbMatrix>(getTargetRow(*hull, targetIndex));
  }

  if (!startSide.isVariable()) {
    const Id startNode[]{std::get<Id>(startSide.value_)};
    TransitivePath::fillTableWithHull<RES_WIDTH>(res, *hull, mapping, startNode,
                                                 startSide.outputCol_,
                                                 targetSide.outputCol_);
  } else {
    TransitivePath::fillTableWithHull<RES_WIDTH>(
        res, *hull, mapping, startSide.outputCol_, targetSide.outputCol_);
  }

  GrbMatrix::finalize();
  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
ResultTable TransitivePath::computeResult() {
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
                    &TransitivePath::computeTransitivePathBound, this, &idTable,
                    subRes->idTable(), boundSide, otherSide,
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
                    &TransitivePath::computeTransitivePath, this, &idTable,
                    subRes->idTable(), rhs_, lhs_);
    // No side is a bound variable, the right side is an unbound variable
    // and the left side is either an unbound Variable or an ID.
  } else {
    CALL_FIXED_SIZE((std::array{resultWidth_, subWidth}),
                    &TransitivePath::computeTransitivePath, this, &idTable,
                    subRes->idTable(), lhs_, rhs_);
  }

  // NOTE: The only place, where the input to a transitive path operation is not
  // an index scan (which has an empty local vocabulary by default) is the
  // `LocalVocabTest`. But it doesn't harm to propagate the local vocab here
  // either.
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePath> TransitivePath::bindLeftSide(
    std::shared_ptr<QueryExecutionTree> leftop, size_t inputCol) const {
  return bindLeftOrRightSide(std::move(leftop), inputCol, true);
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePath> TransitivePath::bindRightSide(
    std::shared_ptr<QueryExecutionTree> rightop, size_t inputCol) const {
  return bindLeftOrRightSide(std::move(rightop), inputCol, false);
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePath> TransitivePath::bindLeftOrRightSide(
    std::shared_ptr<QueryExecutionTree> leftOrRightOp, size_t inputCol,
    bool isLeft) const {
  // Enforce required sorting of `leftOrRightOp`.
  leftOrRightOp = QueryExecutionTree::createSortedTree(std::move(leftOrRightOp),
                                                       {inputCol});
  // Create a copy of this.
  //
  // NOTE: The RHS used to be `std::make_shared<TransitivePath>()`, which is
  // wrong because it first calls the copy constructor of the base class
  // `Operation`, which  would then ignore the changes in `variableColumnMap_`
  // made below (see `Operation::getInternallyVisibleVariableColumns` and
  // `Operation::getExternallyVariableColumns`).
  std::shared_ptr<TransitivePath> p = std::make_shared<TransitivePath>(
      getExecutionContext(), subtree_, lhs_, rhs_, minDist_, maxDist_);
  if (isLeft) {
    p->lhs_.treeAndCol_ = {leftOrRightOp, inputCol};
  } else {
    p->rhs_.treeAndCol_ = {leftOrRightOp, inputCol};
  }

  // Note: The `variable` in the following structured binding is `const`, even
  // if we bind by value. We deliberately make one unnecessary copy of the
  // `variable` to keep the code simpler.
  for (auto [variable, columnIndexWithType] :
       leftOrRightOp->getVariableColumns()) {
    ColumnIndex columnIndex = columnIndexWithType.columnIndex_;
    if (columnIndex == inputCol) {
      continue;
    }

    columnIndexWithType.columnIndex_ += columnIndex > inputCol ? 1 : 2;

    p->variableColumns_[variable] = columnIndexWithType;
    p->resultWidth_++;
  }
  return p;
}

// _____________________________________________________________________________
bool TransitivePath::isBoundOrId() const {
  return lhs_.isBoundVariable() || rhs_.isBoundVariable() ||
         !lhs_.isVariable() || !rhs_.isVariable();
}

// _____________________________________________________________________________
GrbMatrix TransitivePath::transitiveHull(
    const GrbMatrix& graph, std::optional<GrbMatrix> startNodes) const {
  size_t pathLength = 0;
  std::unique_ptr<GrbMatrix> result;

  if (startNodes) {
    result = std::make_unique<GrbMatrix>(std::move(startNodes.value()));
  } else {
    result = std::make_unique<GrbMatrix>(GrbMatrix::diag(graph.nrows()));
  }

  if (minDist_ > 0) {
    result = std::make_unique<GrbMatrix>(result->multiply(graph));
    pathLength++;
  }

  size_t previousNvals = 0;
  size_t nvals = result->nvals();
  while (nvals > previousNvals && pathLength < maxDist_) {
    previousNvals = result->nvals();
    result->accumulateMultiply(graph);
    nvals = result->nvals();
    pathLength++;
  }
  return std::move(*result);
}

// _____________________________________________________________________________
template <size_t WIDTH>
void TransitivePath::fillTableWithHull(IdTableStatic<WIDTH>& table,
                                       const GrbMatrix& hull,
                                       const IdMapping& mapping,
                                       size_t startSideCol,
                                       size_t targetSideCol) {
  std::vector<std::pair<size_t, size_t>> pairs = hull.extractTuples();
  for (size_t i = 0; i < pairs.size(); i++) {
    table.emplace_back();
    auto [startIndex, targetIndex] = pairs[i];
    Id startId = mapping.getId(startIndex);
    Id targetId = mapping.getId(targetIndex);
    table(i, startSideCol) = startId;
    table(i, targetSideCol) = targetId;
  }
}

// _____________________________________________________________________________
template <size_t WIDTH>
void TransitivePath::fillTableWithHull(IdTableStatic<WIDTH>& table,
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
void TransitivePath::fillTableWithHull(IdTableStatic<WIDTH>& table,
                                       const GrbMatrix& hull,
                                       const IdMapping& mapping,
                                       const IdTable& startSideTable,
                                       std::span<const Id> startNodes,
                                       size_t startSideCol,
                                       size_t targetSideCol, size_t skipCol) {
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

      TransitivePath::copyColumns<START_WIDTH, WIDTH>(
          startView, table, rowIndex, resultRowIndex, skipCol);
      resultRowIndex++;
    }
    rowIndex++;
  }
}

// _____________________________________________________________________________
GrbMatrix TransitivePath::getTargetRow(GrbMatrix& hull,
                                       size_t targetIndex) const {
  GrbMatrix transformer = GrbMatrix(hull.ncols(), hull.ncols());
  transformer.setElement(targetIndex, targetIndex, true);
  return hull.multiply(transformer);
}

// _____________________________________________________________________________
std::tuple<GrbMatrix, IdMapping> TransitivePath::setupMatrix(
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
GrbMatrix TransitivePath::setupStartNodeMatrix(std::span<const Id> startIds,
                                               size_t numCols,
                                               IdMapping mapping) const {
  // stardIds.size() is the maximum possible number of columns for the
  // startMatrix, but if some start node does not have a link in the graph it
  // will be skipped, resulting in a zero column at the end of the startMatrix
  GrbMatrix startMatrix = GrbMatrix(startIds.size(), numCols);
  size_t rowIndex = 0;
  for (Id id : startIds) {
    if (!mapping.isContained(id)) {
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
void TransitivePath::copyColumns(const IdTableView<INPUT_WIDTH>& inputTable,
                                 IdTableStatic<OUTPUT_WIDTH>& outputTable,
                                 size_t inputRow, size_t outputRow,
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
