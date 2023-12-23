// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "TransitivePath.h"

#include <limits>

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

  auto [edges, nodes] = setupMapAndNodes<SUB_WIDTH, SIDE_WIDTH>(
      dynSub, startSide, targetSide, startSideTable);

  Map hull(allocator());
  if (!targetSide.isVariable()) {
    hull = transitiveHull(edges, nodes, std::get<Id>(targetSide.value_));
  } else {
    hull = transitiveHull(edges, nodes, std::nullopt);
  }

  TransitivePath::fillTableWithHull<RES_WIDTH, SIDE_WIDTH>(
      res, hull, nodes, startSide.outputCol_, targetSide.outputCol_,
      startSideTable, startSide.treeAndCol_.value().second);

  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
template <size_t RES_WIDTH, size_t SUB_WIDTH>
void TransitivePath::computeTransitivePath(
    IdTable* dynRes, const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  auto [edges, nodes] =
      setupMapAndNodes<SUB_WIDTH>(dynSub, startSide, targetSide);

  Map hull{allocator()};
  if (!targetSide.isVariable()) {
    hull = transitiveHull(edges, nodes, std::get<Id>(targetSide.value_));
  } else {
    hull = transitiveHull(edges, nodes, std::nullopt);
  }

  TransitivePath::fillTableWithHull<RES_WIDTH>(res, hull, startSide.outputCol_,
                                               targetSide.outputCol_);

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
TransitivePath::Map TransitivePath::transitiveHull(
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
void TransitivePath::fillTableWithHull(IdTableStatic<WIDTH>& table,
                                       const Map& hull, std::vector<Id>& nodes,
                                       size_t startSideCol,
                                       size_t targetSideCol,
                                       const IdTable& startSideTable,
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

      TransitivePath::copyColumns<START_WIDTH, WIDTH>(startView, table, i,
                                                      rowIndex, skipCol);

      rowIndex++;
    }
  }
}

// _____________________________________________________________________________
template <size_t WIDTH>
void TransitivePath::fillTableWithHull(IdTableStatic<WIDTH>& table,
                                       const Map& hull, size_t startSideCol,
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
std::pair<TransitivePath::Map, std::vector<Id>>
TransitivePath::setupMapAndNodes(const IdTable& sub,
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
std::pair<TransitivePath::Map, std::vector<Id>>
TransitivePath::setupMapAndNodes(const IdTable& sub,
                                 const TransitivePathSide& startSide,
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
TransitivePath::Map TransitivePath::setupEdgesMap(
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
std::span<const Id> TransitivePath::setupNodes(const IdTable& table,
                                               size_t col) {
  return table.getColumn(col);
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

// _____________________________________________________________________________
void TransitivePath::insertIntoMap(Map& map, Id key, Id value) const {
  auto [it, success] = map.try_emplace(key, allocator());
  it->second.insert(value);
}
