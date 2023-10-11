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
      _subtree(std::move(child)),
      _lhs(leftSide),
      _rhs(rightSide),
      _resultWidth(2),
      _minDist(minDist),
      _maxDist(maxDist) {
  if (leftSide.isVariable()) {
    _variableColumns[std::get<Variable>(leftSide.value)] =
        makeAlwaysDefinedColumn(0);
  }
  if (rightSide.isVariable()) {
    _variableColumns[std::get<Variable>(rightSide.value)] =
        makeAlwaysDefinedColumn(1);
  }

  _lhs.outputCol = 0;
  _rhs.outputCol = 1;
}

// _____________________________________________________________________________
std::string TransitivePath::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "TransitivePath leftCol " << _lhs.subCol << " rightCol " << _rhs.subCol;

  if (std::holds_alternative<Id>(_lhs.value)) {
    os << " leftValue " << std::get<Id>(_lhs.value);
  }
  if (std::holds_alternative<Id>(_rhs.value)) {
    os << " rightValue " << std::get<Id>(_rhs.value);
  }
  os << " minDist " << _minDist << " maxDist " << _maxDist << "\n";
  os << _subtree->asString(indent) << "\n";
  if (_lhs.treeAndCol.has_value()) {
    os << "Left subtree:\n";
    os << _lhs.treeAndCol.value().first->asString(indent) << "\n";
  }
  os << _subtree->asString(indent) << "\n";
  if (_rhs.treeAndCol.has_value()) {
    os << "Right subtree:\n";
    os << _rhs.treeAndCol.value().first->asString(indent) << "\n";
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
std::string TransitivePath::getDescriptor() const {
  std::ostringstream os;
  os << "TransitivePath ";
  // If not full transitive hull, show interval as [min, max].
  if (_minDist > 1 || _maxDist < std::numeric_limits<size_t>::max()) {
    os << "[" << _minDist << ", " << _maxDist << "] ";
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
  if (_lhs.isVariable()) {
    os << std::get<Variable>(_lhs.value).name();
  } else {
    os << getName(std::get<Id>(_lhs.value));
  }
  // The predicate.
  auto scanOperation =
      std::dynamic_pointer_cast<IndexScan>(_subtree->getRootOperation());
  if (scanOperation != nullptr) {
    os << " " << scanOperation->getPredicate() << " ";
  } else {
    // Escaped the question marks to avoid a warning about ignored trigraphs.
    os << R"( <???> )";
  }
  // Right variable or entity name.
  if (_rhs.isVariable()) {
    os << std::get<Variable>(_rhs.value).name();
  } else {
    os << getName(std::get<Id>(_rhs.value));
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t TransitivePath::getResultWidth() const { return _resultWidth; }

// _____________________________________________________________________________
vector<ColumnIndex> TransitivePath::resultSortedOn() const {
  const std::vector<ColumnIndex>& subSortedOn =
      _subtree->getRootOperation()->getResultSortedOn();
  if (_lhs.isBound() && _lhs.isVariable() && _rhs.isBound() &&
      _lhs.isVariable() && subSortedOn.size() > 0 &&
      subSortedOn[0] == _lhs.subCol) {
    // This operation preserves the order of the _leftCol of the subtree.
    return {0};
  }
  if (_lhs.treeAndCol.has_value()) {
    auto tree = _lhs.treeAndCol.value().first;
    auto col = _lhs.treeAndCol.value().second;
    const std::vector<ColumnIndex>& leftSortedOn =
        tree->getRootOperation()->getResultSortedOn();
    if (leftSortedOn.size() > 0 && leftSortedOn[0] == col) {
      return {0};
    }
  }
  if (_rhs.treeAndCol.has_value()) {
    auto tree = _rhs.treeAndCol.value().first;
    auto col = _rhs.treeAndCol.value().second;
    const std::vector<ColumnIndex>& rightSortedOn =
        tree->getRootOperation()->getResultSortedOn();
    if (rightSortedOn.size() > 0 && rightSortedOn[0] == col) {
      return {1};
    }
  }
  return {};
}

// _____________________________________________________________________________
VariableToColumnMap TransitivePath::computeVariableToColumnMap() const {
  return _variableColumns;
}

// _____________________________________________________________________________
void TransitivePath::setTextLimit(size_t limit) {
  _subtree->setTextLimit(limit);
  if (_lhs.treeAndCol.has_value()) {
    _lhs.treeAndCol.value().first->setTextLimit(limit);
  }
  if (_rhs.treeAndCol.has_value()) {
    _rhs.treeAndCol.value().first->setTextLimit(limit);
  }
}

// _____________________________________________________________________________
bool TransitivePath::knownEmptyResult() { return _subtree->knownEmptyResult(); }

// _____________________________________________________________________________
float TransitivePath::getMultiplicity(size_t col) {
  (void)col;
  // The multiplicities are not known.
  return 1;
}

// _____________________________________________________________________________
uint64_t TransitivePath::getSizeEstimateBeforeLimit() {
  if (std::holds_alternative<Id>(_lhs.value) ||
      std::holds_alternative<Id>(_rhs.value)) {
    // If the subject or object is fixed, assume that the number of matching
    // triples is 1000. This will usually be an overestimate, but it will do the
    // job of avoiding query plans that first generate large intermediate
    // results and only then merge them with a triple such as this. In the
    // _lhs.isVar && _rhs.isVar case below, we assume a worst-case blowup of
    // 10000; see the comment there.
    return 1000;
  }
  if (_lhs.treeAndCol.has_value()) {
    return _lhs.treeAndCol.value().first->getSizeEstimate();
  }
  if (_rhs.treeAndCol.has_value()) {
    return _rhs.treeAndCol.value().first->getSizeEstimate();
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
  if (_lhs.isVariable() && _rhs.isVariable()) {
    return _subtree->getSizeEstimate() * 10000;
  }
  // TODO(Florian): this is not necessarily a good estimator
  if (_lhs.isVariable()) {
    return _subtree->getSizeEstimate() / _subtree->getMultiplicity(_lhs.subCol);
  }
  return _subtree->getSizeEstimate();
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

// This instantiantion is needed by the unit tests
// template void TransitivePath::computeTransitivePath<2>(
//     IdTable* res, const IdTable& sub);

// _____________________________________________________________________________
template <size_t RES_WIDTH, size_t SUB_WIDTH, size_t OTHER_WIDTH>
void TransitivePath::computeTransitivePathBound(
    IdTable* dynRes, const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide, const IdTable& otherTable) const {
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  auto [edges, nodes] = setupMapAndNodes<SUB_WIDTH, OTHER_WIDTH>(
      dynSub, startSide, targetSide, otherTable);

  Map hull = transitiveHull(edges, nodes);

  TransitivePath::fillTableWithHull<RES_WIDTH, OTHER_WIDTH>(
      res, hull, nodes, startSide.outputCol, targetSide.outputCol, otherTable,
      startSide.treeAndCol.value().second);

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

  Map hull = transitiveHull(edges, nodes);

  TransitivePath::fillTableWithHull<RES_WIDTH>(res, hull, startSide.outputCol,
                                               targetSide.outputCol);

  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
ResultTable TransitivePath::computeResult() {
  LOG(DEBUG) << "TransitivePath subresult computation..." << std::endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "TransitivePath subresult computation done." << std::endl;

  IdTable idTable{getExecutionContext()->getAllocator()};

  idTable.setNumColumns(getResultWidth());

  size_t subWidth = subRes->idTable().numColumns();
  if (_lhs.treeAndCol.has_value()) {
    LOG(DEBUG) << "TransitivePath left result computation..." << std::endl;
    shared_ptr<const ResultTable> leftRes =
        _lhs.treeAndCol.value().first->getResult();
    LOG(DEBUG) << "TransitivePath left result computation done." << std::endl;
    size_t leftWidth = leftRes->idTable().numColumns();

    LOG(DEBUG) << "TransitivePath result computation..." << std::endl;
    CALL_FIXED_SIZE((std::array{_resultWidth, subWidth, leftWidth}),
                    &TransitivePath::computeTransitivePathBound, this, &idTable,
                    subRes->idTable(), _lhs, _rhs, leftRes->idTable());

  } else if (_rhs.treeAndCol.has_value()) {
    LOG(DEBUG) << "TransitivePath right result computation..." << std::endl;
    shared_ptr<const ResultTable> rightRes =
        _rhs.treeAndCol.value().first->getResult();
    LOG(DEBUG) << "TransitivePath right result computation done." << std::endl;
    size_t rightWidth = rightRes->idTable().numColumns();

    LOG(DEBUG) << "TransitivePath result computation..." << std::endl;
    CALL_FIXED_SIZE((std::array{_resultWidth, subWidth, rightWidth}),
                    &TransitivePath::computeTransitivePathBound, this, &idTable,
                    subRes->idTable(), _rhs, _lhs, rightRes->idTable());

  } else {
    LOG(DEBUG) << "TransitivePath result computation..." << std::endl;
    CALL_FIXED_SIZE((std::array{_resultWidth, subWidth}),
                    &TransitivePath::computeTransitivePath, this, &idTable,
                    subRes->idTable(), _lhs, _rhs);
  }

  LOG(DEBUG) << "TransitivePath result computation done." << std::endl;
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
      getExecutionContext(), _subtree, _lhs, _rhs, _minDist, _maxDist);
  if (isLeft) {
    p->_lhs.treeAndCol = std::make_pair(leftOrRightOp, inputCol);
  } else {
    p->_rhs.treeAndCol = std::make_pair(leftOrRightOp, inputCol);
  }

  // Note: The `variable` in the following structured binding is `const`, even
  // if we bind by value. We deliberately make one unnecessary copy of the
  // `variable` to keep the code simpler.
  for (auto [variable, columnIndexWithType] :
       leftOrRightOp->getVariableColumns()) {
    ColumnIndex columnIndex = columnIndexWithType.columnIndex_;
    if (columnIndex != inputCol) {
      if (columnIndex > inputCol) {
        columnIndexWithType.columnIndex_++;
        p->_variableColumns[variable] = columnIndexWithType;
      } else {
        columnIndexWithType.columnIndex_ += 2;
        p->_variableColumns[variable] = columnIndexWithType;
      }
      p->_resultWidth++;
    }
  }
  return p;
}

// _____________________________________________________________________________
bool TransitivePath::isBound() const {
  return _lhs.isBound() && _rhs.isBound();
}

// _____________________________________________________________________________
bool TransitivePath::leftIsBound() const { return _lhs.isBound(); }

// _____________________________________________________________________________
bool TransitivePath::rightIsBound() const { return _rhs.isBound(); }

// _____________________________________________________________________________
TransitivePath::Map TransitivePath::transitiveHull(
    const Map& edges, const std::vector<Id>& nodes) const {
  using MapIt = TransitivePath::Map::const_iterator;
  // For every node do a dfs on the graph
  Map hull;

  // Stores nodes we already have a path to. This avoids cycles.
  ad_utility::HashSet<Id> marks;

  // The stack used to store the dfs' progress
  std::vector<ad_utility::HashSet<Id>::const_iterator> positions;

  // Used to store all edges leading away from a node for every level.
  // Reduces access to the hashmap, and is safe as the map will not
  // be modified after this point.
  std::vector<std::shared_ptr<const ad_utility::HashSet<Id>>> edgeCache;

  for (size_t i = 0; i < nodes.size(); i++) {
    if (hull.contains(nodes[i])) {
      // We have already computed the hull for this node
      continue;
    }

    MapIt rootEdges = edges.find(nodes[i]);
    if (rootEdges != edges.end()) {
      positions.push_back(rootEdges->second->begin());
      edgeCache.push_back(rootEdges->second);
    }
    if (_minDist == 0) {
      hull.try_emplace(nodes[i], std::make_shared<ad_utility::HashSet<Id>>());
      hull[nodes[i]]->insert(nodes[i]);
    }

    // While we have not found the entire transitive hull and have not reached
    // the max step limit
    while (!positions.empty()) {
      size_t stackIndex = positions.size() - 1;
      // Process the next child of the node at the top of the stack
      ad_utility::HashSet<Id>::const_iterator& pos = positions[stackIndex];
      const ad_utility::HashSet<Id>* nodeEdges = edgeCache.back().get();

      if (pos == nodeEdges->end()) {
        // We finished processing this node
        positions.pop_back();
        edgeCache.pop_back();
        continue;
      }

      Id child = *pos;
      ++pos;
      size_t childDepth = positions.size();
      if (childDepth <= _maxDist && marks.count(child) == 0) {
        // process the child
        if (childDepth >= _minDist) {
          marks.insert(child);
          if (_rhs.isVariable() || child == std::get<Id>(_rhs.value)) {
            hull.try_emplace(nodes[i],
                             std::make_shared<ad_utility::HashSet<Id>>());
            hull[nodes[i]]->insert(child);
          } else if (_lhs.isVariable() || child == std::get<Id>(_lhs.value)) {
            hull.try_emplace(child,
                             std::make_shared<ad_utility::HashSet<Id>>());
            hull[child]->insert(nodes[i]);
          }
        }
        // Add the child to the stack
        MapIt it = edges.find(child);
        if (it != edges.end()) {
          positions.push_back(it->second->begin());
          edgeCache.push_back(it->second);
        }
      }
    }

    if (i + 1 < nodes.size()) {
      // reset everything for the next iteration
      marks.clear();
    }
  }
  return hull;
}

// _____________________________________________________________________________
template <size_t WIDTH, size_t TEMP_WIDTH>
void TransitivePath::fillTableWithHull(IdTableStatic<WIDTH>& table, Map hull,
                                       std::vector<Id>& nodes,
                                       size_t startSideCol,
                                       size_t targetSideCol,
                                       const IdTable& tableTemplate,
                                       size_t skipCol) {
  IdTableView<TEMP_WIDTH> templateView =
      tableTemplate.asStaticView<TEMP_WIDTH>();

  size_t rowIndex = 0;
  for (size_t i = 0; i < nodes.size(); i++) {
    Id node = nodes[i];
    if (!hull.contains(node)) {
      continue;
    }

    for (Id otherNode : *hull[node]) {
      table.emplace_back();
      table(rowIndex, startSideCol) = node;
      table(rowIndex, targetSideCol) = otherNode;

      TransitivePath::copyColumns<TEMP_WIDTH, WIDTH>(templateView, table, i,
                                                     rowIndex, skipCol);

      rowIndex++;
    }
  }
}

// _____________________________________________________________________________
template <size_t WIDTH>
void TransitivePath::fillTableWithHull(IdTableStatic<WIDTH>& table, Map hull,
                                       size_t startSideCol,
                                       size_t targetSideCol) {
  size_t rowIndex = 0;
  for (auto const& [node, linkedNodes] : hull) {
    for (Id linkedNode : *linkedNodes) {
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
  nodes = setupNodesVector<SIDE_WIDTH>(startSideTable,
                                       startSide.treeAndCol.value().second);

  return std::make_pair(edges, nodes);
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
  if (startSide.isBound()) {
    nodes.push_back(std::get<Id>(startSide.value));
    // var -> var
  } else {
    nodes = setupNodesVector<SUB_WIDTH>(sub, startSide.subCol);
    if (_minDist == 0) {
      std::vector<Id> targetNodes =
          setupNodesVector<SUB_WIDTH>(sub, targetSide.subCol);
      nodes.insert(nodes.end(), targetNodes.begin(), targetNodes.end());
    }
  }

  return std::make_pair(edges, nodes);
}

// _____________________________________________________________________________
template <size_t SUB_WIDTH>
TransitivePath::Map TransitivePath::setupEdgesMap(
    const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) {
  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  Map edges;

  for (size_t i = 0; i < sub.size(); i++) {
    // checkTimeoutHashSet();
    Id startId = sub(i, startSide.subCol);
    Id targetId = sub(i, targetSide.subCol);
    MapIt it = edges.find(startId);
    if (it == edges.end()) {
      std::shared_ptr<ad_utility::HashSet<Id>> edgeTargets =
          std::make_shared<ad_utility::HashSet<Id>>();
      edgeTargets->insert(targetId);
      edges[startId] = edgeTargets;
    } else {
      // If r is not in the vector insert it
      it->second->insert(targetId);
    }
  }
  return edges;
}

// _____________________________________________________________________________
template <size_t WIDTH>
std::vector<Id> TransitivePath::setupNodesVector(const IdTable& table,
                                                 size_t col) {
  std::vector<Id> nodes;
  const IdTableView<WIDTH> tableView = table.asStaticView<WIDTH>();
  for (size_t i = 0; i < tableView.size(); i++) {
    nodes.push_back(tableView(i, col));
  }
  return nodes;
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
