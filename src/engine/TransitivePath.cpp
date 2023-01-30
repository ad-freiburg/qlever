// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "TransitivePath.h"

#include <limits>

#include "../util/Exception.h"
#include "CallFixedSize.h"
#include "IndexScan.h"

// _____________________________________________________________________________
TransitivePath::TransitivePath(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    bool leftIsVar, bool rightIsVar, size_t leftSubCol, size_t rightSubCol,
    Id leftValue, Id rightValue, const Variable& leftColName,
    const Variable& rightColName, size_t minDist, size_t maxDist)
    : Operation(qec),
      _leftSideTree(nullptr),
      _leftSideCol(-1),
      _rightSideTree(nullptr),
      _rightSideCol(-1),
      _resultWidth(2),
      _subtree(child),
      _leftIsVar(leftIsVar),
      _rightIsVar(rightIsVar),
      _leftSubCol(leftSubCol),
      _rightSubCol(rightSubCol),
      _leftValue(leftValue),
      _rightValue(rightValue),
      // TODO<joka921> Those should also be variables.
      _leftColName(leftColName.name()),
      _rightColName(rightColName.name()),
      _minDist(minDist),
      _maxDist(maxDist) {
  _variableColumns[leftColName] = 0;
  _variableColumns[rightColName] = 1;
}

// _____________________________________________________________________________
std::string TransitivePath::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "TransitivePath leftCol " << _leftSubCol << " rightCol "
     << _rightSubCol;

  if (!_leftIsVar) {
    os << " leftValue " << _leftValue;
  }
  if (!_rightIsVar) {
    os << " rightValue " << _rightValue;
  }
  os << " minDist " << _minDist << " maxDist " << _maxDist << "\n";
  os << _subtree->asString(indent) << "\n";
  if (_leftSideTree != nullptr) {
    os << "Left subtree:\n";
    os << _leftSideTree->asString(indent) << "\n";
  }
  os << _subtree->asString(indent) << "\n";
  if (_rightSideTree != nullptr) {
    os << "Right subtree:\n";
    os << _rightSideTree->asString(indent) << "\n";
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
  // Left variable or entity name.
  if (_leftIsVar) {
    os << _leftColName;
  } else {
    os << getIndex()
              .idToOptionalString(_leftValue)
              .value_or("#" + std::to_string(_leftValue.getBits()));
  }
  // The predicate.
  auto scanOperation =
      std::dynamic_pointer_cast<IndexScan>(_subtree->getRootOperation());
  if (scanOperation != nullptr) {
    os << " " << scanOperation->getPredicate() << " ";
  } else {
    // Escaped the question marks to avoid a warning about ignored trigraphs.
    os << " <\?\?\?> ";
  }
  // Right variable or entity name.
  if (_rightIsVar) {
    os << _rightColName;
  } else {
    os << getIndex()
              .idToOptionalString(_rightValue)
              .value_or("#" + std::to_string(_rightValue.getBits()));
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t TransitivePath::getResultWidth() const { return _resultWidth; }

// _____________________________________________________________________________
vector<size_t> TransitivePath::resultSortedOn() const {
  const std::vector<size_t>& subSortedOn =
      _subtree->getRootOperation()->getResultSortedOn();
  if (_leftSideTree == nullptr && _rightSideTree == nullptr && _leftIsVar &&
      _rightIsVar && subSortedOn.size() > 0 && subSortedOn[0] == _leftSubCol) {
    // This operation preserves the order of the _leftCol of the subtree.
    return {0};
  }
  if (_leftSideTree != nullptr) {
    const std::vector<size_t>& leftSortedOn =
        _leftSideTree->getRootOperation()->getResultSortedOn();
    if (leftSortedOn.size() > 0 && leftSortedOn[0] == _leftSideCol) {
      return {0};
    }
  }
  if (_rightSideTree != nullptr) {
    const std::vector<size_t>& rightSortedOn =
        _rightSideTree->getRootOperation()->getResultSortedOn();
    if (rightSortedOn.size() > 0 && rightSortedOn[0] == _rightSideCol) {
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
  if (_leftSideTree != nullptr) {
    _leftSideTree->setTextLimit(limit);
  }
  if (_rightSideTree != nullptr) {
    _rightSideTree->setTextLimit(limit);
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
size_t TransitivePath::getSizeEstimate() {
  if (!_leftIsVar || !_rightIsVar) {
    // If the subject or object is fixed, assume that the number of matching
    // triples is 1000. This will usually be an overestimate, but it will do the
    // job of avoiding query plans that first generate large intermediate
    // results and only then merge them with a triple such as this. In the
    // _leftIsVar && _rightIsVar case below, we assume a worst-case blowup of
    // 10000; see the comment there.
    return 1000;
  }
  if (_leftSideTree != nullptr) {
    return _leftSideTree->getSizeEstimate();
  }
  if (_rightSideTree != nullptr) {
    return _rightSideTree->getSizeEstimate();
  }
  // Set costs to something very large, so that we never compute the complete
  // transitive hull (unless the variables on both sides are not bound in any
  // other way, so that the only possible query plan is to compute the complete
  // transitive hull).
  //
  // NOTE: _subtree->getSizeEstimate() is the number of triples of the
  // predicate, for which the transitive hull operator (+) is specified. On
  // Wikidata, the predicate with the largest blowup when taking the
  // transitive hull is wdt:P2789 (connects with). The blowup is then from 90K
  // (without +) to 110M (with +), so about 1000 times larger.
  if (_leftIsVar && _rightIsVar) {
    return _subtree->getSizeEstimate() * 10000;
  }
  // TODO(Florian): this is not necessarily a good estimator
  if (_leftIsVar) {
    return _subtree->getSizeEstimate() / _subtree->getMultiplicity(_leftSubCol);
  }
  return _subtree->getSizeEstimate();
}

// _____________________________________________________________________________
size_t TransitivePath::getCostEstimate() {
  // We assume that the cost of computing the transitive path is proportional to
  // the result size.
  auto costEstimate = getSizeEstimate();
  // Add the cost for the index scan of the predicate involved.
  for (auto* ptr : getChildren()) {
    if (ptr) {
      costEstimate += ptr->getCostEstimate();
    }
  }
  return costEstimate;
}

// _____________________________________________________________________________
template <int SUB_WIDTH>
void TransitivePath::computeTransitivePath(IdTable* res, const IdTable& sub,
                                           bool leftIsVar, bool rightIsVar,
                                           size_t leftSubCol,
                                           size_t rightSubCol, Id leftValue,
                                           Id rightValue, size_t minDist,
                                           size_t maxDist) {
  if (leftIsVar) {
    if (rightIsVar) {
      computeTransitivePath<SUB_WIDTH, true, true>(
          res, sub, leftSubCol, rightSubCol, leftValue, rightValue, minDist,
          maxDist);
    } else {
      computeTransitivePath<SUB_WIDTH, true, false>(
          res, sub, leftSubCol, rightSubCol, leftValue, rightValue, minDist,
          maxDist);
    }
  } else {
    if (rightIsVar) {
      computeTransitivePath<SUB_WIDTH, false, true>(
          res, sub, leftSubCol, rightSubCol, leftValue, rightValue, minDist,
          maxDist);
    } else {
      computeTransitivePath<SUB_WIDTH, false, false>(
          res, sub, leftSubCol, rightSubCol, leftValue, rightValue, minDist,
          maxDist);
    }
  }
}

// This instantiantion is needed by the unit tests
template void TransitivePath::computeTransitivePath<2>(
    IdTable* res, const IdTable& sub, bool leftIsVar, bool rightIsVar,
    size_t leftSubCol, size_t rightSubCol, Id leftValue, Id rightValue,
    size_t minDist, size_t maxDist);

// _____________________________________________________________________________
template <int SUB_WIDTH, bool leftIsVar, bool rightIsVar>
void TransitivePath::computeTransitivePath(IdTable* dynRes,
                                           const IdTable& dynSub,
                                           size_t leftSubCol,
                                           size_t rightSubCol, Id leftValue,
                                           Id rightValue, size_t minDist,
                                           size_t maxDist) {
  using Map = ad_utility::HashMap<Id, std::shared_ptr<ad_utility::HashSet<Id>>>;
  using MapIt = Map::iterator;

  if constexpr (!leftIsVar && !rightIsVar) {
    return;
  }

  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  IdTableStatic<2> res = std::move(*dynRes).toStatic<2>();

  // Used to map entries in the left column to entries they have connection with
  Map edges;
  // All nodes on the graph from which an edge leads to another node
  std::vector<Id> nodes;

  auto checkTimeoutAfterNCalls = checkTimeoutAfterNCallsFactory();
  auto checkTimeoutHashSet = [&checkTimeoutAfterNCalls]() {
    checkTimeoutAfterNCalls(NUM_OPERATIONS_HASHSET_LOOKUP);
  };

  // initialize the map from the subresult
  if constexpr (rightIsVar) {
    (void)rightValue;
    if (!leftIsVar) {
      nodes.push_back(leftValue);
    }
    for (size_t i = 0; i < sub.size(); i++) {
      checkTimeoutHashSet();
      Id l = sub(i, leftSubCol);
      Id r = sub(i, rightSubCol);
      MapIt it = edges.find(l);
      if (it == edges.end()) {
        if constexpr (leftIsVar) {
          nodes.push_back(l);
        }
        std::shared_ptr<ad_utility::HashSet<Id>> s =
            std::make_shared<ad_utility::HashSet<Id>>();
        s->insert(r);
        edges[l] = s;
      } else {
        // If r is not in the vector insert it
        it->second->insert(r);
      }
    }
  } else {
    (void)leftValue;
    nodes.push_back(rightValue);
    for (size_t i = 0; i < sub.size(); i++) {
      checkTimeoutHashSet();
      // Use the inverted edges
      Id l = sub(i, leftSubCol);
      Id r = sub(i, rightSubCol);
      MapIt it = edges.find(r);
      if (it == edges.end()) {
        std::shared_ptr<ad_utility::HashSet<Id>> s =
            std::make_shared<ad_utility::HashSet<Id>>();
        s->insert(l);
        edges[r] = s;
      } else {
        // If r is not in the vector insert it
        it->second->insert(l);
      }
    }
  }

  // For every node do a dfs on the graph

  // Stores nodes we already have a path to. This avoids cycles.
  ad_utility::HashSet<Id> marks;

  // The stack used to store the dfs' progress
  std::vector<ad_utility::HashSet<Id>::const_iterator> positions;

  // Used to store all edges leading away from a node for every level.
  // Reduces access to the hashmap, and is safe as the map will not
  // be modified after this point.
  std::vector<std::shared_ptr<const ad_utility::HashSet<Id>>> edgeCache;

  for (size_t i = 0; i < nodes.size(); i++) {
    MapIt rootEdges = edges.find(nodes[i]);
    if (rootEdges != edges.end()) {
      positions.push_back(rootEdges->second->begin());
      edgeCache.push_back(rootEdges->second);
    }
    if (minDist == 0) {
      AD_THROW(
          "The TransitivePath operation does not support a minimum "
          "distance of 0 (use at least one instead).");
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
      if (childDepth <= maxDist && marks.count(child) == 0) {
        // process the child
        if (childDepth >= minDist) {
          marks.insert(child);
          if constexpr (rightIsVar) {
            res.push_back({nodes[i], child});
          } else {
            res.push_back({child, nodes[i]});
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

  *dynRes = std::move(res).toDynamic();
}

template <int SUB_WIDTH, int LEFT_WIDTH, int RES_WIDTH>
void TransitivePath::computeTransitivePathLeftBound(
    IdTable* dynRes, const IdTable& dynSub, const IdTable& dynLeft,
    size_t leftSideCol, bool rightIsVar, size_t leftSubCol, size_t rightSubCol,
    Id rightValue, size_t minDist, size_t maxDist, size_t resWidth) {
  using Map = ad_utility::HashMap<Id, std::shared_ptr<ad_utility::HashSet<Id>>>;
  using MapIt = Map::iterator;

  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  const IdTableView<LEFT_WIDTH> left = dynLeft.asStaticView<LEFT_WIDTH>();
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  // Used to map entries in the left column to entries they have connection with
  Map edges;

  auto checkTimeoutAfterNCalls = checkTimeoutAfterNCallsFactory();
  auto checkTimeoutHashSet = [&checkTimeoutAfterNCalls]() {
    checkTimeoutAfterNCalls(NUM_OPERATIONS_HASHSET_LOOKUP);
  };
  // initialize the map from the subresult
  for (size_t i = 0; i < sub.size(); i++) {
    checkTimeoutHashSet();
    Id l = sub(i, leftSubCol);
    Id r = sub(i, rightSubCol);
    MapIt it = edges.find(l);
    if (it == edges.end()) {
      std::shared_ptr<ad_utility::HashSet<Id>> s =
          std::make_shared<ad_utility::HashSet<Id>>();
      s->insert(r);
      edges[l] = s;
    } else {
      // If r is not in the vector insert it
      it->second->insert(r);
    }
  }

  // For every node do a dfs on the graph

  // Stores nodes we already have a path to. This avoids cycles.
  ad_utility::HashSet<Id> marks;

  // The stack used to store the dfs' progress
  std::vector<ad_utility::HashSet<Id>::const_iterator> positions;
  // Used to store all edges leading away from a node for every level.
  // Reduces access to the hashmap, and is safe as the map will not
  // be modified after this point.
  std::vector<std::shared_ptr<const ad_utility::HashSet<Id>>> edgeCache;

  Id last_elem = ID_NO_VALUE;
  size_t last_result_begin = 0;
  size_t last_result_end = 0;
  for (size_t i = 0; i < left.size(); i++) {
    checkTimeoutHashSet();
    if (left[i][leftSideCol] == last_elem) {
      // We can repeat the last output
      size_t num_new = last_result_end - last_result_begin;
      size_t res_row = res.size();
      res.resize(res.size() + num_new);
      checkTimeoutAfterNCalls(num_new * resWidth);
      for (size_t j = 0; j < num_new; j++) {
        for (size_t c = 0; c < resWidth; c++) {
          res(res_row + j, c) = res(last_result_begin + j, c);
        }
      }
      continue;
    }
    last_elem = left[i][leftSideCol];
    last_result_begin = res.size();
    MapIt rootEdges = edges.find(last_elem);
    if (rootEdges != edges.end()) {
      positions.push_back(rootEdges->second->begin());
      edgeCache.push_back(rootEdges->second);
    }
    if (minDist == 0) {
      AD_THROW(
          "The TransitivePath operation does not support a minimum "
          "distance of 0 (use at least one instead).");
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
      if (childDepth <= maxDist && marks.count(child) == 0) {
        // process the child
        if (childDepth >= minDist) {
          marks.insert(child);
          if (rightIsVar || child == rightValue) {
            size_t row = res.size();
            res.emplace_back();
            res(row, 0) = last_elem;
            res(row, 1) = child;
            for (size_t k = 2; k < resWidth + 1; k++) {
              if (k - 2 < leftSideCol) {
                res(row, k) = left(i, k - 2);
              } else if (k - 2 > leftSideCol) {
                res(row, k - 1) = left(i, k - 2);
              }
            }
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

    if (i + 1 < left.size()) {
      // reset everything for the next iteration
      marks.clear();
    }

    last_result_end = res.size();
  }

  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
template <int SUB_WIDTH, int LEFT_WIDTH, int RES_WIDTH>
void TransitivePath::computeTransitivePathRightBound(
    IdTable* dynRes, const IdTable& dynSub, const IdTable& dynRight,
    size_t rightSideCol, bool leftIsVar, size_t leftSubCol, size_t rightSubCol,
    Id leftValue, size_t minDist, size_t maxDist, size_t resWidth) {
  // Do the discovery from the right instead of the left.

  using Map = ad_utility::HashMap<Id, std::shared_ptr<ad_utility::HashSet<Id>>>;
  using MapIt = Map::iterator;

  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  const IdTableView<LEFT_WIDTH> right = dynRight.asStaticView<LEFT_WIDTH>();
  IdTableStatic<RES_WIDTH> res = std::move(*dynRes).toStatic<RES_WIDTH>();

  // Used to map entries in the left column to entries they have connection with
  Map edges;
  auto checkTimeoutAfterNCalls = checkTimeoutAfterNCallsFactory();
  auto checkTimeoutHashSet = [&checkTimeoutAfterNCalls]() {
    checkTimeoutAfterNCalls(NUM_OPERATIONS_HASHSET_LOOKUP);
  };

  // initialize the map from the subresult
  for (size_t i = 0; i < sub.size(); i++) {
    checkTimeoutHashSet();
    Id l = sub(i, leftSubCol);
    Id r = sub(i, rightSubCol);
    MapIt it = edges.find(r);
    if (it == edges.end()) {
      std::shared_ptr<ad_utility::HashSet<Id>> s =
          std::make_shared<ad_utility::HashSet<Id>>();
      s->insert(l);
      edges[r] = s;
    } else {
      it->second->insert(l);
    }
  }

  // For every node do a dfs on the graph

  // Stores nodes we already have a path to. This avoids cycles.
  ad_utility::HashSet<Id> marks;

  // The stack used to store the dfs' progress
  std::vector<ad_utility::HashSet<Id>::const_iterator> positions;

  // Used to store all edges leading away from a node for every level.
  // Reduces access to the hashmap, and is safe as the map will not
  // be modified after this point.
  std::vector<std::shared_ptr<const ad_utility::HashSet<Id>>> edgeCache;

  Id last_elem = ID_NO_VALUE;
  size_t last_result_begin = 0;
  size_t last_result_end = 0;
  for (size_t i = 0; i < right.size(); i++) {
    checkTimeoutHashSet();
    if (right[i][rightSideCol] == last_elem) {
      // We can repeat the last output
      size_t num_new = last_result_end - last_result_begin;
      size_t res_row = res.size();
      res.resize(res.size() + num_new);
      for (size_t j = 0; j < num_new; j++) {
        for (size_t c = 0; c < resWidth; c++) {
          res(res_row + j, c) = res(last_result_begin + j, c);
        }
      }
      checkTimeoutAfterNCalls(num_new * resWidth);
      continue;
    }
    last_elem = right(i, rightSideCol);
    last_result_begin = res.size();
    MapIt rootEdges = edges.find(last_elem);
    if (rootEdges != edges.end()) {
      positions.push_back(rootEdges->second->begin());
      edgeCache.push_back(rootEdges->second);
    }
    if (minDist == 0) {
      AD_THROW(
          "The TransitivePath operation does not support a minimum "
          "distance of 0 (use at least one instead).");
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
      if (childDepth <= maxDist && marks.count(child) == 0) {
        // process the child
        if (childDepth >= minDist) {
          marks.insert(child);
          if (leftIsVar || child == leftValue) {
            size_t row = res.size();
            res.emplace_back();
            res(row, 0) = child;
            res(row, 1) = last_elem;
            for (size_t k = 2; k < resWidth + 1; k++) {
              if (k - 2 < rightSideCol) {
                res(row, k) = right(i, k - 2);
              } else if (k - 2 > rightSideCol) {
                res(row, k - 1) = right(i, k - 2);
              }
            }
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

    if (i + 1 < right.size()) {
      // reset everything for the next iteration
      marks.clear();
    }

    last_result_end = res.size();
  }

  *dynRes = std::move(res).toDynamic();
}

// _____________________________________________________________________________
void TransitivePath::computeResult(ResultTable* result) {
  LOG(DEBUG) << "TransitivePath result computation..." << std::endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "TransitivePath subresult computation done." << std::endl;

  // NOTE: The only place, where the input to a transitive path operation is not
  // an index scan (which have empty local vocabularies by default) is the
  // `LocalVocabTest`. But it doesn't harm to propagate `_localVocab` here
  // either.
  result->_localVocab = subRes->_localVocab;

  result->_sortedBy = resultSortedOn();
  if (_leftIsVar || _leftSideTree != nullptr) {
    result->_resultTypes.push_back(subRes->getResultType(_leftSubCol));
  } else {
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
  }
  if (_rightIsVar || _rightSideTree != nullptr) {
    result->_resultTypes.push_back(subRes->getResultType(_rightSubCol));
  } else {
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
  }
  result->_idTable.setNumColumns(getResultWidth());

  int subWidth = subRes->_idTable.numColumns();
  if (_leftSideTree != nullptr) {
    shared_ptr<const ResultTable> leftRes = _leftSideTree->getResult();
    for (size_t c = 0; c < leftRes->_idTable.numColumns(); c++) {
      if (c != _leftSideCol) {
        result->_resultTypes.push_back(leftRes->getResultType(c));
      }
    }
    int leftWidth = leftRes->_idTable.numColumns();
    CALL_FIXED_SIZE(
        (std::array{subWidth, leftWidth, static_cast<int>(_resultWidth)}),
        &TransitivePath::computeTransitivePathLeftBound, this,
        &result->_idTable, subRes->_idTable, leftRes->_idTable, _leftSideCol,
        _rightIsVar, _leftSubCol, _rightSubCol, _rightValue, _minDist, _maxDist,
        _resultWidth);
  } else if (_rightSideTree != nullptr) {
    shared_ptr<const ResultTable> rightRes = _rightSideTree->getResult();
    for (size_t c = 0; c < rightRes->_idTable.numColumns(); c++) {
      if (c != _rightSideCol) {
        result->_resultTypes.push_back(rightRes->getResultType(c));
      }
    }
    int rightWidth = rightRes->_idTable.numColumns();
    CALL_FIXED_SIZE(
        (std::array{subWidth, rightWidth, static_cast<int>(_resultWidth)}),
        &TransitivePath::computeTransitivePathRightBound, this,
        &result->_idTable, subRes->_idTable, rightRes->_idTable, _rightSideCol,
        _leftIsVar, _leftSubCol, _rightSubCol, _leftValue, _minDist, _maxDist,
        _resultWidth);
  } else {
    CALL_FIXED_SIZE(subWidth, &TransitivePath::computeTransitivePath, this,
                    &result->_idTable, subRes->_idTable, _leftIsVar,
                    _rightIsVar, _leftSubCol, _rightSubCol, _leftValue,
                    _rightValue, _minDist, _maxDist);
  }

  LOG(DEBUG) << "TransitivePath result computation done." << std::endl;
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePath> TransitivePath::bindLeftSide(
    std::shared_ptr<QueryExecutionTree> leftop, size_t inputCol) const {
  // Enforce required sorting of `leftop`.
  leftop = QueryExecutionTree::createSortedTree(std::move(leftop), {inputCol});
  // Create a copy of this.
  //
  // NOTE: The RHS used to be `std::make_shared<TransitivePath>()`, which is
  // wrong because it first calls the copy constructor of the base class
  // `Operation`, which  would then ignore the changes in `variableColumnMap_`
  // made below (see `Operation::getInternallyVisibleVariableColumns` and
  // `Operation::getExternallyVariableColumns`).
  std::shared_ptr<TransitivePath> p = std::make_shared<TransitivePath>(
      getExecutionContext(), _subtree, _leftIsVar, _rightIsVar, _leftSubCol,
      _rightSubCol, _leftValue, _rightValue, Variable{_leftColName},
      Variable{_rightColName}, _minDist, _maxDist);
  p->_leftSideTree = leftop;
  p->_leftSideCol = inputCol;
  const auto& var = leftop->getVariableColumns();
  for (const auto& [variable, columnIndex] : var) {
    if (columnIndex != inputCol) {
      if (columnIndex > inputCol) {
        p->_variableColumns[variable] = columnIndex + 1;
      } else {
        p->_variableColumns[variable] = columnIndex + 2;
      }
      p->_resultWidth++;
    }
  }
  return p;
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePath> TransitivePath::bindRightSide(
    std::shared_ptr<QueryExecutionTree> rightop, size_t inputCol) const {
  // TODO<joka921> `bindRightSide` and `bindLeftSide` are almost the same,
  // could and should this be made generic? It probably requires refactoring
  // quite a lot of this class though.
  // Enforce required sorting of `rightop`.
  rightop =
      QueryExecutionTree::createSortedTree(std::move(rightop), {inputCol});
  // Create a copy of this. For a detailed explanation, see the analogous
  // comment in `bindLeftSide` above.
  std::shared_ptr<TransitivePath> p = std::make_shared<TransitivePath>(
      getExecutionContext(), _subtree, _leftIsVar, _rightIsVar, _leftSubCol,
      _rightSubCol, _leftValue, _rightValue, Variable{_leftColName},
      Variable{_rightColName}, _minDist, _maxDist);
  p->_rightSideTree = rightop;
  p->_rightSideCol = inputCol;
  const auto& var = rightop->getVariableColumns();
  for (const auto& [variable, columnIndex] : var) {
    if (columnIndex != inputCol) {
      if (columnIndex > inputCol) {
        p->_variableColumns[variable] = columnIndex + 1;
      } else {
        p->_variableColumns[variable] = columnIndex + 2;
      }
      p->_resultWidth++;
    }
  }
  return p;
}

// _____________________________________________________________________________
bool TransitivePath::isBound() const {
  return _leftSideTree != nullptr || _rightSideTree != nullptr || !_leftIsVar ||
         !_rightIsVar;
}
