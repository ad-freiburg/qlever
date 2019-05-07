// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "TransitivePath.h"

#include <limits>

#include "CallFixedSize.h"

// _____________________________________________________________________________
TransitivePath::TransitivePath(QueryExecutionContext* qec,
                               std::shared_ptr<QueryExecutionTree> child,
                               size_t leftCol, size_t rightCol, size_t minDist,
                               size_t maxDist)
    : Operation(qec),
      _subtree(child),
      _leftCol(leftCol),
      _rightCol(rightCol),
      _minDist(minDist),
      _maxDist(maxDist) {}

// _____________________________________________________________________________
std::string TransitivePath::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "TRANSITIVE left " << _leftCol << " right " << _rightCol << " minDist "
     << _minDist << " maxDist " << _maxDist << "\n";
  os << _subtree->asString(indent) << "\n";
  return os.str();
}

// _____________________________________________________________________________
std::string TransitivePath::getDescriptor() const {
  std::ostringstream os;
  os << "TRANSITIVE left " << _leftCol << " right " << _rightCol << " minDist "
     << _minDist << " maxDist " << _maxDist;
  return os.str();
}

// _____________________________________________________________________________
size_t TransitivePath::getResultWidth() const { return 2; }

// _____________________________________________________________________________
vector<size_t> TransitivePath::resultSortedOn() const {
  const std::vector<size_t>& subSortedOn =
      _subtree->getRootOperation()->getResultSortedOn();
  if (subSortedOn.size() > 0 && subSortedOn[0] == _leftCol) {
    // This operation preserves the order of the _leftCol of the subtree.
    return {0};
  }
  return {};
}

// _____________________________________________________________________________
ad_utility::HashMap<std::string, size_t> TransitivePath::getVariableColumns()
    const {
  ad_utility::HashMap<std::string, size_t> map;
  ad_utility::HashMap<std::string, size_t> subMap =
      _subtree->getVariableColumns();
  for (const std::pair<std::string, size_t>& p : subMap) {
    if (p.second == _leftCol) {
      map[p.first] = 0;
    } else if (p.second == _rightCol) {
      map[p.first] = 1;
    }
  }
  return map;
}

// _____________________________________________________________________________
void TransitivePath::setTextLimit(size_t limit) {
  _subtree->setTextLimit(limit);
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
  // TODO(Florian): this is not necessarily a good estimator
  return _subtree->getSizeEstimate() / _subtree->getMultiplicity(_leftCol);
}

// _____________________________________________________________________________
size_t TransitivePath::getCostEstimate() { return getSizeEstimate(); }

// _____________________________________________________________________________
template <int SUB_WIDTH>
void TransitivePath::computeTransitivePath(IdTable* dynRes,
                                           const IdTable& dynSub,
                                           size_t leftCol, size_t rightCol,
                                           size_t minDist, size_t maxDist) {
  using Map = ad_utility::HashMap<Id, std::shared_ptr<std::vector<size_t>>>;
  using MapIt = Map::iterator;

  const IdTableStatic<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  IdTableStatic<2> res = dynRes->moveToStatic<2>();

  // Used to map entries in the left column to entries they have connection with
  Map edges;
  // All nodes on the graph from which an edge leads to another node
  std::vector<Id> nodes;

  // initialize the map from the subresult
  for (size_t i = 0; i < sub.size(); i++) {
    size_t l = sub(i, leftCol);
    size_t r = sub(i, rightCol);
    MapIt it = edges.find(l);
    if (it == edges.end()) {
      nodes.push_back(l);
      edges[l] = std::make_shared<std::vector<size_t>>(1, r);
    } else {
      // If r is not in the vector insert it
      if (std::find(it->second->begin(), it->second->end(), r) ==
          it->second->end()) {
        it->second->push_back(r);
      }
    }
  }

  // For every node do a dfs on the graph

  // Stores nodes we already have a path to. This avoids cycles.
  ad_utility::HashSet<Id> marks;

  // The stack used to store the dfs' progress
  std::vector<size_t> positions;

  // Used to store all edges leading away from a node for every level.
  // Reduces access to the hashmap, and is safe as the map will not
  // be modified after this point.
  std::vector<std::shared_ptr<const std::vector<Id>>> edgeCache;

  for (size_t i = 0; i < nodes.size(); i++) {
    MapIt rootEdges = edges.find(nodes[i]);
    if (rootEdges != edges.end()) {
      positions.push_back(0);
      edgeCache.push_back(rootEdges->second);
    }
    if (minDist == 0) {
      // add the node itself to its result
      res.push_back({nodes[i], nodes[i]});
      marks.insert(nodes[i]);
    }

    // While we have not found the entire transitive hull and have not reached
    // the max step limit
    while (!positions.empty()) {
      size_t stackIndex = positions.size() - 1;
      // Process the next child of the node at the top of the stack
      size_t pos = positions[stackIndex];
      const std::vector<size_t>* nodeEdges = edgeCache.back().get();

      if (pos >= nodeEdges->size()) {
        // We finished processing this node
        positions.pop_back();
        edgeCache.pop_back();
        continue;
      }

      size_t child = (*nodeEdges)[pos];
      size_t childDepth = positions.size();
      if (childDepth <= maxDist && marks.count(child) == 0) {
        // process the child
        if (childDepth >= minDist) {
          marks.insert(child);
          res.push_back({nodes[i], child});
        }
        // Add the child to the stack
        MapIt it = edges.find(child);
        if (it != edges.end()) {
          positions.push_back(0);
          edgeCache.push_back(it->second);
        }
      }
      ++positions[stackIndex];
    }

    if (i + 1 < nodes.size()) {
      // reset everything for the next iteration
      marks.clear();
    }
  }

  *dynRes = res.moveToDynamic();
}

// _____________________________________________________________________________
void TransitivePath::computeResult(ResultTable* result) {
  LOG(DEBUG) << "TransitivePath result computation..." << std::endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "TransitivePath subresult computation done." << std::endl;

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());

  result->_sortedBy = resultSortedOn();
  result->_resultTypes.push_back(subRes->getResultType(_leftCol));
  result->_resultTypes.push_back(subRes->getResultType(_rightCol));
  result->_data.setCols(getResultWidth());

  int subWidth = subRes->_data.cols();
  CALL_FIXED_SIZE_1(subWidth, computeTransitivePath, &result->_data,
                    subRes->_data, _leftCol, _rightCol, _minDist, _maxDist);

  LOG(DEBUG) << "TransitivePath result computation done." << std::endl;
}
