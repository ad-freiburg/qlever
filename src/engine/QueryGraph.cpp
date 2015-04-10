// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <cassert>
#include <sstream>
#include <vector>
#include <iostream>
#include <limits>
#include "./QueryGraph.h"
#include "Filter.h"

using std::ostringstream;
using std::vector;


// _____________________________________________________________________________
QueryGraph::QueryGraph() :
    _qec(nullptr), _nodeMap(), _nodeIds(), _adjLists(), _nodePayloads(),
    _selectVariables(), _query(), _executionTree(nullptr), _nofTerminals(0) {
}

// _____________________________________________________________________________
QueryGraph::QueryGraph(QueryExecutionContext* qec) :
    _qec(qec), _nodeMap(), _nodeIds(), _adjLists(), _nodePayloads(),
    _selectVariables(), _query(), _executionTree(nullptr), _nofTerminals(0) {
}

// _____________________________________________________________________________
QueryGraph::~QueryGraph() {
  delete _executionTree;
}

// _____________________________________________________________________________
QueryGraph::QueryGraph(const QueryGraph& other) :
    _qec(other._qec), _nodeMap(other._nodeMap), _nodeIds(other._nodeIds),
    _adjLists(other._adjLists),
    _nodePayloads(other._nodePayloads),
    _selectVariables(other._selectVariables),
    _query(other._query),
    _nofTerminals(other._nofTerminals) {
  if (other._executionTree) {
    _executionTree = new QueryExecutionTree(*other._executionTree);
  } else {
    _executionTree = nullptr;
  }
}

// _____________________________________________________________________________
QueryGraph& QueryGraph::operator=(const QueryGraph& other) {
  _qec = other._qec;
  _adjLists = other._adjLists;
  _nodePayloads = other._nodePayloads;
  _selectVariables = other._selectVariables;
  _query = other._query;
  if (other._executionTree) {
    _executionTree = new QueryExecutionTree(*other._executionTree);
  } else {
    _executionTree = nullptr;
  }
  _nofTerminals = other._nofTerminals;
  return *this;
}

// _____________________________________________________________________________
string QueryGraph::addNode(const string& label) {
  string internalLabel = label;
  if (label.size() == 0) {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "Not supporting blank nodes.");
  }
  if (label[0] != '?') {
    std::ostringstream os;
    os << label << "_" << _nofTerminals++;
    internalLabel = os.str();
  }
  if (_nodeIds.count(internalLabel) == 0) {
    _adjLists.push_back(vector<QueryGraph::Edge>{});
    _nodePayloads.push_back(Node(_qec, internalLabel));
    _nodeMap[_adjLists.size() - 1] = &_nodePayloads.back();
    _nodeIds[internalLabel] = _adjLists.size() - 1;
  }
  return internalLabel;
}

// _____________________________________________________________________________
void QueryGraph::addEdge(size_t u, size_t v, const string& label) {
  assert(u < _adjLists.size());
  assert(v < _adjLists.size());
  _adjLists[u].emplace_back(Edge(v, label));
  _adjLists[v].emplace_back(Edge(u, label, true));
}


// _____________________________________________________________________________
size_t QueryGraph::getNodeId(const string& label) const {
  assert(_nodeIds.count(label) > 0);
  return _nodeIds.find(label)->second;
}

// _____________________________________________________________________________
QueryGraph::Node* QueryGraph::getNode(size_t nodeId) {
  if (_nodeMap.count(nodeId) > 0) {
    return _nodeMap[nodeId];
  } else {
    return nullptr;
  }
}

// _____________________________________________________________________________
QueryGraph::Node* QueryGraph::getNode(const string& label) {
  if (_nodeIds.count(label) > 0) {
    return _nodeMap[_nodeIds[label]];
  } else {
    return nullptr;
  }
}

// _____________________________________________________________________________
vector<size_t> QueryGraph::getNodesWithDegreeOne() const {
  vector<size_t> ret;
  for (size_t i = 0; i < _adjLists.size(); ++i) {
    if (_adjLists[i].size() == 1) {
      ret.push_back(i);
    }
  }
  return ret;
}

// _____________________________________________________________________________
void QueryGraph::collapseNode(size_t u) {
  assert(u < _adjLists.size());
  // Ensure there is exactly one connected node, collapse is only allowed for
  // nodes with degree 1.
  assert(_adjLists[u].size() == 1);
  size_t v = _adjLists[u][0]._targetNodeId;
  // Remove edge towards u
  vector<Edge> newList;
  for (size_t j = 0; j < _adjLists[v].size(); ++j) {
    if (_adjLists[v][j]._targetNodeId != u) {
      newList.push_back(_adjLists[v][j]);
    }
  }
  _adjLists[v] = newList;
  // Add the according operation / do the merging.
  getNode(v)->consume(getNode(u), _adjLists[u][0]);
  // Remove edges from this
  _adjLists[u].clear();
};

// _____________________________________________________________________________
string QueryGraph::Node::asString() const {
  ostringstream os;
  os << '(' << _label << ')';
  return os.str();
}

// _____________________________________________________________________________
string QueryGraph::Edge::asString() const {
  ostringstream os;
  os << '{' << _targetNodeId << ',' << _label << (_reversed ? "_r}" : "}");
  return os.str();
}

// _____________________________________________________________________________
string QueryGraph::asString() {
  ostringstream os;
  for (size_t i = 0; i < _adjLists.size(); ++i) {
    os << getNode(i)->asString() << ':';
    for (size_t j = 0; j < _adjLists[i].size(); ++j) {
      os << _adjLists[i][j].asString();
      if (j < _adjLists[i].size() - 1) { os << ","; }
    }
    if (i < _adjLists.size() - 1) { os << '\n'; }
  }
  return os.str();
}

// _____________________________________________________________________________
void QueryGraph::Node::consume(QueryGraph::Node* other,
                               const QueryGraph::Edge& edge) {

  QueryExecutionTree addedSubtree(_qec);
  if (other->getConsumedOperations().isEmpty()) {
    if (!other->isVariableNode()) {
      // Case: Other has no subtree result and a fixed obj (or subj).
      if (edge._reversed) {
        IndexScan is(_qec, IndexScan::POS_BOUND_O);
        is.setObject(other->_label);
        is.setPredicate(edge._label);
        addedSubtree.setOperation(QueryExecutionTree::SCAN, &is);
        addedSubtree.setVariableColumn(_label, 0);
      } else {
        IndexScan is(_qec, IndexScan::PSO_BOUND_S);
        is.setSubject(other->_label);
        is.setPredicate(edge._label);
        addedSubtree.setOperation(QueryExecutionTree::SCAN, &is);
        addedSubtree.setVariableColumn(_label, 0);
      }
    } else {
      // Case: Other has no subtree result but a variable
      if (edge._reversed) {
        IndexScan is(_qec, IndexScan::PSO_FREE_S);  // Ordered by S over O
        is.setPredicate(edge._label);
        addedSubtree.setOperation(QueryExecutionTree::SCAN, &is);
        addedSubtree.setVariableColumn(_label, 0);
        addedSubtree.setVariableColumn(other->_label, 1);
      } else {
        IndexScan is(_qec, IndexScan::POS_FREE_O);  // Ordered by O over S
        is.setPredicate(edge._label);
        addedSubtree.setOperation(QueryExecutionTree::SCAN, &is);
        addedSubtree.setVariableColumn(_label, 0);
        addedSubtree.setVariableColumn(other->_label, 1);
      }
    }
  } else {
    // Case: Other has a subtree result, must be a variable then
    if (edge._reversed) {
      QueryExecutionTree nestedTree(_qec);
      IndexScan is(_qec, IndexScan::POS_FREE_O);  // Ordered by O (join)
      is.setPredicate(edge._label);
      nestedTree.setOperation(QueryExecutionTree::SCAN, &is);
      nestedTree.setVariableColumn(other->_label, 0);
      nestedTree.setVariableColumn(_label, 1);
      Join join(_qec, nestedTree, other->getConsumedOperations(), 0,
                other->_consumedOperations.getVariableColumn(other->_label));
      addedSubtree.setOperation(QueryExecutionTree::JOIN, &join);
      addedSubtree.setVariableColumns(join.getVariableColumns());
    } else {
      QueryExecutionTree nestedTree(_qec);
      IndexScan is(_qec, IndexScan::PSO_FREE_S);  // Ordered by S
      is.setPredicate(edge._label);
      nestedTree.setOperation(QueryExecutionTree::SCAN, &is);
      nestedTree.setVariableColumn(other->_label, 0);
      nestedTree.setVariableColumn(_label, 1);
      nestedTree.setOperation(QueryExecutionTree::SCAN, &is);
      Join join(_qec, nestedTree, other->getConsumedOperations(), 0,
                other->_consumedOperations.getVariableColumn(other->_label));
      addedSubtree.setOperation(QueryExecutionTree::JOIN, &join);
      addedSubtree.setVariableColumns(join.getVariableColumns());
    }
  }

  if (_consumedOperations.isEmpty()) {
    _consumedOperations = addedSubtree;
  } else {
    if (addedSubtree.getVariableColumn(_label)
        == addedSubtree.resultSortedOn()) {
      Join join(_qec, _consumedOperations,
                addedSubtree,
                _consumedOperations.getVariableColumn(_label),
                addedSubtree.getVariableColumn(_label));
      _consumedOperations.setOperation(QueryExecutionTree::JOIN, &join);
      _consumedOperations.setVariableColumns(join.getVariableColumns());
    } else {
      QueryExecutionTree sortedSubtree(_qec);
      Sort sort(_qec, addedSubtree, addedSubtree.getVariableColumn(_label));
      sortedSubtree.setOperation(QueryExecutionTree::SORT, &sort);
      sortedSubtree.setVariableColumns(addedSubtree.getVariableColumnMap());
      Join join(_qec, _consumedOperations,
                sortedSubtree,
                _consumedOperations.getVariableColumn(_label),
                sortedSubtree.getVariableColumn(_label));
      _consumedOperations.setOperation(QueryExecutionTree::JOIN, &join);
      _consumedOperations.setVariableColumns(join.getVariableColumns());
    }
  }
}

// _____________________________________________________________________________
size_t QueryGraph::Node::expectedCardinality(
    size_t remainingRelationCardinality) {
  if (_expectedCardinality == std::numeric_limits<size_t>::max()) {
    if (ad_utility::startsWith(_label, "?")) {
      if (_consumedOperations.getType() != QueryExecutionTree::UNDEFINED) {
        if (_qec) {
          _expectedCardinality = _consumedOperations.getResult().size()
                                 * remainingRelationCardinality;
        } else {
          _expectedCardinality = remainingRelationCardinality * 2;
        };
      } else {
        _expectedCardinality = remainingRelationCardinality;
      }
    } else {
      _expectedCardinality = 1 + remainingRelationCardinality / 4;
    }
  }
  return _expectedCardinality;
}

// _____________________________________________________________________________
void QueryGraph::createFromParsedQuery(const ParsedQuery& pq) {
  for (size_t i = 0; i < pq._whereClauseTriples.size(); ++i) {
    string s = addNode(pq._whereClauseTriples[i]._s);
    string o = addNode(pq._whereClauseTriples[i]._o);
    addEdge(getNodeId(s), getNodeId(o), pq._whereClauseTriples[i]._p);
  }
  for (size_t i = 0; i < pq._selectedVariables.size(); ++i) {
    _selectVariables.insert(pq._selectedVariables[i]);
  }
  _query = pq;
}

// _____________________________________________________________________________
QueryGraph::Node* QueryGraph::collapseAndCreateExecutionTree() {
  auto deg1Nodes = getNodesWithDegreeOne();
  if (deg1Nodes.size() == 0) {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "No support for non-tree queries, yet.")
  }
  size_t lastUpdatedNode = std::numeric_limits<size_t>::max();
  while (deg1Nodes.size() > 0) {
    // Find the one with the minimum expected cardinality
    size_t relSize = _qec ? _qec->getIndex().relationCardinality(
        _adjLists[deg1Nodes[0]][0]._label) : 10;
    size_t minEC = getNode(deg1Nodes[0])->expectedCardinality(relSize);
    size_t minECIndex = deg1Nodes[0];
    for (size_t i = 1; i < deg1Nodes.size(); ++i) {
      relSize = _qec ? _qec->getIndex().relationCardinality(
          _adjLists[deg1Nodes[i]][0]._label) : 10;
      size_t ec = getNode(deg1Nodes[i])->expectedCardinality(relSize);
      if (ec < minEC ||
          (ec == minEC &&
           _selectVariables.count(getNode(deg1Nodes[i])->_label) == 0)) {
        minEC = ec;
        minECIndex = deg1Nodes[i];
      }
    }
    // Collapse this node
    lastUpdatedNode = _adjLists[minECIndex][0]._targetNodeId;
    collapseNode(minECIndex);
    deg1Nodes = getNodesWithDegreeOne();
  }
  assert(lastUpdatedNode != std::numeric_limits<size_t>::max());
  return (getNode(lastUpdatedNode));
}

// _____________________________________________________________________________
QueryGraph::Node::Node(QueryExecutionContext* qec,
                       const string& label) :
    _label(label),
    _qec(qec),
    _expectedCardinality(std::numeric_limits<size_t>::max()),
    _consumedOperations(QueryExecutionTree(qec)) {
}

// _____________________________________________________________________________
QueryGraph::Node::~Node() {

}

// _____________________________________________________________________________
QueryGraph::Node::Node(const QueryGraph::Node& other) :
    _label(other._label),
    _qec(other._qec),
    _expectedCardinality(other._expectedCardinality),
    _consumedOperations(other._consumedOperations) {

}

// _____________________________________________________________________________
QueryGraph::Node& QueryGraph::Node::operator=(QueryGraph::Node const& other) {
  _label = other._label;
  _qec = other._qec;
  _expectedCardinality = other._expectedCardinality;
  _consumedOperations = other._consumedOperations;
  return *this;
}

// _____________________________________________________________________________
const QueryExecutionTree& QueryGraph::getExecutionTree() {
  if (!_executionTree) {
    _executionTree = new QueryExecutionTree(_qec);
    Node* root = collapseAndCreateExecutionTree();
    QueryExecutionTree interm(_qec);
    applySolutionModifiers(root->getConsumedOperations(), &interm);
    applyFilters(interm, _executionTree);
  }
  LOG(TRACE) << "Final execution tree: " << _executionTree->asString() << '\n';
  return *_executionTree;
}

// _____________________________________________________________________________
void QueryGraph::applySolutionModifiers(const QueryExecutionTree& treeSoFar,
                                        QueryExecutionTree* finalTree) const {
  if (_query._orderBy.size() > 0) {
    if (_query._orderBy.size() == 1 && !_query._orderBy[0]._desc) {
      size_t orderCol = treeSoFar.getVariableColumn(_query._orderBy[0]._key);
      if (orderCol == treeSoFar.resultSortedOn()) {
        // Already sorted perfectly
        *finalTree = treeSoFar;
      } else {
        finalTree->setVariableColumns(treeSoFar.getVariableColumnMap());
        Sort sort(_qec, treeSoFar, orderCol);
        finalTree->setOperation(QueryExecutionTree::SORT, &sort);
      }
    } else {
      finalTree->setVariableColumns(treeSoFar.getVariableColumnMap());
      vector<pair<size_t, bool>> sortIndices;
      for (auto& ord : _query._orderBy) {
        sortIndices.emplace_back(
            pair<size_t, bool>{treeSoFar.getVariableColumn(ord._key), ord
                ._desc});
      }
      OrderBy ob(_qec, treeSoFar, sortIndices);
      finalTree->setOperation(QueryExecutionTree::ORDER_BY, &ob);
    }
  } else {
    *finalTree = treeSoFar;
  }
  // TODO: Keyword REDUCED is ignored. Which is legit but not optimal.
  if (_query._distinct) {
    // TODO: Add support!
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "DISTINCT not "
        "supported, yet!");
  }
}

// _____________________________________________________________________________
void QueryGraph::applyFilters(const QueryExecutionTree& treeSoFar,
                              QueryExecutionTree* treeAfter) {
  *treeAfter = treeSoFar;
  for (auto& f : _query._filters) {
    QueryExecutionTree lastTree(*treeAfter);
    Filter filter(_qec, lastTree, f._type, treeSoFar.getVariableColumn(f._lhs),
                  treeSoFar.getVariableColumn(f._rhs));
    treeAfter->setOperation(QueryExecutionTree::FILTER, &filter);
  }
}
