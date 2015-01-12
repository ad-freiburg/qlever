// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <cassert>
#include <sstream>
#include <vector>
#include <iostream>
#include "./QueryGraph.h"

using std::ostringstream;
using std::vector;

// _____________________________________________________________________________
void QueryGraph::addNode(const string& label) {
  if (_nodeIds.count(label) == 0) {
    _adjLists.push_back(vector<QueryGraph::Edge>{});
    _nodePayloads.emplace_back(Node(label));
    _nodeMap[_adjLists.size() - 1] = &_nodePayloads.back();
    _nodeIds[label] = _adjLists.size() - 1;
  }
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
  for (int i = 0; i < _adjLists.size(); ++i) {
    if (_adjLists[i].size() == 1) {
      ret.push_back(i);
    }
  }
  return ret;
}

// _____________________________________________________________________________
void QueryGraph::collapseNode(size_t u) {
  assert(u < _adjLists.size());
  // Ensure there is exactly one connected node, collapse is only allow for
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
  getNode(v)->consume(getNode(u));
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
  os << '{' << _targetNodeId << ',' << _label  << (_reversed ? "_r}" : "}");
  return os.str();
}

// _____________________________________________________________________________
string QueryGraph::asString() {
  ostringstream os;
  for (size_t i = 0; i < _adjLists.size(); ++i) {
    os << getNode(i)->asString() << ':';
    for (size_t j = 0; j < _adjLists[i].size(); ++j) {
      os << _adjLists[i][j].asString();
      if (j < _adjLists[i].size() - 1) {os << ",";}
    }
    if (i < _adjLists.size() - 1) {os << '\n';}
  }
  return os.str();
}

// _____________________________________________________________________________
void QueryGraph::Node::consume(QueryGraph::Node* other) {

}

// _____________________________________________________________________________
void QueryGraph::createFromParsedQuery(const ParsedQuery& pq) {
  for (size_t i = 0; i < pq._whereClauseTriples.size(); ++i) {
    addNode(pq._whereClauseTriples[i]._s);
    addNode(pq._whereClauseTriples[i]._o);
    addEdge(getNodeId(pq._whereClauseTriples[i]._s),
        getNodeId(pq._whereClauseTriples[i]._o),
        pq._whereClauseTriples[i]._p);
  }
}

// _____________________________________________________________________________
QueryExecutionTree QueryGraph::collapseAndCreateExecutionTree() {

}