// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <list>
#include "./QueryExecutionTree.h"
#include "../parser/ParsedQuery.h"

using std::string;
using std::vector;
using std::unordered_map;
using std::list;

// A simple query graph.
// Nodes correspond to variables or URIs / literals, edges correspond to
// triples / relations.
// The graph can collapse nodes with degree one, whereas the nodes data (i.e.
// operations to compute a matching result table) is transformed into the
// parent. Finally only one node is left with a tree of operations beneath.
class QueryGraph {
public:

  struct Node {
  public:
    Node(const string& label) : _label(label) {}
    string _label;
    void consume(Node* other);

    string asString() const;
  };

  struct Edge {
  public:
    Edge(size_t targetNode, const string& label, bool reversed = false) :
        _targetNodeId(targetNode), _label(label), _reversed(reversed) {}
    size_t _targetNodeId;
    string _label;
    bool _reversed;

    string asString() const;
  };

  void createFromParsedQuery(const ParsedQuery& pq);
  void addNode(const string& label);
  void addEdge(size_t u, size_t v, const string& label);

  string asString();

  size_t getNodeId(const string& label) const;

  Node* getNode(size_t nodeId);
  Node* getNode(const string& label);
  vector<size_t> getNodesWithDegreeOne() const;

  void collapseNode(size_t u);
  QueryExecutionTree collapseAndCreateExecutionTree();

private:
  unordered_map<size_t, Node*> _nodeMap;
  unordered_map<string, size_t> _nodeIds;
  vector<vector<Edge>> _adjLists;
  list<Node> _nodePayloads;

};
