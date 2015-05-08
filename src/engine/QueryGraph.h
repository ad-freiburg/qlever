// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <utility>
#include <limits>
#include "../parser/ParsedQuery.h"
#include "./QueryExecutionTree.h"

using std::string;
using std::vector;
using std::unordered_map;
using std::unordered_set;
using std::list;
using std::pair;

// -------------------------------------
// Thoughts on planning query execution:
// -------------------------------------

// Each triple leads to a ResultTable
// Each variable repetition is a join
// Planned joins determine the ordering required from ResultTables.
// TODO: Cyclic queries are delayed for now
// TODO: Variables for predicates are delayed for now.
// TODO: Text is delayed for now.

// Given the above, there are two kinds of triple:
// 1. One variable - the ResultTable has 1 column, the ordering required will
// always be by the variable column and it can be read directly from the
// P - otherFixed - var permutation
// 2. Two variables - The Result Table has two columns, the ordering
// is determined by the join it is needed in and can be read directly
// from the according permutation

// The simple join ordering can use exact numbers for triples of kind 1
// from the actual ResultTables (because it is sure how they have to be read)
// and the size for all ResultTables for triples of kind 2 is the size
// of the relation and feasible to pre-compute.
// Result cardinality of joins is not know and for now no statistic are used.
// We just predict the size of a full cartesian product as very simple
// heuristic.

// All joins for a variable always have to be done at the same level in
// the tree, other variables will require re-ordering.
// Therefore the QueryGraph will have nodes for variables.
// Variable with more than one occurrence have degree > 1,
// Leaves will be triples of type 1 and triples of type 2 where
// One of the variables only occurs once.
// An internal node will have out degree according to the number of
// occurrences of its variable and edges (labeled with a relation name)
// represent joins done at the level.

// In conclusion, I want to build a query graph where S and O are nodes,
// and predicates are edge labels. Literals, URIs, etc and
// variables with only one occurrence will lead to nodes with degree 1.
// At nodes with degree > 1, a number of joins will happen.
// The order can be determined by the heuristic described above.
// In order to find an execution plan, take the node with degree 1,
// that has the lowest expected cardinality, describe its result by
// operations (with is either a trivial scan or involves a number of joins)
// and collapse it's result into the neighbor which will get a lower degree.
// Continue until only one node is left.
// If there is no node with degree 1, there is a cycle which can be
// resolved by temporarily removing an edge and using it later to filter
// the result. TODO: delay this for now.

// Ordering joins inside a node can be done smallest-first, again.

// Columns always correspond to variables and this has to be remembered
// throughout join operations.
// Intermediate results will feature only a limited set of variables.
// Projections can be done in the end (leads to lower
// availability of optimized joins) or in between if columns are not needed
// in the select clause.

// A simple query graph.
// Nodes correspond to variables or URIs / literals, edges correspond to
// triples / relations.
// The graph can collapse nodes with degree one, whereas the nodes data (i.e.
// operations to compute a matching result table) is transformed into the
// parent. Finally only one node is left with a tree of operations beneath.
class QueryGraph {
public:

  struct Edge {
  public:
    Edge(size_t targetNode, const string& label, bool reversed = false) :
        _targetNodeId(targetNode), _label(label), _reversed(reversed) {
    }

    size_t _targetNodeId;
    string _label;
    bool _reversed;

    string asString() const;
  };

  struct Node {
  public:
    Node(QueryExecutionContext* qec, const string& label);

    virtual ~Node();

    explicit Node(const Node& other);

    Node& operator=(const Node& other);

    string _label;
    bool _isContextNode;

    // Consumes another node. i.e. it includes the calculations made for that
    // other node to the calculations made for this node.
    // The edge between the two nodes always yields a relation than has to be
    // accessed during every consumption.
    // Updates the expected cardinality and the list of subtree results that
    // have to be joined on the columns that represent the node's label.
    void consume(Node* other, const QueryGraph::Edge& edge);

    // Does the actual consumption. Does not handle joiningg with previously
    // consumed subtrees, yet.
    QueryExecutionTree consumeIntoSubtree(Node* other,
                                          const QueryGraph::Edge& edge);

    // Special case: relation is in-context
    QueryExecutionTree consumeIcIntoSubtree(Node* other,
                                            const QueryGraph::Edge& edge);
    // Special case: relation is has-contexts
    QueryExecutionTree consumeHcIntoSubtree(Node* other,
                                            const QueryGraph::Edge& edge);

    string asString() const;

    // Get the expectedCardinality.
    // Lazily computes the value once.
    size_t expectedCardinality(size_t remainingRelationCardinality);

    const QueryExecutionTree& getConsumedOperations() const {
      return _consumedOperations;
    }

    bool isVariableNode() const {
      return ad_utility::startsWith(_label, "?");
    }

    void useContextRootOperation();

  private:
    QueryExecutionContext* _qec;
    size_t _expectedCardinality;
    QueryExecutionTree _consumedOperations;
    vector<pair<QueryExecutionTree, size_t>> _storedOperations;
    string _storedWords;
  };

  QueryGraph();

  explicit QueryGraph(QueryExecutionContext* qec);

  ~QueryGraph();

  QueryGraph(const QueryGraph& other);

  QueryGraph& operator=(const QueryGraph& other);

  void createFromParsedQuery(const ParsedQuery& pq);

  string asString();

  const QueryExecutionTree& getExecutionTree();

  static unordered_map<string, size_t> createVariableColumnsMapForTextOperation(
      const string& contextVar,
      const string& entityVar,
      const vector<pair<QueryExecutionTree, size_t>>& subtrees);

private:
  QueryExecutionContext* _qec;  // No ownership, don't delete.
  unordered_map<size_t, Node*> _nodeMap;
  unordered_map<string, size_t> _nodeIds;
  vector<vector<Edge>> _adjLists;
  list<Node> _nodePayloads;
  unordered_set<string> _selectVariables;
  ParsedQuery _query;
  QueryExecutionTree* _executionTree;  // Ownership, new when created. Delete!
  size_t _nofTerminals;

  void collapseNode(size_t u);

  Node* collapseAndCreateExecutionTree();

  string addNode(const string& label);

  void addEdge(size_t u, size_t v, const string& label);

  size_t getNodeId(const string& label) const;

  Node* getNode(size_t nodeId);

  Node* getNode(const string& label);

  vector<size_t> getNodesWithDegreeOne() const;

  void applyFilters(const QueryExecutionTree& treeSoFar,
                    QueryExecutionTree* treeAfter);

  void applySolutionModifiers(const QueryExecutionTree& treeSoFar,
                              QueryExecutionTree* finalTree) const;


  friend class QueryGraphTest_testAddNode_Test;

  friend class QueryGraphTest_testAddEdge_Test;

  friend class QueryGraphTest_testCollapseNode_Test;

  friend class QueryGraphTest_testCreate_Test;

  friend class QueryGraphTest_testCollapseByHand_Test;

  friend class QueryGraphTest_testCollapseAndCreateExecutionTree_Test;
};
