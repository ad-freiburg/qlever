// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>
#include "../parser/ParsedQuery.h"
#include "QueryExecutionTree.h"

using std::vector;

class QueryPlanner {
  public:
    explicit QueryPlanner(QueryExecutionContext *qec);

    QueryExecutionTree createExecutionTree(const ParsedQuery& pq) const;

    class TripleGraph {
      public:

        class Node {
          public:
            Node(size_t id, const SparqlTriple& t) : _id(id), _triple(t) {
              if (isVariable(t._s)) { _variables.insert(t._s); }
              if (isVariable(t._p)) { _variables.insert(t._p); }
              if (isVariable(t._o)) { _variables.insert(t._o); }
            }

            size_t _id;
            SparqlTriple _triple;
            std::unordered_set<string> _variables;
        };

        string asString() const;

        vector<vector<size_t>> _adjLists;
        std::unordered_map<size_t, Node *> _nodeMap;
        std::list<Node> _nodeStorage;

      vector<pair<TripleGraph, vector<SparqlFilter>>> split(
          const vector<SparqlFilter>& origFilters) const;
    };

    class SubtreePlan {
      public:
        explicit SubtreePlan(QueryExecutionContext *qec) : _qet(qec) { }

        QueryExecutionTree _qet;
        std::unordered_set<size_t> _idsOfIncludedNodes;
        std::unordered_set<size_t> _idsOfIncludedFilters;

        size_t getCostEstimate() const;

        size_t getSizeEstimate() const;
    };

    TripleGraph createTripleGraph(const ParsedQuery& query) const;

  private:
    QueryExecutionContext *_qec;

    static bool isVariable(const string& elem);

    static bool isWords(const string& elem);

    void getVarTripleMap(const ParsedQuery& pq,
                         unordered_map<string, vector<SparqlTriple>>& varToTrip,
                         unordered_set<string>& contextVars) const;

    vector<SubtreePlan> seedWithScans(const TripleGraph& tg) const;

    vector<SubtreePlan> merge(const vector<SubtreePlan>& a,
                              const vector<SubtreePlan>& b,
                              const TripleGraph& tg) const;

    vector<SubtreePlan> getOrderByRow(
        const ParsedQuery& pq,
        const vector<vector<SubtreePlan>>& dpTab) const;

    bool connected(const SubtreePlan& a, const SubtreePlan& b,
                   const TripleGraph& graph) const;

    vector<array<size_t, 2>> getJoinColumns(
        const SubtreePlan& a, const SubtreePlan& b) const;

    string getPruningKey(const SubtreePlan& plan, size_t orderedOnCol) const;

    void applyFiltersIfPossible(vector<SubtreePlan>& row,
                                const vector<SparqlFilter>& filters) const;

    vector<vector<SubtreePlan>> fillDpTab(const TripleGraph& graph,
                                          const vector<SparqlFilter>& fs) const;
};

