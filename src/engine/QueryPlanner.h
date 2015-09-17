// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include "../parser/ParsedQuery.h"
#include "QueryExecutionTree.h"

class QueryPlanner {
public:
  explicit QueryPlanner(QueryExecutionContext *qec);

  QueryExecutionTree createExecutionTree(const ParsedQuery& pq) const;

private:
  QueryExecutionContext *_qec;

  bool isVariable(const string& elem) const;

  bool isWords(const string& elem) const;

  void getVarTripleMap(const ParsedQuery& pq,
                       unordered_map<string, vector<SparqlTriple>>& varToTrip,
                       unordered_set<string>& contextVars) const;
};

