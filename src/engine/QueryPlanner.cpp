// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#include "./QueryPlanner.h"

// _____________________________________________________________________________
QueryPlanner::QueryPlanner(QueryExecutionContext *qec) : _qec(qec) { }

// _____________________________________________________________________________
QueryExecutionTree QueryPlanner::createExecutionTree(
    const ParsedQuery& pq) const {

  std::unordered_map<string, vector<SparqlTriple>> varToTrip;
  std::unordered_set<string> contextVars;
  getVarTripleMap(pq, varToTrip, contextVars);

  //  std::unordered_map contextVarRoots;
  // TODO: Optimization for join s.th. like name is delayed.
  // Get context roots (how?) and
  // getContextVarRoots(varToTrip, contextVars, contextVarRoots);


  // Go through the map and replace triples with operations when possible.

  // Triples with only one var are trivial.

  // Multiple operations for a variable can be combined
  // into joins with one join as root operation for the variable.

  // Now left: triples with more than one var ()

  return QueryExecutionTree(nullptr);
}

// _____________________________________________________________________________
void QueryPlanner::getVarTripleMap(
    const ParsedQuery& pq,
    unordered_map<string, vector<SparqlTriple>>& varToTrip,
    unordered_set<string>& contextVars) const {
  for (auto& t: pq._whereClauseTriples) {
    if (isVariable(t._s)) {
      varToTrip[t._s].push_back(t);
    }
    if (isVariable(t._p)) {
      varToTrip[t._p].push_back(t);
    }
    if (isVariable(t._o)) {
      varToTrip[t._o].push_back(t);
    }

    if (t._p == IN_CONTEXT_RELATION) {
      if (isVariable(t._s) || isWords(t._o)) {
        contextVars.insert(t._s);
      }
      if (isVariable(t._o) || isWords(t._s)) {
        contextVars.insert(t._o);
      }
    }
  }
}

// _____________________________________________________________________________
bool QueryPlanner::isVariable(const string& elem) const {
  return ad_utility::startsWith(elem, "?");
}

// _____________________________________________________________________________
bool QueryPlanner::isWords(const string& elem) const {
  return !isVariable(elem) && elem.size() > 0 && elem[0] != '<';
}
