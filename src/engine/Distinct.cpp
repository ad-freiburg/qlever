// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Distinct.h"

#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/QueryExecutionTree.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
size_t Distinct::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
Distinct::Distinct(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   const vector<ColumnIndex>& keepIndices)
    : Operation(qec), _subtree(subtree), _keepIndices(keepIndices) {}

// _____________________________________________________________________________
string Distinct::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "Distinct " << _subtree->getCacheKey();
  return std::move(os).str();
}

// _____________________________________________________________________________
string Distinct::getDescriptor() const { return "Distinct"; }

// _____________________________________________________________________________
VariableToColumnMap Distinct::computeVariableToColumnMap() const {
  return _subtree->getVariableColumns();
}

// _____________________________________________________________________________
Result Distinct::computeResult([[maybe_unused]] bool requestLaziness) {
  IdTable idTable{getExecutionContext()->getAllocator()};
  LOG(DEBUG) << "Getting sub-result for distinct result computation..." << endl;
  std::shared_ptr<const Result> subRes = _subtree->getResult();

  LOG(DEBUG) << "Distinct result computation..." << endl;
  idTable.setNumColumns(subRes->idTable().numColumns());
  size_t width = subRes->idTable().numColumns();
  CALL_FIXED_SIZE(width, &Engine::distinct, subRes->idTable(), _keepIndices,
                  &idTable);
  LOG(DEBUG) << "Distinct result computation done." << endl;
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}
