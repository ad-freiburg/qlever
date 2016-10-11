// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <string>
#include <sstream>
#include <algorithm>
#include "./QueryExecutionTree.h"
#include "./IndexScan.h"
#include "./Join.h"
#include "./Sort.h"
#include "./OrderBy.h"
#include "./Filter.h"
#include "./Distinct.h"
#include "TextOperationForContexts.h"
#include "TextOperationWithFilter.h"
#include "TextOperationWithoutFilter.h"
#include "TwoColumnJoin.h"

using std::string;

// _____________________________________________________________________________
QueryExecutionTree::QueryExecutionTree(QueryExecutionContext* qec) :
    _qec(qec),
    _variableColumnMap(),
    _rootOperation(nullptr),
    _type(OperationType::UNDEFINED),
    _contextVars() {
}

// _____________________________________________________________________________
string QueryExecutionTree::asString() const {
  if (_rootOperation) {
    std::ostringstream os;
    os << "{" << _rootOperation->asString() << " | width: " << getResultWidth()
       << "}";
    if (_qec) {
      os << " [estimated size: " << getSizeEstimate() << "]";
    }
    return os.str();
  } else {
    return "<Empty QueryExecutionTree>";
  }
}

// _____________________________________________________________________________
void QueryExecutionTree::setOperation(QueryExecutionTree::OperationType type,
                                      std::shared_ptr<Operation> op) {
  _type = type;
  _rootOperation = op;
}

// _____________________________________________________________________________
void QueryExecutionTree::setVariableColumn(const string& variable,
                                           size_t column) {
  _variableColumnMap[variable] = column;
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getVariableColumn(const string& variable) const {
  if (_variableColumnMap.count(variable) == 0) {
    AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
             "Variable could not be mapped to result column. Var: " + variable);
  }
  return _variableColumnMap.find(variable)->second;
}

// _____________________________________________________________________________
void QueryExecutionTree::setVariableColumns(
    std::unordered_map<string, size_t> const& map) {
  _variableColumnMap = map;
}


// _____________________________________________________________________________
void QueryExecutionTree::writeResultToStream(std::ostream& out,
                                             const vector<string>& selectVars,
                                             size_t limit,
                                             size_t offset,
                                             char sep) const {
  // They may trigger computation (but does not have to).
  const ResultTable& res = getResult();
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  vector<pair<size_t, OutputType>> validIndices;
  for (auto var : selectVars) {
    OutputType outputType = OutputType::KB;
    if (ad_utility::startsWith(var, "SCORE(") || isContextvar(var)) {
      outputType = OutputType::VERBATIM;
    }
    if (ad_utility::startsWith(var, "TEXT(")) {
      outputType = OutputType::TEXT;
      var = var.substr(5, var.rfind(')') - 5);
    }

    if (getVariableColumnMap().find(var) != getVariableColumnMap().end()) {
      validIndices.push_back(
          pair<size_t, OutputType>(getVariableColumnMap().find(var)->second,
                                   outputType));
    }
  }
  if (validIndices.size() == 0) { return; }
  if (res._nofColumns == 1) {
    auto data = static_cast<vector<array<Id, 1>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeTable(*data, sep, offset, upperBound, validIndices, out);
  } else if (res._nofColumns == 2) {
    auto data = static_cast<vector<array<Id, 2>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeTable(*data, sep, offset, upperBound, validIndices, out);
  } else if (res._nofColumns == 3) {
    auto data = static_cast<vector<array<Id, 3>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeTable(*data, sep, offset, upperBound, validIndices, out);
  } else if (res._nofColumns == 4) {
    auto data = static_cast<vector<array<Id, 4>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeTable(*data, sep, offset, upperBound, validIndices, out);
  } else if (res._nofColumns == 5) {
    auto data = static_cast<vector<array<Id, 5>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeTable(*data, sep, offset, upperBound, validIndices, out);
  } else {
    size_t upperBound = std::min<size_t>(offset + limit,
                                         res._varSizeData.size());
    writeTable(res._varSizeData, sep, offset, upperBound, validIndices, out);
  }
  LOG(DEBUG) << "Done creating readable result.\n";
}

// _____________________________________________________________________________
void QueryExecutionTree::writeResultToStreamAsJson(
    std::ostream& out,
    const vector<string>& selectVars,
    size_t limit,
    size_t offset) const {
  out << "[\r\n";
  // They may trigger computation (but does not have to).
  const ResultTable& res = getResult();
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  vector<pair<size_t, OutputType>> validIndices;
  for (auto var : selectVars) {
    OutputType outputType = OutputType::KB;
    if (ad_utility::startsWith(var, "SCORE(") || isContextvar(var)) {
      outputType = OutputType::VERBATIM;
    }
    if (ad_utility::startsWith(var, "TEXT(")) {
      outputType = OutputType::TEXT;
      var = var.substr(5, var.rfind(')') - 5);
    }

    if (getVariableColumnMap().find(var) != getVariableColumnMap().end()) {
      validIndices.push_back(
          pair<size_t, OutputType>(getVariableColumnMap().find(var)->second,
                                   outputType));
    }
  }
  if (validIndices.size() == 0) { return; }
  if (res._nofColumns == 1) {
    auto data = static_cast<vector<array<Id, 1>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeJsonTable(*data, offset, upperBound, validIndices, out);
  } else if (res._nofColumns == 2) {
    auto data = static_cast<vector<array<Id, 2>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeJsonTable(*data, offset, upperBound, validIndices, out);
  } else if (res._nofColumns == 3) {
    auto data = static_cast<vector<array<Id, 3>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeJsonTable(*data, offset, upperBound, validIndices, out);
  } else if (res._nofColumns == 4) {
    auto data = static_cast<vector<array<Id, 4>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeJsonTable(*data, offset, upperBound, validIndices, out);
  } else if (res._nofColumns == 5) {
    auto data = static_cast<vector<array<Id, 5>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    writeJsonTable(*data, offset, upperBound, validIndices, out);
  } else {
    size_t upperBound = std::min<size_t>(offset + limit,
                                         res._varSizeData.size());
    writeJsonTable(res._varSizeData, offset, upperBound, validIndices, out);
  }
  out << "]";
  LOG(DEBUG) << "Done creating readable result.\n";
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getCostEstimate() const {
  return _rootOperation->getCostEstimate();
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getSizeEstimate() const {
  return _rootOperation->getSizeEstimate();
}

// _____________________________________________________________________________
bool QueryExecutionTree::varCovered(string var) const {
  return _variableColumnMap.count(var) > 0;
}
