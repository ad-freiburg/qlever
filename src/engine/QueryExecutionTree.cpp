// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <string>
#include <sstream>
#include <algorithm>
#include <dirent.h>
#include <ev.h>
#include <fcntl.h>
#include "./QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
QueryExecutionTree::QueryExecutionTree(QueryExecutionContext* qec) :
    _qec(qec),
    _variableColumnMap(),
    _rootOperation(nullptr),
    _type(OperationType::UNDEFINED) {
}

// _____________________________________________________________________________
QueryExecutionTree::QueryExecutionTree(const QueryExecutionTree& other) :
    _qec(other._qec),
    _variableColumnMap(other._variableColumnMap),
    _type(other._type) {
  switch (other._type) {
    case OperationType::SCAN:
      _rootOperation = new IndexScan(
          *static_cast<IndexScan*>(other._rootOperation));
      break;
    case OperationType::JOIN:
      _rootOperation = new Join(
          *static_cast<Join*>(other._rootOperation));
      break;
    case OperationType::SORT:
      _rootOperation = new Sort(
          *static_cast<Sort*>(other._rootOperation));
      break;
    case OperationType::ORDER_BY:
      _rootOperation = new OrderBy(
          *static_cast<OrderBy*>(other._rootOperation));
      break;
    case UNDEFINED:
    default:
      _rootOperation = nullptr;
  }
}

// _____________________________________________________________________________
QueryExecutionTree& QueryExecutionTree::operator=(
    const QueryExecutionTree& other) {
  _qec = other._qec;
  _variableColumnMap = other._variableColumnMap;
  setOperation(other._type, other._rootOperation);
  return *this;
}

// _____________________________________________________________________________
QueryExecutionTree::~QueryExecutionTree() {
  delete _rootOperation;
}

// _____________________________________________________________________________
string QueryExecutionTree::asString() const {
  std::ostringstream os;
  os << "{" << _rootOperation->asString() << " | width: " << getResultWidth()
  << "}";
  return os.str();
}

// _____________________________________________________________________________
void QueryExecutionTree::setOperation(QueryExecutionTree::OperationType type,
                                      Operation* op) {
  _type = type;
  switch (type) {
    case OperationType::SCAN:
      delete _rootOperation;
      _rootOperation = new IndexScan(*static_cast<IndexScan*>(op));
      break;
    case OperationType::JOIN:
      delete _rootOperation;
      _rootOperation = new Join(*static_cast<Join*>(op));
      break;
    case OperationType::SORT:
      delete _rootOperation;
      _rootOperation = new Sort(*static_cast<Sort*>(op));
      break;
    case OperationType::ORDER_BY:
      delete _rootOperation;
      _rootOperation = new OrderBy(*static_cast<OrderBy*>(op));
      break;
    default: AD_THROW(ad_semsearch::Exception::ASSERT_FAILED, "Cannot set "
        "an operation with undefined type.");
  }
}

// _____________________________________________________________________________
void QueryExecutionTree::setVariableColumn(const string& variable, int column) {
  _variableColumnMap[variable] = column;
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getVariableColumn(const string& variable) const {
  AD_CHECK(_variableColumnMap.count(variable) > 0);
  return _variableColumnMap.find(variable)->second;
}

// _____________________________________________________________________________
void QueryExecutionTree::setVariableColumns(
    unordered_map<string, size_t> const& map) {
  _variableColumnMap = map;
}

// _____________________________________________________________________________
void QueryExecutionTree::writeResultToStream(std::ostream& out, size_t limit,
                                             size_t offset) const {
  const ResultTable& res = getResult();
  if (res._nofColumns == 1) {
    auto data = static_cast<vector<array<Id, 1>>*>(res._fixedSizeData);
    for (auto row : *data) {
      out << _qec->getIndex().idToString(row[0]) << '\n';
    }
  } else if (res._nofColumns == 2) {
    auto data = static_cast<vector<array<Id, 2>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      out << _qec->getIndex().idToString(row[0]) << '\t'
      << _qec->getIndex().idToString(row[1]) << '\n';
    }
  } else if (res._nofColumns == 3) {
    auto data = static_cast<vector<array<Id, 3>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      out << _qec->getIndex().idToString(row[0]) << '\t'
      << _qec->getIndex().idToString(row[1]) << '\t'
      << _qec->getIndex().idToString(row[2]) << '\n';
    }
  } else if (res._nofColumns == 4) {
    auto data = static_cast<vector<array<Id, 4>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      out << _qec->getIndex().idToString(row[0]) << '\t'
      << _qec->getIndex().idToString(row[1]) << '\t'
      << _qec->getIndex().idToString(row[2]) << '\t'
      << _qec->getIndex().idToString(row[2]) << '\n';
    }
  } else if (res._nofColumns == 5) {
    auto data = static_cast<vector<array<Id, 5>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      out << _qec->getIndex().idToString(row[0]) << '\t'
      << _qec->getIndex().idToString(row[1]) << '\t'
      << _qec->getIndex().idToString(row[2]) << '\t'
      << _qec->getIndex().idToString(row[3]) << '\t'
      << _qec->getIndex().idToString(row[4]) << '\n';
    }
  } else {
    size_t upperBound = std::min<size_t>(offset + limit,
                                         res._varSizeData.size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = res._varSizeData[i];
      for (size_t i = 0; i + 1 < row.size(); ++i) {
        out << _qec->getIndex().idToString(row[i]) << '\t';
      }
      if (row.size() > 0) {
        out << _qec->getIndex().idToString(row[row.size() - 1]) << '\n';
      }
    }
  }
}

// _____________________________________________________________________________
void QueryExecutionTree::writeResultToStream(std::ostream& out,
                                             const vector<string>& selectVars,
                                             size_t limit,
                                             size_t offset) const {
  // They may trigger computation (but does not have to).
  const ResultTable& res = getResult();
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  vector<size_t> validIndices;
  for (const auto& var : selectVars) {
    if (getVariableColumnMap().find(var) != getVariableColumnMap().end()) {
      validIndices.push_back(getVariableColumnMap().find(var)->second);
    }
  }
  if (validIndices.size() == 0) { return; }
  if (res._nofColumns == 1) {
    auto data = static_cast<vector<array<Id, 1>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << _qec->getIndex().idToString(row[validIndices[j]]) << '\t';
      }
      out << _qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]]) << '\n';
    }
  } else if (res._nofColumns == 2) {
    auto data = static_cast<vector<array<Id, 2>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << _qec->getIndex().idToString(row[validIndices[j]]) << '\t';
      }
      out << _qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]]) << '\n';
    }
  } else if (res._nofColumns == 3) {
    auto data = static_cast<vector<array<Id, 3>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << _qec->getIndex().idToString(row[validIndices[j]]) << '\t';
      }
      out << _qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]]) << '\n';
    }
  } else if (res._nofColumns == 4) {
    auto data = static_cast<vector<array<Id, 4>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << _qec->getIndex().idToString(row[validIndices[j]]) << '\t';
      }
      out << _qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]]) << '\n';
    }
  } else if (res._nofColumns == 5) {
    auto data = static_cast<vector<array<Id, 5>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << _qec->getIndex().idToString(row[validIndices[j]]) << '\t';
      }
      out << _qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]]) << '\n';;
    }
  } else {
    size_t upperBound = std::min<size_t>(offset + limit,
                                         res._varSizeData.size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = res._varSizeData[i];
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << _qec->getIndex().idToString(row[validIndices[j]]) << '\t';
      }
      out << _qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]]) << '\n';
    }
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
  vector<size_t> validIndices;
  for (const auto& var : selectVars) {
    if (getVariableColumnMap().find(var) != getVariableColumnMap().end()) {
      validIndices.push_back(getVariableColumnMap().find(var)->second);
    }
  }
  if (validIndices.size() == 0) { return; }
  if (res._nofColumns == 1) {
    auto data = static_cast<vector<array<Id, 1>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      out << "\"";
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << ad_utility::escapeForJson(
            _qec->getIndex().idToString(row[validIndices[j]]))
        << ad_utility::escapeForJson("\t");
      }
      out << ad_utility::escapeForJson(_qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]])) << "\"";
      if (i + 1 < upperBound) { out << ','; }
      out << "\r\n";
    }
  } else if (res._nofColumns == 2) {
    auto data = static_cast<vector<array<Id, 2>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      out << "\"";
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << ad_utility::escapeForJson(
            _qec->getIndex().idToString(row[validIndices[j]]))
        << ad_utility::escapeForJson("\t");
      }
      out << ad_utility::escapeForJson(_qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]])) << "\"";
      if (i + 1 < upperBound) { out << ','; }
      out << "\r\n";
    }
  } else if (res._nofColumns == 3) {
    auto data = static_cast<vector<array<Id, 3>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      out << "\"";
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << ad_utility::escapeForJson(
            _qec->getIndex().idToString(row[validIndices[j]]))
        << ad_utility::escapeForJson("\t");
      }
      out << ad_utility::escapeForJson(_qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]])) << "\"";
      if (i + 1 < upperBound) { out << ','; }
      out << "\r\n";
    }
  } else if (res._nofColumns == 4) {
    auto data = static_cast<vector<array<Id, 4>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      out << "\"";
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << ad_utility::escapeForJson(
            _qec->getIndex().idToString(row[validIndices[j]]))
        << ad_utility::escapeForJson("\t");
      }
      out << ad_utility::escapeForJson(_qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]])) << "\"";
      if (i + 1 < upperBound) { out << ','; }
      out << "\r\n";
    }
  } else if (res._nofColumns == 5) {
    auto data = static_cast<vector<array<Id, 5>>*>(res._fixedSizeData);
    size_t upperBound = std::min<size_t>(offset + limit, data->size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = (*data)[i];
      out << "\"";
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << ad_utility::escapeForJson(
            _qec->getIndex().idToString(row[validIndices[j]]))
        << ad_utility::escapeForJson("\t");
      }
      out << ad_utility::escapeForJson(_qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]])) << "\"";
      if (i + 1 < upperBound) { out << ','; }
      out << "\r\n";
    }
  } else {
    size_t upperBound = std::min<size_t>(offset + limit,
                                         res._varSizeData.size());
    for (size_t i = offset; i < upperBound; ++i) {
      auto row = res._varSizeData[i];
      out << "\"";
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        out << ad_utility::escapeForJson(
            _qec->getIndex().idToString(row[validIndices[j]]))
        << ad_utility::escapeForJson("\t");
      }
      out << ad_utility::escapeForJson(_qec->getIndex().idToString(
          row[validIndices[validIndices.size() - 1]])) << "\"";
      if (i + 1 < upperBound) { out << ','; }
      out << "\r\n";
    }
  }
  out << "]";
  LOG(DEBUG) << "Done creating readable result.\n";
}
