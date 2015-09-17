// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include "./QueryExecutionContext.h"
#include "./Operation.h"


using std::string;
using std::unordered_map;
using std::unordered_set;

// A query execution tree.
// Processed bottom up, this gives an ordering to the operations
// needed to solve a query.
class QueryExecutionTree {

public:
  explicit QueryExecutionTree(QueryExecutionContext* qec);

  // Copy constructor
  QueryExecutionTree(const QueryExecutionTree& other);

  // Assignment operator
  QueryExecutionTree& operator=(const QueryExecutionTree& other);

  virtual ~QueryExecutionTree();

  enum OperationType {
    UNDEFINED = 0,
    SCAN = 1,
    JOIN = 2,
    SORT = 3,
    ORDER_BY = 4,
    FILTER = 5,
    DISTINCT = 6,
    TEXT_FOR_CONTEXTS = 7,
    TEXT_FOR_ENTITIES = 8
  };

  enum OutputType {
    KB = 0,
    VERBATIM = 1,
    TEXT = 2
  };

  void setOperation(OperationType type, Operation* op);

  string asString() const;

  QueryExecutionContext* getQec() const {
    return _qec;
  }

  const unordered_map<string, size_t>& getVariableColumnMap() const {
    return _variableColumnMap;
  }

  const Operation* getRootOperation() const {
    return _rootOperation;
  }

  const OperationType& getType() const {
    return _type;
  }

  bool isEmpty() const {
    return _type == OperationType::UNDEFINED || !_rootOperation;
  }

  void setVariableColumn(const string& var, int i);

  size_t getVariableColumn(const string& var) const;

  void setVariableColumns(const unordered_map<string, size_t>& map);

  void setContextVars(const unordered_set<string>& set) {
    _contextVars = set;
  }

  const unordered_set<string>& getContextVars() const {
    return _contextVars;
  }

  size_t getResultWidth() const {
    return _rootOperation->getResultWidth();
  }

  const ResultTable& getResult() const {
    return _rootOperation->getResult();
  }

  void writeResultToStream(std::ostream& out,
                           const vector<string>& selectVars,
                           size_t limit = MAX_NOF_ROWS_IN_RESULT,
                           size_t offset = 0) const;

  void writeResultToStreamAsJson(std::ostream& out,
                                 const vector<string>& selectVars,
                                 size_t limit = MAX_NOF_ROWS_IN_RESULT,
                                 size_t offset = 0) const;

  size_t resultSortedOn() const { return _rootOperation->resultSortedOn(); }

  bool isContextvar(const string& var) const {
    return _contextVars.count(var) > 0;
  }

  void addContextVar(const string& var) {
    _contextVars.insert(var);
  }

  void setTextLimit(size_t limit) {
    _rootOperation->setTextLimit(limit);
  }

private:
  QueryExecutionContext* _qec;   // No ownership
  unordered_map<string, size_t> _variableColumnMap;
  Operation* _rootOperation;  // Owned child. Will be deleted at deconstruction.
  OperationType _type;
  unordered_set<string> _contextVars;

  template<typename Row>
  void writeJsonTable(const vector<Row>& data, size_t from,
                      size_t upperBound,
                      const vector<pair<size_t, OutputType>>& validIndices,
                      std::ostream& out) const {
    for (size_t i = from; i < upperBound; ++i) {
      const auto& row = data[i];
      out << "[\"";
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        switch (validIndices[j].second) {
          case KB:
            out << ad_utility::escapeForJson(
                _qec->getIndex().idToString(row[validIndices[j].first]))
            << "\",\"";
            break;
          case VERBATIM:
            out << row[validIndices[j].first] << "\",\"";
            break;
          case TEXT:
            out << ad_utility::escapeForJson(
                _qec->getIndex().getTextExcerpt(row[validIndices[j].first]))
            << "\",\"";
            break;
          default: AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                            "Cannot deduce output type.");
        }
      }
      switch (validIndices[validIndices.size() - 1].second) {
        case KB:
          out << ad_utility::escapeForJson(
              _qec->getIndex()
                  .idToString(row[validIndices[validIndices.size() - 1].first]))
          << "\"]";
          break;
        case VERBATIM:
          out << row[validIndices[validIndices.size() - 1].first] << "\"]";
          break;
        case TEXT:
          out << ad_utility::escapeForJson(
              _qec->getIndex().getTextExcerpt(
                  row[validIndices[validIndices.size() - 1].first]))
          << "\"]";
          break;
        default: AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                          "Cannot deduce output type.");
      }
      if (i + 1 < upperBound) { out << ", "; }
      out << "\r\n";
    }
  }

  template<typename Row>
  void writeTsvTable(const vector<Row>& data, size_t from,
                     size_t upperBound,
                     const vector<pair<size_t, OutputType>>& validIndices,
                     std::ostream& out) const {
    for (size_t i = from; i < upperBound; ++i) {
      const auto& row = data[i];
      for (size_t j = 0; j < validIndices.size(); ++j) {
        switch (validIndices[j].second) {
          case KB:
            out << _qec->getIndex().idToString(row[validIndices[j].first]);
            break;
          case VERBATIM:
            out << row[validIndices[j].first] << "\",\"";
            break;
          case TEXT:
            out << _qec->getIndex().getTextExcerpt(row[validIndices[j].first]);
            break;
          default: AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                            "Cannot deduce output type.");
        }
        out << (j + 1 < validIndices.size() ? '\t' : '\n');
      }
    }
  }
};


