// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include "./QueryExecutionContext.h"
#include "./Operation.h"
#include "../util/Conversions.h"
#include "../util/HashSet.h"


using std::string;

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
    TEXT_WITHOUT_FILTER = 8,
    TEXT_WITH_FILTER = 9,
    TWO_COL_JOIN = 10
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

  const ad_utility::HashMap<string, size_t>& getVariableColumnMap() const {
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

  void setVariableColumn(const string& var, size_t i);

  size_t getVariableColumn(const string& var) const;

  void setVariableColumns(const ad_utility::HashMap<string, size_t>& map);

  void setContextVars(const ad_utility::HashSet<string>& set) {
    _contextVars = set;
  }

  const ad_utility::HashSet<string>& getContextVars() const {
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
                           size_t offset = 0,
                           char sep = '\t') const;

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

  size_t getCostEstimate() const;

  size_t getSizeEstimate() const;

  bool varCovered(string var) const;

  bool knownEmptyResult() const {
    return _rootOperation->knownEmptyResult();
  }

private:
  QueryExecutionContext* _qec;   // No ownership
  ad_utility::HashMap<string, size_t> _variableColumnMap;
  Operation* _rootOperation;  // Owned child. Will be deleted at deconstruction.
  OperationType _type;
  ad_utility::HashSet<string> _contextVars;

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
          case KB: {
            string entity = _qec->getIndex().idToString(
                row[validIndices[j].first]);
            if (ad_utility::startsWith(entity, VALUE_PREFIX)) {
              entity = ad_utility::convertIndexWordToValueLiteral(entity);
            }
            out << ad_utility::escapeForJson(entity) << "\",\"";
            break;
          }
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
        case KB: {
          string entity = _qec->getIndex()
              .idToString(row[validIndices[validIndices.size() - 1].first]);
          if (ad_utility::startsWith(entity, VALUE_PREFIX)) {
            entity = ad_utility::convertIndexWordToValueLiteral(entity);
          }
          out << ad_utility::escapeForJson(entity) << "\"]";
          break;
        }
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
  void writeTable(const vector<Row>& data, char sep, size_t from,
                  size_t upperBound,
                  const vector<pair<size_t, OutputType>>& validIndices,
                  std::ostream& out) const {
    for (size_t i = from; i < upperBound; ++i) {
      const auto& row = data[i];
      for (size_t j = 0; j < validIndices.size(); ++j) {
        switch (validIndices[j].second) {
          case KB: {
            string entity = _qec->getIndex().idToString(
                row[validIndices[j].first]);
            if (ad_utility::startsWith(entity, VALUE_PREFIX)) {
              out << ad_utility::convertIndexWordToValueLiteral(entity);
            } else {
              out << entity;
            }
            break;
          }
          case VERBATIM:
            out << row[validIndices[j].first] << "\",\"";
            break;
          case TEXT:
            out << _qec->getIndex().getTextExcerpt(
                row[validIndices[j].first]);
            break;
          default: AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                            "Cannot deduce output type.");
        }
        out << (j + 1 < validIndices.size() ? sep : '\n');
      }
    }
  }
};