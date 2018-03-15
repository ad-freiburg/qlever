// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <bits/unordered_set.h>
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
    TWO_COL_JOIN = 10,
    OPTIONAL_JOIN = 11
  };

  enum OutputType {
    KB = 0,
    VERBATIM = 1,
    TEXT = 2
  };

  void setOperation(OperationType type, std::shared_ptr<Operation> op);

  string asString(size_t indent = 0);

  QueryExecutionContext* getQec() const {
    return _qec;
  }

  const std::unordered_map<string, size_t>& getVariableColumnMap() const {
    return _variableColumnMap;
  }

  std::shared_ptr<Operation> getRootOperation() const {
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

  void setVariableColumns(const std::unordered_map<string, size_t>& map);

  void setContextVars(const std::unordered_set<string>& set) {
    _contextVars = set;
  }

  const std::unordered_set<string>& getContextVars() const {
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
                                 size_t offset = 0,
                                 size_t maxSend = MAX_NOF_ROWS_IN_RESULT) const;


  size_t resultSortedOn() const { return _rootOperation->resultSortedOn(); }

  bool isContextvar(const string& var) const {
    return _contextVars.count(var) > 0;
  }

  void addContextVar(const string& var) {
    _contextVars.insert(var);
  }

  void setTextLimit(size_t limit) {
    _rootOperation->setTextLimit(limit);
    // Invalidate caches asString representation.
    _asString = "";  // triggers recomputation.
    _sizeEstimate = std::numeric_limits<size_t>::max();
  }

  size_t getCostEstimate();

  size_t getSizeEstimate();

  float getMultiplicity(size_t col) const {
    return _rootOperation->getMultiplicity(col);
  }

  size_t getDistinctEstimate(size_t col) const {
    return static_cast<size_t>(_rootOperation->getSizeEstimate() /
           _rootOperation->getMultiplicity(col));
  }

  bool varCovered(string var) const;

  bool knownEmptyResult();

private:
  QueryExecutionContext* _qec;   // No ownership
  std::unordered_map<string, size_t> _variableColumnMap;
  std::shared_ptr<Operation> _rootOperation;  // Owned child. Will be deleted at deconstruction.
  OperationType _type;
  std::unordered_set<string> _contextVars;
  string _asString;
  size_t _sizeEstimate;

  template<typename Row>
  void writeJsonTable(const vector<Row>& data, size_t from,
                      size_t upperBound,
                      const vector<pair<size_t, OutputType>>& validIndices,
                      size_t maxSend,
                      std::ostream& out) const {
    std::ostringstream throwaway;
    for (size_t i = from; i < upperBound; ++i) {
      const auto& row = data[i];
      auto& os = (i < (maxSend + from) ? out : throwaway);
      os << "[\"";
      for (size_t j = 0; j + 1 < validIndices.size(); ++j) {
        switch (validIndices[j].second) {
          case KB: {
            string entity = _qec->getIndex().idToString(
                row[validIndices[j].first]);
            if (ad_utility::startsWith(entity, VALUE_PREFIX)) {
              entity = ad_utility::convertIndexWordToValueLiteral(entity);
            }
            os << ad_utility::escapeForJson(entity) << "\",\"";
            break;
          }
          case VERBATIM:
            os << row[validIndices[j].first] << "\",\"";
            break;
          case TEXT:
            os << ad_utility::escapeForJson(
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
          os << ad_utility::escapeForJson(entity) << "\"]";
          break;
        }
        case VERBATIM:
          os << row[validIndices[validIndices.size() - 1].first] << "\"]";
          break;
        case TEXT:
          os << ad_utility::escapeForJson(
              _qec->getIndex().getTextExcerpt(
                  row[validIndices[validIndices.size() - 1].first]))
              << "\"]";
          break;
        default: AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                          "Cannot deduce output type.");
      }
      if (i + 1 < upperBound && i + 1 < maxSend + from) { os << ", "; }
      os << "\r\n";
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
