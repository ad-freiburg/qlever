// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <grp.h>
#include "./QueryExecutionContext.h"
#include "./Operation.h"
#include "./Join.h"
#include "./IndexScan.h"
#include "./Sort.h"


using std::string;

// A query execution tree.
// Processed bottom up, this gives an ordering to the operations
// needed to solve a query.
class QueryExecutionTree {

public:
  explicit QueryExecutionTree(QueryExecutionContext* qec) :
      _qec(qec),
      _variableColumnMap(),
      _rootOperation(nullptr),
      _type(OperationType::UNDEFINED) {
  }

  // Copy constructor
  explicit QueryExecutionTree(const QueryExecutionTree& other) :
      _qec(other._qec),
      _variableColumnMap(other._variableColumnMap),
      _type(other._type) {
    switch (other._type) {
      case OperationType::SCAN:
        _rootOperation = new IndexScan(
            *reinterpret_cast<IndexScan*>(other._rootOperation));
        break;
      case OperationType::JOIN:
        _rootOperation = new Join(
            *reinterpret_cast<Join*>(other._rootOperation));
        break;
      case OperationType::SORT:
        _rootOperation = new Sort(
            *reinterpret_cast<Sort*>(other._rootOperation));
        break;
      case UNDEFINED:
      default:
        _rootOperation = nullptr;
    }
  }

  // Assignment operator
  QueryExecutionTree& operator=(const QueryExecutionTree& other) {
    _qec = other._qec;
    _variableColumnMap = other._variableColumnMap;
    setOperation(other._type, other._rootOperation);
    return *this;
  }

  virtual ~QueryExecutionTree() {
    delete _rootOperation;
  }

  enum OperationType {
    UNDEFINED = 0,
    SCAN = 1,
    JOIN = 2,
    SORT = 3
  };

  void setOperation(OperationType type, Operation* op);

  string asString() const;

  QueryExecutionContext* getQec() const {
    return _qec;
  }

  const unordered_map<string, size_t>& getVariableColumnMap() const {
    return _variableColumnMap;
  }

  Operation* getRootOperation() const {
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

  size_t getResultWidth() const {
    return _rootOperation->getResultWidth();
  }

private:
  QueryExecutionContext* _qec;   // No ownership
  unordered_map<string, size_t> _variableColumnMap;
  Operation* _rootOperation;  // Owned child. Will be deleted at deconstruction.
  OperationType _type;
};


