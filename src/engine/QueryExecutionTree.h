// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include "./QueryExecutionContext.h"
#include "./Operation.h"
#include "./Join.h"
#include "./IndexScan.h"
#include "./Sort.h"
#include "./OrderBy.h"


using std::string;

// A query execution tree.
// Processed bottom up, this gives an ordering to the operations
// needed to solve a query.
class QueryExecutionTree {

public:
  explicit QueryExecutionTree(QueryExecutionContext* qec);

  // Copy constructor
  explicit QueryExecutionTree(const QueryExecutionTree& other);

  // Assignment operator
  QueryExecutionTree& operator=(const QueryExecutionTree& other);

  virtual ~QueryExecutionTree();

  enum OperationType {
    UNDEFINED = 0,
    SCAN = 1,
    JOIN = 2,
    SORT = 3,
    ORDER_BY = 4
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

  const ResultTable& getResult() const {
    return _rootOperation->getResult();
  }

  void writeResultToStream(std::ostream& out,
      size_t limit = MAX_NOF_ROWS_IN_RESULT,
      size_t offset = 0) const;

  void writeResultToStream(std::ostream& out,
      const vector<string>& selectVars,
      size_t limit = MAX_NOF_ROWS_IN_RESULT,
      size_t offset = 0) const;

  void writeResultToStreamAsJson(std::ostream& out,
                           const vector<string>& selectVars,
                           size_t limit = MAX_NOF_ROWS_IN_RESULT,
                           size_t offset = 0) const;

  size_t resultSortedOn() const { return _rootOperation->resultSortedOn(); }

private:
  QueryExecutionContext* _qec;   // No ownership
  unordered_map<string, size_t> _variableColumnMap;
  Operation* _rootOperation;  // Owned child. Will be deleted at deconstruction.
  OperationType _type;
};


