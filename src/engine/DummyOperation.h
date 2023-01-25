//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "engine/ResultTable.h"
#include "util/Random.h"
// used to test HasRelationScan with a subtree
class DummyOperation : public Operation {
 private:
  IdTable table_;
  std::vector<Variable> variables_;

 public:
  DummyOperation(QueryExecutionContext* ctx, IdTable table,
                 std::vector<Variable> variables)
      : Operation{ctx},
        table_{std::move(table)},
        variables_{std::move(variables)} {
    AD_CHECK(variables_.size() == table_.numColumns());
  }
  virtual void computeResult(ResultTable* result) override {
    for (size_t i = 0; i < table_.numColumns(); ++i) {
      result->_resultTypes.push_back(ResultTable::ResultType::KB);
    }
    result->_idTable = table_.clone();
  }

 private:
  string asStringImpl(size_t indent = 0) const override {
    (void)indent;
    std::stringstream str;
    str << "dummy operation with " << table_.numColumns()
        << "columns and contents ";
    for (size_t i = 0; i < table_.numColumns(); ++i) {
      for (Id entry : table_.getColumn(i)) {
        str << entry << ' ';
      }
    }
    return std::move(str).str();
  }

 public:
  string getDescriptor() const override { return "dummy"; }

  virtual size_t getResultWidth() const override { return table_.numColumns(); }

  // TODO<joka921> Maybe we will need to store sorted tables for future unit
  // tests.
  virtual vector<size_t> resultSortedOn() const override { return {}; }

  virtual void setTextLimit(size_t limit) override { (void)limit; }

  virtual size_t getCostEstimate() override { return table_.numRows(); }

  virtual size_t getSizeEstimate() override { return table_.numRows(); }

  virtual float getMultiplicity(size_t col) override {
    (void)col;
    return (col + 1) * 42.0;
  }

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  virtual bool knownEmptyResult() override { return table_.empty(); }

 private:
  virtual VariableToColumnMap computeVariableToColumnMap() const override {
    VariableToColumnMap m;
    for (size_t i = 0; i < variables_.size(); ++i) {
      m[variables_.at(i)] = i;
    }
    return m;
  }
};
