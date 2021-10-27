//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "../src/engine/Operation.h"

#ifndef QLEVER_DUMMYOPERATION_H
#define QLEVER_DUMMYOPERATION_H
// A configurable dummy operation for testing purposes
class DummyOperation : public Operation {
 public:
  DummyOperation(QueryExecutionContext* ctx) : Operation(ctx) {}
  virtual void computeResult(ResultTable* result) override {
    *result = std::move(_result);
    return;
  }

  string asString(size_t indent = 0) const override {
    (void)indent;
    return "dummy";
  }

  string getDescriptor() const override { return "dummy"; }

  virtual size_t getResultWidth() const override { return 2; }

  virtual vector<size_t> resultSortedOn() const override { return {1}; }

  virtual void setTextLimit(size_t limit) override { (void)limit; }

  virtual size_t getCostEstimate() override { return 10; }

  virtual size_t getSizeEstimate() override { return 10; }

  virtual float getMultiplicity(size_t col) override {
    (void)col;
    return 1;
  }

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  virtual bool knownEmptyResult() override { return false; }

  using VarColMap = ad_utility::HashMap<string, size_t>;

  virtual VarColMap getVariableColumns() const override { return _varColMap; }

  void setVariableColumns(ad_utility::HashMap<string, size_t> m) {
    _varColMap = std::move(m);
  }

  void setResult(ResultTable result) { _result = std::move(result); }

 private:
  VarColMap _varColMap;
  ResultTable _result{ad_utility::AllocatorWithLimit<Id>{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(100)}};
};

#endif  // QLEVER_DUMMYOPERATION_H
