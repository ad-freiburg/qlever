// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)
#pragma once

#include <engine/Operation.h>

/// The neutral element wrt `JOIN`. It contains one element, but binds no
/// variables (which means it has 0 columns).
class NeutralElementOperation : public Operation {
 public:
  explicit NeutralElementOperation(QueryExecutionContext* qec)
      : Operation{qec} {}
  std::vector<QueryExecutionTree*> getChildren() override { return {}; }

 private:
  // The individual implementation of `asString` (see above) that has to be
  // customized by every child class.
  [[nodiscard]] string asStringImpl(size_t) const override {
    return "Neutral Element";
  };

 public:
  [[nodiscard]] string getDescriptor() const override {
    return "NeutralElement";
  };
  [[nodiscard]] size_t getResultWidth() const override { return 0; };
  void setTextLimit(size_t) override{};
  size_t getCostEstimate() override { return 0; }
  size_t getSizeEstimate() override { return 1; }
  float getMultiplicity(size_t) override { return 0; };
  bool knownEmptyResult() override { return false; };

 protected:
  [[nodiscard]] vector<size_t> resultSortedOn() const override { return {}; };

 private:
  void computeResult(ResultTable* result) override {
    result->_idTable.setNumColumns(0);
    result->_idTable.resize(1);
  }

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap()
      const override {
    return {};
  };
};
