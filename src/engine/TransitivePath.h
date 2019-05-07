// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#pragma once

#include "IdTable.h"
#include "Operation.h"
#include "QueryExecutionTree.h"

class TransitivePath : public Operation {
 public:
  TransitivePath(QueryExecutionContext* qec,
                 std::shared_ptr<QueryExecutionTree> child, size_t leftCol,
                 size_t rightCol, size_t minDist, size_t maxDist);

  virtual std::string asString(size_t indent = 0) const override;

  virtual std::string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<std::string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit) override;

  virtual bool knownEmptyResult() override;

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

  // The method is declared here to make it unit testable
  template <int SUB_WIDTH>
  static void computeTransitivePath(IdTable* res, const IdTable& sub,
                                    size_t leftCol, size_t rightCol,
                                    size_t minDist, size_t maxDist);

 private:
  virtual void computeResult(ResultTable* result) override;

  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _leftCol;
  size_t _rightCol;
  size_t _minDist;
  size_t _maxDist;
};
