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
                 std::shared_ptr<QueryExecutionTree> child, bool leftIsVar,
                 bool rightIsVar, size_t leftSubCol, size_t rightSubCol,
                 size_t leftValue, size_t rightValue,
                 const std::string& leftColName,
                 const std::string& rightColName, size_t minDist,
                 size_t maxDist);

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
  /**
   * @brief If leftIsVar is true left is interpreted as a column index in sub,
   * otherwise it is interpreted as the id of a single entity.
   */
  template <int SUB_WIDTH>
  static void computeTransitivePath(IdTable* res, const IdTable& sub,
                                    bool leftIsVar, bool rightIsVar,
                                    size_t leftSubCol, size_t rightSubCol,
                                    Id leftValue, Id rightValue, size_t minDist,
                                    size_t maxDist);

 private:
  virtual void computeResult(ResultTable* result) override;

  std::shared_ptr<QueryExecutionTree> _subtree;
  bool _leftIsVar;
  bool _rightIsVar;
  size_t _leftSubCol;
  size_t _rightSubCol;
  size_t _leftValue;
  size_t _rightValue;
  std::string _leftColName;
  std::string _rightColName;
  size_t _minDist;
  size_t _maxDist;
};
