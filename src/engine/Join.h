// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include "../util/HashMap.h"
#include "../util/HashSet.h"
#include "./IndexScan.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;

class Join : public Operation {
 public:
  Join(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
       std::shared_ptr<QueryExecutionTree> t2, size_t t1JoinCol,
       size_t t2JoinCol, bool keepJoinColumn = true);

  virtual string asString(size_t indent = 0) const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<string, size_t> getVariableColumns() const;

  std::unordered_set<string> getContextVars() const;

  virtual void setTextLimit(size_t limit) override {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
    _sizeEstimateComputed = false;
  }

  virtual size_t getSizeEstimate() override {
    if (!_sizeEstimateComputed) {
      computeSizeEstimateAndMultiplicities();
      _sizeEstimateComputed = true;
    }
    return _sizeEstimate;
  }

  virtual size_t getCostEstimate() override;

  virtual bool knownEmptyResult() override {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  void computeSizeEstimateAndMultiplicities();

  virtual float getMultiplicity(size_t col) override;

 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  size_t _leftJoinCol;
  size_t _rightJoinCol;

  bool _keepJoinColumn;

  bool _sizeEstimateComputed;
  size_t _sizeEstimate;

  vector<float> _multiplicities;

  virtual void computeResult(ResultTable* result) override;

  static bool isFullScanDummy(std::shared_ptr<QueryExecutionTree> tree) {
    return tree->getType() == QueryExecutionTree::SCAN &&
           tree->getResultWidth() == 3;
  }

  void computeResultForJoinWithFullScanDummy(ResultTable* result) const;

  typedef void (Index::*ScanMethodType)(Id, IdTable*) const;

  ScanMethodType getScanMethod(
      std::shared_ptr<QueryExecutionTree> fullScanDummyTree) const;

  void doComputeJoinWithFullScanDummyLeft(const IdTable& v, IdTable* r) const;

  void doComputeJoinWithFullScanDummyRight(const IdTable& v, IdTable* r) const;

  void appendCrossProduct(IdTable::const_iterator& leftBegin,
                          IdTable::const_iterator& leftEnd,
                          IdTable::const_iterator& rightBegin,
                          IdTable::const_iterator& rightEnd,
                          IdTable* res) const;
};
