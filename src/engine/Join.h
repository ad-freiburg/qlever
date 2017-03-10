// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include "./Operation.h"
#include "./QueryExecutionTree.h"
#include "./IndexScan.h"
#include "../util/HashMap.h"
#include "../util/HashSet.h"

using std::list;

class Join : public Operation {
public:

  Join(QueryExecutionContext* qec,
       std::shared_ptr<QueryExecutionTree> t1,
       std::shared_ptr<QueryExecutionTree> t2,
       size_t t1JoinCol,
       size_t t2JoinCol,
       bool keepJoinColumn = true);

  virtual string asString() const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const;

  std::unordered_map<string, size_t> getVariableColumns() const;

  std::unordered_set<string> getContextVars() const;

  virtual void setTextLimit(size_t limit) {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
    _sizeEstimateComputed = false;
  }

  virtual size_t getSizeEstimate() {
    if (!_sizeEstimateComputed) {
      _sizeEstimate = computeSizeEstimate();
      _sizeEstimateComputed = true;
    }
    return _sizeEstimate;
  }

  virtual size_t getCostEstimate();


  virtual bool knownEmptyResult() {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  size_t computeSizeEstimate();

  virtual float getMultiplicity(size_t col);

private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  size_t _leftJoinCol;
  size_t _rightJoinCol;

  bool _keepJoinColumn;

  bool _sizeEstimateComputed;
  size_t _sizeEstimate;

  vector<float> _multiplicities;

  virtual void computeResult(ResultTable* result) const;

  void computeMultiplicities();

  static bool isFullScanDummy(std::shared_ptr<QueryExecutionTree> tree) {
    return tree->getType() == QueryExecutionTree::SCAN &&
           tree->getResultWidth() == 3;
  }

  void computeResultForJoinWithFullScanDummy(ResultTable* result) const;

  typedef void (Index::*ScanMethodType)(Id, Index::WidthTwoList*) const;

  ScanMethodType getFittingScanMethod(
      std::shared_ptr<QueryExecutionTree> fullScanDummyTree) const;

  template<typename NonDummyResultList, typename ResultList>
  void doComputeJoinWithFullScanDummyLeft(const NonDummyResultList& v,
                                      ResultList* r) const;

  template<typename NonDummyResultList, typename ResultList>
  void doComputeJoinWithFullScanDummyRight(const NonDummyResultList& v,
                                          ResultList* r) const;
};
