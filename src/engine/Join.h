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

  // A very explicit constructor, which initializes an invalid join object (it
  // has no subtrees, which violates class invariants). These invalid Join
  // objects can be used for unit tests that only test member functions which
  // don't access the subtrees.
  struct InvalidOnlyForTestingJoinTag {};
  explicit Join(InvalidOnlyForTestingJoinTag) {}

  virtual string asString(size_t indent = 0) const override;

  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<string, size_t> getVariableColumns() const override;

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

  size_t getCostEstimate() override;

  bool knownEmptyResult() override {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  void computeSizeEstimateAndMultiplicities();

  float getMultiplicity(size_t col) override;

  vector<QueryExecutionTree*> getChildren() override {
    return {_left.get(), _right.get()};
  }

  /**
   * @brief Joins IdTables dynA and dynB on join column jc2, returning
   * the result in dynRes. Creates a cross product for matching rows
   **/
  template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  void join(const IdTable& dynA, size_t jc1, const IdTable& dynB, size_t jc2,
            IdTable* dynRes);

  class RightLargerTag {};
  class LeftLargerTag {};
  template <typename TagType, int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  static void doGallopInnerJoin(TagType, const IdTableView<L_WIDTH>& l1,
                                size_t jc1, const IdTableView<R_WIDTH>& l2,
                                size_t jc2, IdTableStatic<OUT_WIDTH>* result);

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

 public:
  static bool isFullScanDummy(std::shared_ptr<QueryExecutionTree> tree) {
    return tree->getType() == QueryExecutionTree::SCAN &&
           tree->getResultWidth() == 3;
  }

 private:
  void computeResultForJoinWithFullScanDummy(ResultTable* result);

  using ScanMethodType = std::function<void(Id, IdTable*)>;

  ScanMethodType getScanMethod(
      std::shared_ptr<QueryExecutionTree> fullScanDummyTree) const;

  void doComputeJoinWithFullScanDummyLeft(const IdTable& v, IdTable* r) const;

  void doComputeJoinWithFullScanDummyRight(const IdTable& v, IdTable* r) const;

  void appendCrossProduct(const IdTable::const_iterator& leftBegin,
                          const IdTable::const_iterator& leftEnd,
                          const IdTable::const_iterator& rightBegin,
                          const IdTable::const_iterator& rightEnd,
                          IdTable* res) const;
};
