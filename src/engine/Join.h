// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <engine/Operation.h>
#include <engine/QueryExecutionTree.h>
#include <util/HashMap.h>
#include <util/HashSet.h>

#include <list>

using std::list;

class Join : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  size_t _leftJoinCol;
  size_t _rightJoinCol;

  bool _keepJoinColumn;

  bool _sizeEstimateComputed;
  size_t _sizeEstimate;

  vector<float> _multiplicities;

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

  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

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
   * the result in dynRes. Creates a cross product for matching rows.
   *
   * This should be a switch, which shoud decide which algorithm to use for
   * joining two IdTables.
   * The possible algorithms should be:
   * - The normal merge join.
   * - The doGallopInnerJoin.
   * - The hashJoinImpl.
   * Currently it only decides between doGallopInnerJoin and the standard merge
   * join, with the merge join code directly written in the function.
   * TODO Move the merge join into it's own function and make this function
   * a proper switch.
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

  /**
   * @brief Joins IdTables dynA and dynB on join column jc2, returning
   * the result in dynRes. Creates a cross product for matching rows by putting
   * the smaller IdTable in a hash map and using that, to faster find the
   * matching rows.
   * Needed to be a seperate function from the actual implementation, because
   * compiler optimization keept inlining it, which make testing impossible,
   * because you couldn't call the function after linking and just got
   * 'undefined reference' errors.
   *
   * @return The result is only sorted, if the bigger table is sorted.
   * Otherwise it is not sorted.
   **/
  void hashJoin(const IdTable& dynA, size_t jc1, const IdTable& dynB,
                size_t jc2, IdTable* dynRes);

  static bool isFullScanDummy(std::shared_ptr<QueryExecutionTree> tree) {
    return tree->getType() == QueryExecutionTree::SCAN &&
           tree->getResultWidth() == 3;
  }

 protected:
  virtual string asStringImpl(size_t indent = 0) const override;

 private:
  void computeResult(ResultTable* result) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

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
  /*
   * @brief Combines 2 rows like in a join and inserts the result in the
   * given table.
   *
   *@tparam The amount of columns in the rows and the table.
   *
   * @param[in] The left row of the join.
   * @param[in] The right row of the join.
   * @param[in] The numerical position of the join column of row b.
   * @param[in] The table, in which the new combined row should be insterted.
   * Must be static.
   */
  template <typename ROW_A, typename ROW_B, int TABLE_WIDTH>
  static void addCombinedRowToIdTable(const ROW_A& rowA, const ROW_B& rowB,
                                      const size_t jcRowB,
                                      IdTableStatic<TABLE_WIDTH>* table);

  /*
   * @brief The implementation of hashJoin.
   */
  template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  void hashJoinImpl(const IdTable& dynA, size_t jc1, const IdTable& dynB,
                    size_t jc2, IdTable* dynRes);
};
