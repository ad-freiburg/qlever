// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <engine/AddCombinedRowToTable.h>
#include <engine/CallFixedSize.h>
#include <engine/IndexScan.h>
#include <engine/Join.h>
#include <global/Constants.h>
#include <global/Id.h>
#include <util/Exception.h>
#include <util/HashMap.h>

#include <functional>
#include <sstream>
#include <type_traits>
#include <vector>

using std::endl;
using std::string;

// _____________________________________________________________________________
Join::Join(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
           std::shared_ptr<QueryExecutionTree> t2, ColumnIndex t1JoinCol,
           ColumnIndex t2JoinCol, bool keepJoinColumn)
    : Operation(qec) {
  AD_CONTRACT_CHECK(t1 && t2);
  // Currently all join algorithms require both inputs to be sorted, so we
  // enforce the sorting here.
  t1 = QueryExecutionTree::createSortedTree(std::move(t1), {t1JoinCol});
  t2 = QueryExecutionTree::createSortedTree(std::move(t2), {t2JoinCol});

  // Make sure that the subtrees are ordered so that identical queries can be
  // identified.
  auto swapChildren = [&]() {
    std::swap(t1, t2);
    std::swap(t1JoinCol, t2JoinCol);
  };
  if (t1->getCacheKey() > t2->getCacheKey()) {
    swapChildren();
  }
  _left = std::move(t1);
  _leftJoinCol = t1JoinCol;
  _right = std::move(t2);
  _rightJoinCol = t2JoinCol;
  _keepJoinColumn = keepJoinColumn;
  _sizeEstimate = 0;
  _sizeEstimateComputed = false;
  _multiplicities.clear();
  auto findJoinVar = [](const QueryExecutionTree& tree,
                        ColumnIndex joinCol) -> Variable {
    return tree.getVariableAndInfoByColumnIndex(joinCol).first;
  };
  _joinVar = findJoinVar(*_left, _leftJoinCol);
  AD_CONTRACT_CHECK(_joinVar == findJoinVar(*_right, _rightJoinCol));
}

// _____________________________________________________________________________
Join::Join(InvalidOnlyForTestingJoinTag, QueryExecutionContext* qec)
    : Operation(qec) {
  // Needed, so that the timeout checker in Join::join doesn't create a seg
  // fault if it tries to create a message about the timeout.
  _left = std::make_shared<QueryExecutionTree>(qec);
  _right = _left;
}

// _____________________________________________________________________________
string Join::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "JOIN\n"
     << _left->getCacheKey() << " join-column: [" << _leftJoinCol << "]\n";
  os << "|X|\n"
     << _right->getCacheKey() << " join-column: [" << _rightJoinCol << "]";
  return std::move(os).str();
}

// _____________________________________________________________________________
string Join::getDescriptor() const { return "Join on " + _joinVar.name(); }

// _____________________________________________________________________________
ResultTable Join::computeResult() {
  LOG(DEBUG) << "Getting sub-results for join result computation..." << endl;
  size_t leftWidth = _left->getResultWidth();
  size_t rightWidth = _right->getResultWidth();
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(leftWidth + rightWidth - 1);

  if (_left->knownEmptyResult() || _right->knownEmptyResult()) {
    _left->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    _right->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    return {std::move(idTable), resultSortedOn(), LocalVocab()};
  }

  // Always materialize results that meet one of the following criteria:
  // * They are already present in the cache
  // * Their result is small
  // * They might contain UNDEF values in the join column
  // The first two conditions are for performance reasons, the last one is
  // because we currently cannot perform the optimized lazy joins when UNDEF
  // values are involved.
  auto getCachedOrSmallResult = [](QueryExecutionTree& tree,
                                   ColumnIndex joinCol) {
    bool isSmall =
        tree.getRootOperation()->getSizeEstimate() <
        RuntimeParameters().get<"lazy-index-scan-max-size-materialization">();
    auto undefStatus =
        tree.getVariableAndInfoByColumnIndex(joinCol).second.mightContainUndef_;
    bool containsUndef =
        undefStatus == ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined;
    // The third argument means "only get the result if it can be read from the
    // cache". So effectively, this returns the result if it is small, contains
    // UNDEF values, or is contained in the cache, otherwise `nullptr`.
    return tree.getRootOperation()->getResult(false,
                                              !(isSmall || containsUndef));
  };

  auto leftResIfCached = getCachedOrSmallResult(*_left, _leftJoinCol);
  checkCancellation();
  auto rightResIfCached = getCachedOrSmallResult(*_right, _rightJoinCol);
  checkCancellation();

  if (_left->getType() == QueryExecutionTree::SCAN &&
      _right->getType() == QueryExecutionTree::SCAN) {
    if (rightResIfCached && !leftResIfCached) {
      idTable = computeResultForIndexScanAndIdTable<true>(
          rightResIfCached->idTable(), _rightJoinCol,
          dynamic_cast<IndexScan&>(*_left->getRootOperation()), _leftJoinCol);
      checkCancellation();
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};

    } else if (!leftResIfCached) {
      idTable = computeResultForTwoIndexScans();
      checkCancellation();
      // TODO<joka921, hannahbast, SPARQL update> When we add triples to the
      // index, the vocabularies of index scans will not necessarily be empty
      // and we need a mechanism to still retrieve them when using the lazy
      // scan.
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    }
  }

  shared_ptr<const ResultTable> leftRes =
      leftResIfCached ? leftResIfCached : _left->getResult();
  checkCancellation();
  if (leftRes->size() == 0) {
    _right->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    // TODO<joka921, hannahbast, SPARQL update> When we add triples to the
    // index, the vocabularies of index scans will not necessarily be empty and
    // we need a mechanism to still retrieve them when using the lazy scan.
    return {std::move(idTable), resultSortedOn(), LocalVocab()};
  }

  // Note: If only one of the children is a scan, then we have made sure in the
  // constructor that it is the right child.
  // We currently cannot use this optimized lazy scan if the result from `_left`
  // contains UNDEF values.
  const auto& leftIdTable = leftRes->idTable();
  auto leftHasUndef =
      !leftIdTable.empty() && leftIdTable.at(0, _leftJoinCol).isUndefined();
  if (_right->getType() == QueryExecutionTree::SCAN && !rightResIfCached &&
      !leftHasUndef) {
    idTable = computeResultForIndexScanAndIdTable<false>(
        leftRes->idTable(), _leftJoinCol,
        dynamic_cast<IndexScan&>(*_right->getRootOperation()), _rightJoinCol);
    checkCancellation();
    return {std::move(idTable), resultSortedOn(),
            leftRes->getSharedLocalVocab()};
  }

  shared_ptr<const ResultTable> rightRes =
      rightResIfCached ? rightResIfCached : _right->getResult();
  checkCancellation();
  join(leftRes->idTable(), _leftJoinCol, rightRes->idTable(), _rightJoinCol,
       &idTable);
  checkCancellation();

  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          ResultTable::getSharedLocalVocabFromNonEmptyOf(*leftRes, *rightRes)};
}

// _____________________________________________________________________________
VariableToColumnMap Join::computeVariableToColumnMap() const {
  return makeVarToColMapForJoinOperation(
      _left->getVariableColumns(), _right->getVariableColumns(),
      {{_leftJoinCol, _rightJoinCol}}, BinOpType::Join,
      _left->getResultWidth());
}

// _____________________________________________________________________________
size_t Join::getResultWidth() const {
  size_t res = _left->getResultWidth() + _right->getResultWidth() -
               (_keepJoinColumn ? 1 : 2);
  AD_CONTRACT_CHECK(res > 0);
  return res;
}

// _____________________________________________________________________________
vector<ColumnIndex> Join::resultSortedOn() const { return {_leftJoinCol}; }

// _____________________________________________________________________________
float Join::getMultiplicity(size_t col) {
  if (_multiplicities.empty()) {
    computeSizeEstimateAndMultiplicities();
    _sizeEstimateComputed = true;
  }
  return _multiplicities[col];
}

// _____________________________________________________________________________
size_t Join::getCostEstimate() {
  size_t costJoin = _left->getSizeEstimate() + _right->getSizeEstimate();

  // TODO<joka921> once the `getCostEstimate` functions are `const`,
  // the argument can also be `const auto`
  auto costOfSubtree = [](auto& subtree) { return subtree->getCostEstimate(); };

  return getSizeEstimateBeforeLimit() + costJoin + costOfSubtree(_left) +
         costOfSubtree(_right);
}

// _____________________________________________________________________________
void Join::computeSizeEstimateAndMultiplicities() {
  _multiplicities.clear();
  if (_left->getSizeEstimate() == 0 || _right->getSizeEstimate() == 0) {
    for (size_t i = 0; i < getResultWidth(); ++i) {
      _multiplicities.emplace_back(1);
    }
    return;
  }

  size_t nofDistinctLeft = std::max(
      size_t(1), static_cast<size_t>(_left->getSizeEstimate() /
                                     _left->getMultiplicity(_leftJoinCol)));
  size_t nofDistinctRight = std::max(
      size_t(1), static_cast<size_t>(_right->getSizeEstimate() /
                                     _right->getMultiplicity(_rightJoinCol)));

  size_t nofDistinctInResult = std::min(nofDistinctLeft, nofDistinctRight);

  double adaptSizeLeft =
      _left->getSizeEstimate() *
      (static_cast<double>(nofDistinctInResult) / nofDistinctLeft);
  double adaptSizeRight =
      _right->getSizeEstimate() *
      (static_cast<double>(nofDistinctInResult) / nofDistinctRight);

  double corrFactor = _executionContext
                          ? _executionContext->getCostFactor(
                                "JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR")
                          : 1.0;

  double jcMultiplicityInResult = _left->getMultiplicity(_leftJoinCol) *
                                  _right->getMultiplicity(_rightJoinCol);
  _sizeEstimate = std::max(
      size_t(1), static_cast<size_t>(corrFactor * jcMultiplicityInResult *
                                     nofDistinctInResult));

  LOG(TRACE) << "Estimated size as: " << _sizeEstimate << " := " << corrFactor
             << " * " << jcMultiplicityInResult << " * " << nofDistinctInResult
             << std::endl;

  for (auto i = ColumnIndex{0}; i < _left->getResultWidth(); ++i) {
    double oldMult = _left->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * _right->getMultiplicity(_rightJoinCol) * corrFactor);
    if (i != _leftJoinCol && nofDistinctLeft != nofDistinctInResult) {
      double oldDist = _left->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeLeft);
      m = (_sizeEstimate / corrFactor) / newDist;
    }
    _multiplicities.emplace_back(m);
  }
  for (auto i = ColumnIndex{0}; i < _right->getResultWidth(); ++i) {
    if (i == _rightJoinCol) {
      continue;
    }
    double oldMult = _right->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * _left->getMultiplicity(_leftJoinCol) * corrFactor);
    if (i != _rightJoinCol && nofDistinctRight != nofDistinctInResult) {
      double oldDist = _right->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeRight);
      m = (_sizeEstimate / corrFactor) / newDist;
    }
    _multiplicities.emplace_back(m);
  }

  assert(_multiplicities.size() == getResultWidth());
}

// ______________________________________________________________________________

void Join::join(const IdTable& a, ColumnIndex jc1, const IdTable& b,
                ColumnIndex jc2, IdTable* result) const {
  LOG(DEBUG) << "Performing join between two tables.\n";
  LOG(DEBUG) << "A: width = " << a.numColumns() << ", size = " << a.size()
             << "\n";
  LOG(DEBUG) << "B: width = " << b.numColumns() << ", size = " << b.size()
             << "\n";

  // Check trivial case.
  if (a.empty() || b.empty()) {
    return;
  }
  checkCancellation();
  ad_utility::JoinColumnMapping joinColumnData{
      {{jc1, jc2}}, a.numColumns(), b.numColumns()};
  auto joinColumnL = a.getColumn(jc1);
  auto joinColumnR = b.getColumn(jc2);

  auto aPermuted = a.asColumnSubsetView(joinColumnData.permutationLeft());
  auto bPermuted = b.asColumnSubsetView(joinColumnData.permutationRight());

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(
      1, aPermuted, bPermuted, std::move(*result), cancellationHandle_);
  auto addRow = [beginLeft = joinColumnL.begin(),
                 beginRight = joinColumnR.begin(),
                 &rowAdder](const auto& itLeft, const auto& itRight) {
    rowAdder.addRow(itLeft - beginLeft, itRight - beginRight);
  };

  // The UNDEF values are right at the start, so this calculation works.
  size_t numUndefA =
      std::ranges::upper_bound(joinColumnL, ValueId::makeUndefined()) -
      joinColumnL.begin();
  size_t numUndefB =
      std::ranges::upper_bound(joinColumnR, ValueId::makeUndefined()) -
      joinColumnR.begin();
  std::pair undefRangeA{joinColumnL.begin(), joinColumnL.begin() + numUndefA};
  std::pair undefRangeB{joinColumnR.begin(), joinColumnR.begin() + numUndefB};

  auto cancellationCallback = [this]() { checkCancellation(); };

  // Determine whether we should use the galloping join optimization.
  if (a.size() / b.size() > GALLOP_THRESHOLD && numUndefA == 0 &&
      numUndefB == 0) {
    // The first argument to the galloping join will always be the smaller
    // input, so we need to switch the rows when adding them.
    auto inverseAddRow = [&addRow](const auto& rowA, const auto& rowB) {
      addRow(rowB, rowA);
    };
    ad_utility::gallopingJoin(joinColumnR, joinColumnL, std::ranges::less{},
                              inverseAddRow, {}, cancellationCallback);
  } else if (b.size() / a.size() > GALLOP_THRESHOLD && numUndefA == 0 &&
             numUndefB == 0) {
    ad_utility::gallopingJoin(joinColumnL, joinColumnR, std::ranges::less{},
                              addRow, {}, cancellationCallback);
  } else {
    auto findSmallerUndefRangeLeft =
        [undefRangeA](
            auto&&...) -> cppcoro::generator<decltype(undefRangeA.first)> {
      for (auto it = undefRangeA.first; it != undefRangeA.second; ++it) {
        co_yield it;
      }
    };
    auto findSmallerUndefRangeRight =
        [undefRangeB](
            auto&&...) -> cppcoro::generator<decltype(undefRangeB.first)> {
      for (auto it = undefRangeB.first; it != undefRangeB.second; ++it) {
        co_yield it;
      }
    };

    auto numOutOfOrder = [&]() {
      if (numUndefB == 0 && numUndefA == 0) {
        return ad_utility::zipperJoinWithUndef(
            joinColumnL, joinColumnR, std::ranges::less{}, addRow,
            ad_utility::noop, ad_utility::noop, {}, cancellationCallback);

      } else {
        return ad_utility::zipperJoinWithUndef(
            joinColumnL, joinColumnR, std::ranges::less{}, addRow,
            findSmallerUndefRangeLeft, findSmallerUndefRangeRight, {},
            cancellationCallback);
      }
    }();
    AD_CORRECTNESS_CHECK(numOutOfOrder == 0);
  }
  *result = std::move(rowAdder).resultTable();
  // The column order in the result is now
  // [joinColumns, non-join-columns-a, non-join-columns-b] (which makes the
  // algorithms above easier), be the order that is expected by the rest of
  // the code is [columns-a, non-join-columns-b]. Permute the columns to fix
  // the order.
  result->setColumnSubset(joinColumnData.permutationResult());

  LOG(DEBUG) << "Join done.\n";
  LOG(DEBUG) << "Result: width = " << result->numColumns()
             << ", size = " << result->size() << "\n";
}

// ______________________________________________________________________________
template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
void Join::hashJoinImpl(const IdTable& dynA, ColumnIndex jc1,
                        const IdTable& dynB, ColumnIndex jc2, IdTable* dynRes) {
  const IdTableView<L_WIDTH> a = dynA.asStaticView<L_WIDTH>();
  const IdTableView<R_WIDTH> b = dynB.asStaticView<R_WIDTH>();

  LOG(DEBUG) << "Performing hashJoin between two tables.\n";
  LOG(DEBUG) << "A: width = " << a.numColumns() << ", size = " << a.size()
             << "\n";
  LOG(DEBUG) << "B: width = " << b.numColumns() << ", size = " << b.size()
             << "\n";

  // Check trivial case.
  if (a.empty() || b.empty()) {
    return;
  }

  IdTableStatic<OUT_WIDTH> result = std::move(*dynRes).toStatic<OUT_WIDTH>();

  // Puts the rows of the given table into a hash map, with the value of
  // the join column of a row as the key, and returns the hash map.
  auto idTableToHashMap = []<typename Table>(const Table& table,
                                             const ColumnIndex jc) {
    // This declaration works, because generic lambdas are just syntactic sugar
    // for templates.
    ad_utility::HashMap<Id, std::vector<typename Table::row_type>> map;
    for (const auto& row : table) {
      map[row[jc]].push_back(row);
    }
    return map;
  };

  /*
   * @brief Joins the two tables, putting the result in result. Creates a cross
   *  product for matching rows by putting the smaller IdTable in a hash map and
   *  using that, to faster find the matching rows.
   *
   * @tparam leftIsLarger If the left table in the join operation has more
   *  rows, or the right. True, if he has. False, if he hasn't.
   * @tparam LargerTableType, SmallerTableType The types of the tables given.
   *  Can be easily done with type interference, so no need to write them out.
   *
   * @param largerTable, smallerTable The two tables of the join operation.
   * @param largerTableJoinColumn, smallerTableJoinColumn The join columns
   *  of the tables
   */
  auto performHashJoin = [&idTableToHashMap,
                          &result]<bool leftIsLarger, typename LargerTableType,
                                   typename SmallerTableType>(
                             const LargerTableType& largerTable,
                             const ColumnIndex largerTableJoinColumn,
                             const SmallerTableType& smallerTable,
                             const ColumnIndex smallerTableJoinColumn) {
    // Put the smaller table into the hash table.
    auto map = idTableToHashMap(smallerTable, smallerTableJoinColumn);

    // Create cross product by going through the larger table.
    for (size_t i = 0; i < largerTable.size(); i++) {
      // Skip, if there is no matching entry for the join column.
      auto entry = map.find(largerTable(i, largerTableJoinColumn));
      if (entry == map.end()) {
        continue;
      }

      for (const auto& row : entry->second) {
        // Based on which table was larger, the arguments of
        // addCombinedRowToIdTable are different.
        // However this information is known at compile time, so the other
        // branch gets discarded at compile time, which makes this
        // condition have constant runtime.
        if constexpr (leftIsLarger) {
          addCombinedRowToIdTable(largerTable[i], row, smallerTableJoinColumn,
                                  &result);
        } else {
          addCombinedRowToIdTable(row, largerTable[i], largerTableJoinColumn,
                                  &result);
        }
      }
    }
  };

  // Cannot just switch a and b around because the order of
  // items in the result tuples is important.
  // Procceding with the actual hash join depended on which IdTableView
  // is bigger.
  if (a.size() >= b.size()) {
    performHashJoin.template operator()<true>(a, jc1, b, jc2);
  } else {
    performHashJoin.template operator()<false>(b, jc2, a, jc1);
  }
  *dynRes = std::move(result).toDynamic();

  LOG(DEBUG) << "HashJoin done.\n";
  LOG(DEBUG) << "Result: width = " << dynRes->numColumns()
             << ", size = " << dynRes->size() << "\n";
}

// ______________________________________________________________________________
void Join::hashJoin(const IdTable& dynA, ColumnIndex jc1, const IdTable& dynB,
                    ColumnIndex jc2, IdTable* dynRes) {
  CALL_FIXED_SIZE(
      (std::array{dynA.numColumns(), dynB.numColumns(), dynRes->numColumns()}),
      &Join::hashJoinImpl, this, dynA, jc1, dynB, jc2, dynRes);
}

// ___________________________________________________________________________
template <typename ROW_A, typename ROW_B, int TABLE_WIDTH>
void Join::addCombinedRowToIdTable(const ROW_A& rowA, const ROW_B& rowB,
                                   const ColumnIndex jcRowB,
                                   IdTableStatic<TABLE_WIDTH>* table) {
  // Add a new, empty row.
  const size_t backIndex = table->size();
  table->emplace_back();

  // Copy the entire rowA in the table.
  for (auto h = ColumnIndex{0}; h < rowA.numColumns(); h++) {
    (*table)(backIndex, h) = rowA[h];
  }

  // Copy rowB columns before the join column.
  for (auto h = ColumnIndex{0}; h < jcRowB; h++) {
    (*table)(backIndex, h + rowA.numColumns()) = rowB[h];
  }

  // Copy rowB columns after the join column.
  for (auto h = jcRowB + 1; h < rowB.numColumns(); h++) {
    (*table)(backIndex, h + rowA.numColumns() - 1) = rowB[h];
  }
}

namespace {
// Convert a `generator<IdTable` to a `generator<IdTableAndFirstCol>` for more
// efficient access in the join columns below.
cppcoro::generator<ad_utility::IdTableAndFirstCol<IdTable>,
                   CompressedRelationReader::LazyScanMetadata>
convertGenerator(Permutation::IdTableGenerator gen) {
  co_await cppcoro::getDetails = gen.details();
  gen.setDetailsPointer(&co_await cppcoro::getDetails);
  for (auto& table : gen) {
    ad_utility::IdTableAndFirstCol t{std::move(table)};
    co_yield t;
  }
}

// Set the runtime info of the `scanTree` when it was lazily executed during a
// join.
void updateRuntimeInfoForLazyScan(
    IndexScan& scanTree,
    const CompressedRelationReader::LazyScanMetadata& metadata) {
  scanTree.updateRuntimeInformationWhenOptimizedOut(
      RuntimeInformation::Status::lazilyMaterialized);
  auto& rti = scanTree.runtimeInfo();
  rti.numRows_ = metadata.numElementsRead_;
  rti.totalTime_ = metadata.blockingTime_;
  rti.addDetail("num-blocks-read", metadata.numBlocksRead_);
  rti.addDetail("num-blocks-all", metadata.numBlocksAll_);
}
}  // namespace

// ______________________________________________________________________________________________________
IdTable Join::computeResultForTwoIndexScans() {
  AD_CORRECTNESS_CHECK(_left->getType() == QueryExecutionTree::SCAN &&
                       _right->getType() == QueryExecutionTree::SCAN);
  // The join column already is the first column in both inputs, so we don't
  // have to permute the inputs and results for the `AddCombinedRowToIdTable`
  // class to work correctly.
  AD_CORRECTNESS_CHECK(_leftJoinCol == 0 && _rightJoinCol == 0);
  ad_utility::AddCombinedRowToIdTable rowAdder{
      1, IdTable{getResultWidth(), getExecutionContext()->getAllocator()},
      cancellationHandle_};

  auto& leftScan = dynamic_cast<IndexScan&>(*_left->getRootOperation());
  auto& rightScan = dynamic_cast<IndexScan&>(*_right->getRootOperation());

  ad_utility::Timer timer{ad_utility::timer::Timer::InitialStatus::Started};
  auto [leftBlocksInternal, rightBlocksInternal] =
      IndexScan::lazyScanForJoinOfTwoScans(leftScan, rightScan);
  runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());

  auto leftBlocks = convertGenerator(std::move(leftBlocksInternal));
  auto rightBlocks = convertGenerator(std::move(rightBlocksInternal));

  ad_utility::zipperJoinForBlocksWithoutUndef(leftBlocks, rightBlocks,
                                              std::less{}, rowAdder);

  updateRuntimeInfoForLazyScan(leftScan, leftBlocks.details());
  updateRuntimeInfoForLazyScan(rightScan, rightBlocks.details());

  AD_CORRECTNESS_CHECK(leftBlocks.details().numBlocksRead_ <=
                       rightBlocks.details().numElementsRead_);
  AD_CORRECTNESS_CHECK(rightBlocks.details().numBlocksRead_ <=
                       leftBlocks.details().numElementsRead_);

  return std::move(rowAdder).resultTable();
}

// ______________________________________________________________________________________________________
template <bool idTableIsRightInput>
IdTable Join::computeResultForIndexScanAndIdTable(const IdTable& idTable,
                                                  ColumnIndex joinColTable,
                                                  IndexScan& scan,
                                                  ColumnIndex joinColScan) {
  // We first have to permute the columns.
  auto [jcLeft, jcRight, numColsLeft, numColsRight] = [&]() {
    return idTableIsRightInput
               ? std::tuple{joinColScan, joinColTable, scan.getResultWidth(),
                            idTable.numColumns()}
               : std::tuple{joinColTable, joinColScan, idTable.numColumns(),
                            scan.getResultWidth()};
  }();

  auto joinColMap = ad_utility::JoinColumnMapping{
      {{jcLeft, jcRight}}, numColsLeft, numColsRight};
  ad_utility::AddCombinedRowToIdTable rowAdder{
      1, IdTable{getResultWidth(), getExecutionContext()->getAllocator()},
      cancellationHandle_};

  AD_CORRECTNESS_CHECK(joinColScan == 0);
  auto permutationIdTable =
      ad_utility::IdTableAndFirstCol{idTable.asColumnSubsetView(
          idTableIsRightInput ? joinColMap.permutationRight()
                              : joinColMap.permutationLeft())};

  ad_utility::Timer timer{ad_utility::timer::Timer::InitialStatus::Started};
  auto rightBlocksInternal = IndexScan::lazyScanForJoinOfColumnWithScan(
      permutationIdTable.col(), scan);
  auto rightBlocks = convertGenerator(std::move(rightBlocksInternal));

  runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());

  auto doJoin = [&rowAdder](auto& left, auto& right) mutable {
    ad_utility::zipperJoinForBlocksWithoutUndef(left, right, std::less{},
                                                rowAdder);
  };
  auto blockForIdTable = std::span{&permutationIdTable, 1};
  if (idTableIsRightInput) {
    doJoin(rightBlocks, blockForIdTable);
  } else {
    doJoin(blockForIdTable, rightBlocks);
  }
  auto result = std::move(rowAdder).resultTable();
  result.setColumnSubset(joinColMap.permutationResult());

  updateRuntimeInfoForLazyScan(scan, rightBlocks.details());
  return result;
}
