// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/Join.h"

#include <functional>
#include <sstream>
#include <type_traits>
#include <vector>

#include "engine/AddCombinedRowToTable.h"
#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "engine/JoinHelpers.h"
#include "engine/Service.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/RuntimeParameters.h"
#include "util/Exception.h"
#include "util/Generators.h"
#include "util/HashMap.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

using namespace qlever::joinHelpers;

using std::endl;
using std::string;

// _____________________________________________________________________________
Join::Join(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
           std::shared_ptr<QueryExecutionTree> t2, ColumnIndex t1JoinCol,
           ColumnIndex t2JoinCol, bool allowSwappingChildrenOnlyForTesting)
    : Operation(qec) {
  AD_CONTRACT_CHECK(t1 && t2);
  // Currently all join algorithms require both inputs to be sorted, so we
  // enforce the sorting here.
  t1 = QueryExecutionTree::createSortedTree(std::move(t1), {t1JoinCol});
  t2 = QueryExecutionTree::createSortedTree(std::move(t2), {t2JoinCol});

  // Make the order of the two subtrees deterministic. That way, queries that
  // are identical except for the order of the join operands, are easier to
  // identify.
  auto swapChildren = [&]() {
    // This can be disabled by tests to fix the order of the subtrees.
    if (allowSwappingChildrenOnlyForTesting) {
      std::swap(t1, t2);
      std::swap(t1JoinCol, t2JoinCol);
    }
  };
  if (t1->getCacheKey() > t2->getCacheKey()) {
    swapChildren();
  }
  // If one of the inputs is a SCAN and the other one is not, always make the
  // SCAN the right child (which also gives a deterministic order of the
  // subtrees). This simplifies several branches in the `computeResult` method.
  if (std::dynamic_pointer_cast<IndexScan>(t1->getRootOperation()) &&
      !std::dynamic_pointer_cast<IndexScan>(t2->getRootOperation())) {
    swapChildren();
  }
  _left = std::move(t1);
  _leftJoinCol = t1JoinCol;
  _right = std::move(t2);
  _rightJoinCol = t2JoinCol;
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
Result Join::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "Getting sub-results for join result computation..." << endl;
  if (_left->knownEmptyResult() || _right->knownEmptyResult()) {
    _left->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    _right->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    return createEmptyResult();
  }

  // If one of the RootOperations is a Service, precompute the result of its
  // sibling.
  Service::precomputeSiblingResult(_left->getRootOperation(),
                                   _right->getRootOperation(), false,
                                   requestLaziness);

  // Always materialize results that meet one of the following criteria:
  // * They are already present in the cache
  // * Their result is small
  // This is purely for performance reasons.
  auto getCachedOrSmallResult = [](const QueryExecutionTree& tree) {
    bool isSmall =
        tree.getRootOperation()->getSizeEstimate() <
        RuntimeParameters().get<"lazy-index-scan-max-size-materialization">();
    // The third argument means "only get the result if it can be read from the
    // cache". So effectively, this returns the result if it is small, or is
    // contained in the cache, otherwise `nullptr`.
    // TODO<joka921> Add a unit test that checks the correct conditions
    return tree.getRootOperation()->getResult(
        false, isSmall ? ComputationMode::FULLY_MATERIALIZED
                       : ComputationMode::ONLY_IF_CACHED);
  };

  auto leftResIfCached = getCachedOrSmallResult(*_left);
  checkCancellation();
  auto rightResIfCached = getCachedOrSmallResult(*_right);
  checkCancellation();

  auto leftIndexScan =
      std::dynamic_pointer_cast<IndexScan>(_left->getRootOperation());
  if (leftIndexScan &&
      std::dynamic_pointer_cast<IndexScan>(_right->getRootOperation())) {
    if (rightResIfCached && !leftResIfCached) {
      AD_CORRECTNESS_CHECK(rightResIfCached->isFullyMaterialized());
      return computeResultForIndexScanAndIdTable<true>(
          requestLaziness, std::move(rightResIfCached), leftIndexScan);

    } else if (!leftResIfCached) {
      return computeResultForTwoIndexScans(requestLaziness);
    }
  }

  std::shared_ptr<const Result> leftRes =
      leftResIfCached ? leftResIfCached : _left->getResult(true);
  checkCancellation();
  if (leftRes->isFullyMaterialized() && leftRes->idTable().empty()) {
    _right->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    return createEmptyResult();
  }

  // Note: If only one of the children is a scan, then we have made sure in the
  // constructor that it is the right child.
  auto rightIndexScan =
      std::dynamic_pointer_cast<IndexScan>(_right->getRootOperation());
  if (rightIndexScan && !rightResIfCached) {
    if (leftRes->isFullyMaterialized()) {
      return computeResultForIndexScanAndIdTable<false>(
          requestLaziness, std::move(leftRes), rightIndexScan);
    }
    return computeResultForIndexScanAndLazyOperation(
        requestLaziness, std::move(leftRes), rightIndexScan);
  }

  std::shared_ptr<const Result> rightRes =
      rightResIfCached ? rightResIfCached : _right->getResult(true);
  checkCancellation();
  if (leftRes->isFullyMaterialized() && rightRes->isFullyMaterialized()) {
    return computeResultForTwoMaterializedInputs(std::move(leftRes),
                                                 std::move(rightRes));
  }
  return lazyJoin(std::move(leftRes), std::move(rightRes), requestLaziness);
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
  size_t res = _left->getResultWidth() + _right->getResultWidth() - 1;
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

void Join::join(const IdTable& a, const IdTable& b, IdTable* result) const {
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
  ad_utility::JoinColumnMapping joinColumnData = getJoinColumnMapping();
  auto joinColumnL = a.getColumn(_leftJoinCol);
  auto joinColumnR = b.getColumn(_rightJoinCol);

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
      ql::ranges::upper_bound(joinColumnL, ValueId::makeUndefined()) -
      joinColumnL.begin();
  size_t numUndefB =
      ql::ranges::upper_bound(joinColumnR, ValueId::makeUndefined()) -
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
    ad_utility::gallopingJoin(joinColumnR, joinColumnL, ql::ranges::less{},
                              inverseAddRow, {}, cancellationCallback);
  } else if (b.size() / a.size() > GALLOP_THRESHOLD && numUndefA == 0 &&
             numUndefB == 0) {
    ad_utility::gallopingJoin(joinColumnL, joinColumnR, ql::ranges::less{},
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
            joinColumnL, joinColumnR, ql::ranges::less{}, addRow,
            ad_utility::noop, ad_utility::noop, {}, cancellationCallback);

      } else {
        return ad_utility::zipperJoinWithUndef(
            joinColumnL, joinColumnR, ql::ranges::less{}, addRow,
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

// _____________________________________________________________________________
CPP_template_def(typename ActionT)(
    requires ad_utility::InvocableWithExactReturnType<
        ActionT, Result::IdTableVocabPair,
        std::function<void(IdTable&, LocalVocab&)>>)
    Result Join::createResult(
        bool requestedLaziness, ActionT action,
        std::optional<std::vector<ColumnIndex>> permutation) const {
  if (requestedLaziness) {
    return {runLazyJoinAndConvertToGenerator(std::move(action),
                                             std::move(permutation)),
            resultSortedOn()};
  } else {
    auto [idTable, localVocab] = action(ad_utility::noop);
    applyPermutation(idTable, permutation);
    return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
  }
}

// ______________________________________________________________________________
Result Join::lazyJoin(std::shared_ptr<const Result> a,
                      std::shared_ptr<const Result> b,
                      bool requestLaziness) const {
  // If both inputs are fully materialized, we can join them more
  // efficiently.
  AD_CONTRACT_CHECK(!a->isFullyMaterialized() || !b->isFullyMaterialized());
  ad_utility::JoinColumnMapping joinColMap = getJoinColumnMapping();
  auto resultPermutation = joinColMap.permutationResult();
  return createResult(
      requestLaziness,
      [this, a = std::move(a), b = std::move(b),
       joinColMap = std::move(joinColMap)](
          std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        auto rowAdder = makeRowAdder(std::move(yieldTable));
        auto leftRange = resultToView(*a, joinColMap.permutationLeft());
        auto rightRange = resultToView(*b, joinColMap.permutationRight());
        std::visit(
            [&rowAdder](auto& leftBlocks, auto& rightBlocks) {
              ad_utility::zipperJoinForBlocksWithPotentialUndef(
                  leftBlocks, rightBlocks, std::less{}, rowAdder);
            },
            leftRange, rightRange);
        auto localVocab = std::move(rowAdder.localVocab());
        return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                        std::move(localVocab)};
      },
      std::move(resultPermutation));
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
  auto idTableToHashMap = [](const auto& table, const ColumnIndex jc) {
    // This declaration works, because generic lambdas are just syntactic sugar
    // for templates.
    using Table = std::decay_t<decltype(table)>;
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
  auto performHashJoin = ad_utility::ApplyAsValueIdentity{
      [&idTableToHashMap, &result](auto leftIsLarger, const auto& largerTable,
                                   const ColumnIndex largerTableJoinColumn,
                                   const auto& smallerTable,
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
              addCombinedRowToIdTable(largerTable[i], row,
                                      smallerTableJoinColumn, &result);
            } else {
              addCombinedRowToIdTable(row, largerTable[i],
                                      largerTableJoinColumn, &result);
            }
          }
        }
      }};

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
      &Join::hashJoinImpl, dynA, jc1, dynB, jc2, dynRes);
}

// ___________________________________________________________________________
template <typename ROW_A, typename ROW_B, int TABLE_WIDTH>
void Join::addCombinedRowToIdTable(const ROW_A& rowA, const ROW_B& rowB,
                                   ColumnIndex jcRowB,
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

// ______________________________________________________________________________________________________
Result Join::computeResultForTwoIndexScans(bool requestLaziness) const {
  return createResult(
      requestLaziness,
      [this](std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        auto leftScan =
            std::dynamic_pointer_cast<IndexScan>(_left->getRootOperation());
        auto rightScan =
            std::dynamic_pointer_cast<IndexScan>(_right->getRootOperation());
        AD_CORRECTNESS_CHECK(leftScan && rightScan);
        // The join column already is the first column in both inputs, so we
        // don't have to permute the inputs and results for the
        // `AddCombinedRowToIdTable` class to work correctly.
        AD_CORRECTNESS_CHECK(_leftJoinCol == 0 && _rightJoinCol == 0);
        auto rowAdder = makeRowAdder(std::move(yieldTable));

        ad_utility::Timer timer{
            ad_utility::timer::Timer::InitialStatus::Started};
        auto [leftBlocksInternal, rightBlocksInternal] =
            IndexScan::lazyScanForJoinOfTwoScans(*leftScan, *rightScan);
        runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());

        auto leftBlocks = convertGenerator(std::move(leftBlocksInternal));
        auto rightBlocks = convertGenerator(std::move(rightBlocksInternal));

        ad_utility::zipperJoinForBlocksWithoutUndef(leftBlocks, rightBlocks,
                                                    std::less{}, rowAdder);

        leftScan->updateRuntimeInfoForLazyScan(leftBlocks.details());
        rightScan->updateRuntimeInfoForLazyScan(rightBlocks.details());

        auto localVocab = std::move(rowAdder.localVocab());
        return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                        std::move(localVocab)};
      });
}

// ______________________________________________________________________________________________________
template <bool idTableIsRightInput>
Result Join::computeResultForIndexScanAndIdTable(
    bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
    std::shared_ptr<IndexScan> scan) const {
  AD_CORRECTNESS_CHECK((idTableIsRightInput ? _leftJoinCol : _rightJoinCol) ==
                       0);
  ad_utility::JoinColumnMapping joinColMap = getJoinColumnMapping();
  auto resultPermutation = joinColMap.permutationResult();
  return createResult(
      requestLaziness,
      [this, scan = std::move(scan),
       resultWithIdTable = std::move(resultWithIdTable),
       joinColMap = std::move(joinColMap)](
          std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        const IdTable& idTable = resultWithIdTable->idTable();
        auto rowAdder = makeRowAdder(std::move(yieldTable));

        auto permutationIdTable = ad_utility::IdTableAndFirstCol{
            idTable.asColumnSubsetView(idTableIsRightInput
                                           ? joinColMap.permutationRight()
                                           : joinColMap.permutationLeft()),
            resultWithIdTable->getCopyOfLocalVocab()};

        ad_utility::Timer timer{
            ad_utility::timer::Timer::InitialStatus::Started};
        bool idTableHasUndef =
            !idTable.empty() &&
            idTable.at(0, idTableIsRightInput ? _rightJoinCol : _leftJoinCol)
                .isUndefined();
        std::optional<std::shared_ptr<const Result>> indexScanResult =
            std::nullopt;
        auto rightBlocks = [&scan, idTableHasUndef, &permutationIdTable,
                            &indexScanResult]()
            -> std::variant<LazyInputView, GeneratorWithDetails> {
          if (idTableHasUndef) {
            indexScanResult =
                scan->getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
            AD_CORRECTNESS_CHECK(
                !indexScanResult.value()->isFullyMaterialized());
            return convertGenerator(
                std::move(indexScanResult.value()->idTables()));
          } else {
            auto rightBlocksInternal =
                scan->lazyScanForJoinOfColumnWithScan(permutationIdTable.col());
            return convertGenerator(std::move(rightBlocksInternal));
          }
        }();

        runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());
        auto doJoin = [&rowAdder](auto& left, auto& right) mutable {
          // Note: The `zipperJoinForBlocksWithPotentialUndef` automatically
          // switches to a more efficient implementation if there are no UNDEF
          // values in any of the inputs.
          ad_utility::zipperJoinForBlocksWithPotentialUndef(
              left, right, std::less{}, rowAdder);
        };
        auto blockForIdTable = std::array{std::move(permutationIdTable)};
        std::visit(
            [&doJoin, &blockForIdTable](auto& blocks) {
              if constexpr (idTableIsRightInput) {
                doJoin(blocks, blockForIdTable);
              } else {
                doJoin(blockForIdTable, blocks);
              }
            },
            rightBlocks);

        if (std::holds_alternative<GeneratorWithDetails>(rightBlocks)) {
          scan->updateRuntimeInfoForLazyScan(
              std::get<GeneratorWithDetails>(rightBlocks).details());
        }

        auto localVocab = std::move(rowAdder.localVocab());
        return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                        std::move(localVocab)};
      },
      std::move(resultPermutation));
}

// ______________________________________________________________________________________________________
Result Join::computeResultForIndexScanAndLazyOperation(
    bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
    std::shared_ptr<IndexScan> scan) const {
  AD_CORRECTNESS_CHECK(_rightJoinCol == 0);

  ad_utility::JoinColumnMapping joinColMap = getJoinColumnMapping();
  auto resultPermutation = joinColMap.permutationResult();
  return createResult(
      requestLaziness,
      [this, scan = std::move(scan),
       resultWithIdTable = std::move(resultWithIdTable),
       joinColMap = std::move(joinColMap)](
          std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        auto rowAdder = makeRowAdder(std::move(yieldTable));

        auto [joinSide, indexScanSide] = scan->prefilterTables(
            std::move(resultWithIdTable->idTables()), _leftJoinCol);

        // Note: The `zipperJoinForBlocksWithPotentialUndef` automatically
        // switches to a more efficient implementation if there are no UNDEF
        // values in any of the inputs.
        zipperJoinForBlocksWithPotentialUndef(
            convertGenerator(std::move(joinSide), joinColMap.permutationLeft()),
            convertGenerator(std::move(indexScanSide),
                             joinColMap.permutationRight()),
            std::less{}, rowAdder);

        auto localVocab = std::move(rowAdder.localVocab());
        return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                        std::move(localVocab)};
      },
      std::move(resultPermutation));
}
// _____________________________________________________________________________
Result Join::computeResultForTwoMaterializedInputs(
    std::shared_ptr<const Result> leftRes,
    std::shared_ptr<const Result> rightRes) const {
  IdTable idTable{getResultWidth(), allocator()};
  join(leftRes->idTable(), rightRes->idTable(), &idTable);
  checkCancellation();

  return {std::move(idTable), resultSortedOn(),
          Result::getMergedLocalVocab(*leftRes, *rightRes)};
}

// _____________________________________________________________________________
Result Join::createEmptyResult() const {
  return {IdTable{getResultWidth(), allocator()}, resultSortedOn(),
          LocalVocab{}};
}

// _____________________________________________________________________________
ad_utility::JoinColumnMapping Join::getJoinColumnMapping() const {
  return ad_utility::JoinColumnMapping{{{_leftJoinCol, _rightJoinCol}},
                                       _left->getResultWidth(),
                                       _right->getResultWidth()};
}

// _____________________________________________________________________________
ad_utility::AddCombinedRowToIdTable Join::makeRowAdder(
    std::function<void(IdTable&, LocalVocab&)> callback) const {
  return ad_utility::AddCombinedRowToIdTable{
      1, IdTable{getResultWidth(), allocator()}, cancellationHandle_,
      CHUNK_SIZE, std::move(callback)};
}

// _____________________________________________________________________________
std::unique_ptr<Operation> Join::cloneImpl() const {
  auto copy = std::make_unique<Join>(*this);
  copy->_left = _left->clone();
  copy->_right = _right->clone();
  return copy;
}

// _____________________________________________________________________________
bool Join::columnOriginatesFromGraphOrUndef(const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  // For the join column we don't union the elements, we intersect them so we
  // can have a more efficient implementation.
  if (variable == _joinVar) {
    return doesJoinProduceGuaranteedGraphValuesOrUndef(_left, _right, variable);
  }
  return Operation::columnOriginatesFromGraphOrUndef(variable);
}
