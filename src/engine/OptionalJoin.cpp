// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "engine/OptionalJoin.h"

#include <absl/strings/str_join.h>

#include "engine/AddCombinedRowToTable.h"
#include "engine/CallFixedSize.h"
#include "engine/ColumnStrippingHelpers.h"
#include "engine/IndexScan.h"
#include "engine/JoinHelpers.h"
#include "engine/JoinWithIndexScanHelpers.h"
#include "engine/OptionalJoinConditionFilter.h"
#include "engine/Service.h"
#include "engine/Sort.h"
#include "global/RuntimeParameters.h"
#include "index/IdTableUtils.h"
#include "util/Algorithm.h"
#include "util/joinAlgorithms/IndexNestedLoopJoin.h"
#include "util/joinAlgorithms/JoinAlgorithms.h"

using namespace qlever::joinHelpers;
using namespace qlever::joinWithIndexScanHelpers;

using std::endl;
using std::string;

// _____________________________________________________________________________
OptionalJoin::OptionalJoin(QueryExecutionContext* qec,
                           std::shared_ptr<QueryExecutionTree> t1,
                           std::shared_ptr<QueryExecutionTree> t2,
                           bool keepJoinColumns, JoinCondition condition)
    : Operation(qec),
      _left{std::move(t1)},
      _right{std::move(t2)},
      _joinColumns(QueryExecutionTree::getJoinColumns(*_left, *_right)),
      keepJoinColumns_{keepJoinColumns},
      condition_{std::move(condition)} {
  // An OPTIONAL without shared variables and without a condition is handled by
  // the `QueryPlanner`, see `optionalJoinWithoutJoinColumns` for the other
  // case.
  AD_CORRECTNESS_CHECK(!_joinColumns.empty() || !condition_.empty());

  // The condition is evaluated on the joined rows, so the join columns have to
  // be kept if the condition uses one of the join variables.
  for (const auto& [leftCol, rightCol] : _joinColumns) {
    (void)rightCol;
    const auto& var = _left->getVariableAndInfoByColumnIndex(leftCol).first;
    if (ql::ranges::any_of(condition_, [&var](const auto& expression) {
          return expression.isVariableContained(var);
        })) {
      keepJoinColumns_ = true;
    }
  }

  // If `_right` contains no UNDEF in the join columns and at most one column in
  // `_left` contains UNDEF values, and that column is the last join column,
  // then a cheaper implementation can be used. The following code determines
  // whether only one join column in `_left` can contain UNDEF values and makes
  // this join column the last one.
  bool rightHasUndefColumn = false;
  size_t numUndefColumnsLeft = 0;
  ColumnIndex undefColumnLeftIndex = 0;
  for (size_t i = 0; i < _joinColumns.size(); ++i) {
    auto [leftCol, rightCol] = _joinColumns.at(i);
    auto leftIt = _left->getVariableAndInfoByColumnIndex(leftCol);
    auto rightIt = _right->getVariableAndInfoByColumnIndex(rightCol);
    if (leftIt.second.mightContainUndef_ ==
        ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined) {
      ++numUndefColumnsLeft;
      undefColumnLeftIndex = i;
    }
    if (rightIt.second.mightContainUndef_ ==
        ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined) {
      rightHasUndefColumn = true;
    }
  }
  if (!rightHasUndefColumn && numUndefColumnsLeft == 0) {
    implementation_ = Implementation::NoUndef;
  } else if (!rightHasUndefColumn && numUndefColumnsLeft == 1) {
    std::swap(_joinColumns.at(undefColumnLeftIndex), _joinColumns.back());
    implementation_ = Implementation::OnlyUndefInLastJoinColumnOfLeft;
  }

  // The inputs must be sorted by the join columns.
  std::tie(_left, _right) = QueryExecutionTree::createSortedTrees(
      std::move(_left), std::move(_right), _joinColumns);
}

// _____________________________________________________________________________
string OptionalJoin::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "OPTIONAL_JOIN\n";
  if (!keepJoinColumns_) {
    os << "Dropping join-columns\n";
  }
  os << _left->getCacheKey() << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][0] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]\n";
  os << "|X|\n" << _right->getCacheKey() << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][1] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]";
  if (!condition_.empty()) {
    auto varColMap = computeVariableToColumnMap();
    os << "\ncondition: ["
       << absl::StrJoin(condition_, " && ",
                        [&varColMap](std::string* out, const auto& expression) {
                          out->append(expression.getCacheKey(varColMap));
                        })
       << "]";
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
void OptionalJoin::onLimitOffsetChanged(const LimitOffsetClause&,
                                        bool childrenAreExclusivelyOwned) {
  // Note that we use the merged `getLimitOffset()` and not the clause that was
  // passed in, which only holds the increment that was just added. The bound
  // below depends on the total limit and offset, so for nested subqueries the
  // increment alone would be too small.
  const auto& limitOffset = getLimitOffset();
  if (limitOffset._limit.has_value()) {
    std::optional<uint64_t> safeLimit = std::nullopt;
    auto limit = limitOffset._limit.value();
    auto offset = limitOffset._offset;
    // We have to be careful to not cause an overflow when adding the offset and
    // the limit.
    if (limit <= std::numeric_limits<uint64_t>::max() - offset) {
      safeLimit = limit + offset;
    }
    // If we have a limit applied, we can apply limit + offset as a limit to the
    // left side, since the result of the optional join is at least as large as
    // the left side. This can significantly speed up the query if the left side
    // is large and the limit is small. The right side is optional, so reducing
    // it can drop matches; we leave it untouched.
    applyLimitOffsetToChild(_left, LimitOffsetClause{safeLimit},
                            childrenAreExclusivelyOwned);

    // The pushdown may have un-sorted `_left`, which our join algorithms
    // require to be sorted on the join columns, so restore that order (see the
    // caution note on `Operation::applyLimitOffset`). This is cheap, as `_left`
    // now yields at most `limit + offset` rows.
    _left = QueryExecutionTree::createSortedTree(std::move(_left),
                                                 leftJoinColumns());
  }
}

// _____________________________________________________________________________
std::vector<ColumnIndex> OptionalJoin::leftJoinColumns() const {
  std::vector<ColumnIndex> result;
  result.reserve(_joinColumns.size());
  ql::ranges::transform(_joinColumns, std::back_inserter(result),
                        [](const auto& cols) { return cols[0]; });
  return result;
}

// _____________________________________________________________________________
string OptionalJoin::getDescriptor() const {
  std::string joinVars;
  for (auto [leftCol, rightCol] : _joinColumns) {
    (void)rightCol;
    const std::string& varName =
        _left->getVariableAndInfoByColumnIndex(leftCol).first.name();
    joinVars += varName + " ";
  }
  std::string result = "OptionalJoin ";
  if (!joinVars.empty()) {
    result += "on " + joinVars;
  } else {
    result += "(Cartesian product) ";
  }
  if (!condition_.empty()) {
    result += "with condition " +
              absl::StrJoin(condition_, " && ",
                            [](std::string* out, const auto& expression) {
                              out->append(expression.getDescriptor());
                            });
  }
  return result;
}

// _____________________________________________________________________________
Result OptionalJoin::computeResult(bool requestLaziness) {
  AD_LOG_DEBUG << "OptionalJoin result computation..." << endl;

  // If the right of the RootOperations is a Service, precompute the result of
  // its sibling.
  Service::precomputeSiblingResult(_left->getRootOperation(),
                                   _right->getRootOperation(), true,
                                   requestLaziness);

  if (_joinColumns.empty()) {
    return optionalJoinWithoutJoinColumns(requestLaziness);
  }

  // With a condition, neither the result nor the left input are lazy: the
  // `ConditionFilter` needs unique left row indices, and a left row without a
  // match is only known after the whole join.
  if (!condition_.empty()) {
    requestLaziness = false;
  }

  if (auto res = tryIndexNestedLoopJoinIfSuitable(requestLaziness)) {
    return std::move(res).value();
  }

  // We can currently use a prefiltered version if all of the following are
  // true:
  // 1. It is allowed by the `RuntimeParameters`.
  // 2. The right child is an `IndexScan`.
  // 3. There either is a single join columns, or there are 2 join columns, and
  // only the second join column in the left input can have
  //    UNDEF values.
  // 4. `condition_` is empty, or there is a single join column. With 2 join
  //    columns, `specialOptionalJoinForBlocks` copies parts of the left input
  //    to temporary tables, so the row indices it reports don't refer to it.
  const bool isTwoColumnSpecialOptionalJoin =
      implementation_ == Implementation::OnlyUndefInLastJoinColumnOfLeft &&
      _joinColumns.size() == 2;
  if (getRuntimeParameter<&RuntimeParameters::prefilteredOptionalJoin_>() &&
      (_joinColumns.size() == 1 ||
       (isTwoColumnSpecialOptionalJoin && condition_.empty()))) {
    if (auto indexScan = IndexScan::getIfSupportsPrefilteredJoin(*_right)) {
      return optionalJoinWithIndexScan(_left->getResult(condition_.empty()),
                                       std::move(indexScan), requestLaziness);
    }
  }

  IdTable idTable{getResultWidth(), getExecutionContext()->getAllocator()};

  AD_CONTRACT_CHECK(idTable.numColumns() >= _joinColumns.size() ||
                    !keepJoinColumns_);
  // The optional join implementation does only work if there's just a single
  // join column. This might be extended in the future.
  bool lazyJoinIsSupported = _joinColumns.size() == 1;

  auto leftResult = _left->getResult(lazyJoinIsSupported && condition_.empty());
  auto rightResult = _right->getResult(lazyJoinIsSupported);

  checkCancellation();

  AD_LOG_DEBUG << "OptionalJoin subresult computation done." << std::endl;

  if (!leftResult->isFullyMaterialized() ||
      !rightResult->isFullyMaterialized()) {
    return lazyOptionalJoin(std::move(leftResult), std::move(rightResult),
                            requestLaziness);
  }

  AD_LOG_DEBUG << "Computing optional join between results of size "
               << leftResult->idTableView().size() << " and "
               << rightResult->idTableView().size() << endl;

  optionalJoin(leftResult->idTableView(), rightResult->idTableView(),
               _joinColumns, &idTable, implementation_);

  checkCancellation();

  AD_LOG_DEBUG << "OptionalJoin result computation done." << endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          Result::getMergedLocalVocab(*leftResult, *rightResult)};
}

// _____________________________________________________________________________
Result OptionalJoin::optionalJoinWithoutJoinColumns(bool requestLaziness) {
  AD_CORRECTNESS_CHECK(_joinColumns.empty() && !condition_.empty());
  auto left = _left->getResult(false);
  auto right = _right->getResult(false);
  checkCancellation();

  // Compute the result for the next block of rows of the left input, or
  // `std::nullopt` if there are no more rows. A block has as many left rows
  // as possible, such that it has at most `CHUNK_SIZE` pairs with the rows of
  // the right input (but at least one left row).
  // TODO<ninofrey> The condition is still evaluated for every pair of rows, so
  // the running time is O(n * m).
  auto getNextBlock = [this, left, right,
                       beginLeft =
                           size_t{0}]() mutable -> std::optional<IdTable> {
    const auto& leftTable = left->idTableView();
    const auto& rightTable = right->idTableView();
    if (beginLeft >= leftTable.size()) {
      return std::nullopt;
    }
    size_t numLeftRows = std::max(
        size_t{1}, CHUNK_SIZE / std::max(size_t{1}, rightTable.size()));
    size_t endLeft = std::min(leftTable.size(), beginLeft + numLeftRows);
    auto block =
        joinBlockWithoutJoinColumns(leftTable, rightTable, beginLeft, endLeft);
    beginLeft = endLeft;
    return block;
  };

  if (requestLaziness) {
    auto localVocab = std::make_shared<LocalVocab>(left->getCopyOfLocalVocab());
    localVocab->mergeWith(right->localVocab());
    auto get = [getNextBlock = std::move(getNextBlock),
                localVocab = std::move(localVocab)]() mutable
        -> std::optional<Result::IdTableVocabPair> {
      auto block = getNextBlock();
      if (!block.has_value()) {
        return std::nullopt;
      }
      return Result::IdTableVocabPair{std::move(block).value(),
                                      localVocab->clone()};
    };
    return {Result::LazyResult{
                ad_utility::InputRangeFromGetCallable{std::move(get)}},
            resultSortedOn()};
  }

  IdTable result{getResultWidth(), allocator()};
  while (auto block = getNextBlock()) {
    result.insertAtEnd(block.value());
  }
  return {std::move(result), resultSortedOn(),
          Result::getMergedLocalVocab(*left, *right)};
}

// _____________________________________________________________________________
IdTable OptionalJoin::joinBlockWithoutJoinColumns(IdTableView<0> left,
                                                  IdTableView<0> right,
                                                  size_t beginLeft,
                                                  size_t endLeft) const {
  ad_utility::AddCombinedRowToIdTable rowAdder{
      0,
      left,
      right,
      IdTable{getResultWidth(), allocator()},
      cancellationHandle(),
      true,
      conditionChunkSize_};
  ConditionFilter conditionFilter{*this, left, rowAdder, beginLeft, endLeft};
  auto leftRows = ql::views::iota(beginLeft, endLeft);
  if (right.empty()) {
    ql::ranges::for_each(
        leftRows, [&rowAdder](size_t row) { rowAdder.addOptionalRow(row); });
  } else {
    rowAdder.addRows(leftRows, ql::views::iota(size_t{0}, right.size()));
  }
  IdTable block = std::move(rowAdder).resultTable();
  // Without join columns the order of the rows never changes.
  conditionFilter.finish(block);
  return block;
}

// _____________________________________________________________________________
VariableToColumnMap OptionalJoin::computeVariableToColumnMap() const {
  return makeVarToColMapForJoinOperation(
      _left->getVariableColumns(), _right->getVariableColumns(), _joinColumns,
      BinOpType::OptionalJoin, _left->getResultWidth(), keepJoinColumns_);
}

// _____________________________________________________________________________
size_t OptionalJoin::getResultWidth() const {
  size_t res =
      _left->getResultWidth() + _right->getResultWidth() - _joinColumns.size();
  AD_CONTRACT_CHECK(res > 0);
  if (!keepJoinColumns_) {
    res -= _joinColumns.size();
  }
  return res;
}

// _____________________________________________________________________________
qlm::vector<ColumnIndex> OptionalJoin::resultSortedOn() const {
  if (!keepJoinColumns_) {
    return qlm::vector<ColumnIndex>{allocator()};
  }
  qlm::vector<ColumnIndex> sortedOn{allocator()};
  // This optimization doesn't allow preserving sort order.
  if (isIndexNestedLoopJoinSuitable()) {
    return sortedOn;
  }
  // The result is sorted on all join columns from the left subtree.
  for (const auto& [joinColumnLeft, joinColumnRight] : _joinColumns) {
    (void)joinColumnRight;
    sortedOn.push_back(joinColumnLeft);
  }
  return sortedOn;
}

// _____________________________________________________________________________
float OptionalJoin::getMultiplicity(size_t col) {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _multiplicities[col];
}

// _____________________________________________________________________________
uint64_t OptionalJoin::getSizeEstimateBeforeLimit() {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t OptionalJoin::getCostEstimate() {
  if (!_costEstimate.has_value()) {
    size_t costEstimate = getSizeEstimateBeforeLimit() +
                          _left->getSizeEstimate() + _right->getSizeEstimate();
    // The optional join is about 3-7 times slower than a normal join, due to
    // its increased complexity
    costEstimate *= 4;
    // Make the join 7% more expensive per join column
    if (!_joinColumns.empty()) {
      costEstimate *= (1 + (_joinColumns.size() - 1) * 0.07);
    }

    _costEstimate =
        _left->getCostEstimate() + _right->getCostEstimate() + costEstimate;
  }
  return _costEstimate.value();
}

// _____________________________________________________________________________
void OptionalJoin::computeSizeEstimateAndMultiplicities() {
  if (_joinColumns.empty()) {
    // Without join columns, each left row yields at most one row per right
    // row (or a single row with UNDEF), so the Cartesian product is an
    // upper bound.
    size_t sizeLeft = _left->getSizeEstimate();
    size_t sizeRight = std::max(size_t{1}, _right->getSizeEstimate());
    _sizeEstimate = sizeLeft * sizeRight + 1;
    _multiplicities.clear();
    for (size_t i = 0; i < _left->getResultWidth(); i++) {
      _multiplicities.push_back(_left->getMultiplicity(i) * sizeRight);
    }
    for (size_t i = 0; i < _right->getResultWidth(); i++) {
      _multiplicities.push_back(_right->getMultiplicity(i) *
                                std::max(size_t{1}, sizeLeft));
    }
    _multiplicitiesComputed = true;
    return;
  }
  // The number of distinct entries in the result is at most the minimum of
  // the numbers of distinc entries in all join columns.
  // The multiplicity in the result is approximated by the product of the
  // maximum of the multiplicities of each side.

  // compute the minimum number of distinct elements in the join columns
  size_t numDistinctLeft = std::numeric_limits<size_t>::max();
  size_t numDistinctRight = std::numeric_limits<size_t>::max();
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    size_t dl = std::max(1.0f, _left->getSizeEstimate() /
                                   _left->getMultiplicity(_joinColumns[i][0]));
    size_t dr = std::max(1.0f, _right->getSizeEstimate() /
                                   _right->getMultiplicity(_joinColumns[i][1]));
    numDistinctLeft = std::min(numDistinctLeft, dl);
    numDistinctRight = std::min(numDistinctRight, dr);
  }
  size_t numDistinctResult = std::min(numDistinctLeft, numDistinctRight);
  // The number of distinct is at least the number of distinct in a non optional
  // column, if the other one is optional.
  numDistinctRight = std::max(numDistinctRight, numDistinctResult);

  // compute an estimate for the results multiplicity
  float multLeft = std::numeric_limits<float>::max();
  float multRight = std::numeric_limits<float>::max();
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    multLeft = std::min(multLeft, _left->getMultiplicity(_joinColumns[i][0]));
    multRight =
        std::min(multRight, _right->getMultiplicity(_joinColumns[i][1]));
  }
  float multResult = multLeft * multRight;

  _sizeEstimate = _left->getSizeEstimate() * multResult;

  // Don't estimate 0 since then some parent operations
  // (in particular joins) using isKnownEmpty() will
  // will assume the size to be exactly zero
  _sizeEstimate += 1;

  // compute estimates for the multiplicities of the result columns
  _multiplicities.clear();

  auto jcolsLeft = _joinColumns | ql::views::transform(ad_utility::first);
  for (size_t i = 0; i < _left->getResultWidth(); i++) {
    float mult = _left->getMultiplicity(i) * (multResult / multLeft);
    if (keepJoinColumns_ || !ad_utility::contains(jcolsLeft, i)) {
      _multiplicities.push_back(mult);
    }
  }

  auto jcolsRight = _joinColumns | ql::views::transform(ad_utility::second);
  for (size_t i = 0; i < _right->getResultWidth(); i++) {
    if (ad_utility::contains(jcolsRight, i)) {
      continue;
    }
    float mult = _right->getMultiplicity(i) * (multResult / multRight);
    _multiplicities.push_back(mult);
  }
  _multiplicitiesComputed = true;
}

// ______________________________________________________________
auto OptionalJoin::computeImplementationFromIdTables(
    const IdTableView<0>& left, const IdTableView<0>& right,
    ql::span<const std::array<ColumnIndex, 2>> joinColumns) -> Implementation {
  auto implementation = Implementation::NoUndef;
  auto anyIsUndefined = [](auto column) {
    return ql::ranges::any_of(column, Id::isUndefinedL);
  };
  for (size_t i = 0; i < joinColumns.size(); ++i) {
    auto [leftCol, rightCol] = joinColumns[i];
    if (anyIsUndefined(right.getColumn(rightCol))) {
      return Implementation::GeneralCase;
    }
    if (anyIsUndefined(left.getColumn(leftCol))) {
      if (i == joinColumns.size() - 1) {
        implementation = Implementation::OnlyUndefInLastJoinColumnOfLeft;
      } else {
        return Implementation::GeneralCase;
      }
    }
  }
  return implementation;
}

// _____________________________________________________________________________
bool OptionalJoin::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  // For the join columns, we only need to look at the left side of the tree for
  // this check if it doesn't contain undefined values.
  if (_left->getVariableColumnOrNullopt(variable).has_value() &&
      _right->getVariableColumnOrNullopt(variable).has_value()) {
    return _left->getRootOperation()->columnOriginatesFromGraphOrUndef(
               variable) &&
           (_left->getVariableColumns().at(variable).mightContainUndef_ ==
                ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined ||
            _right->getRootOperation()->columnOriginatesFromGraphOrUndef(
                variable));
  }
  // For all other columns, we just delegate to the default implementation
  return Operation::columnOriginatesFromGraphOrUndef(variable);
}

// ______________________________________________________________
void OptionalJoin::optionalJoin(
    const IdTableView<0>& left, const IdTableView<0>& right,
    ql::span<const std::array<ColumnIndex, 2>> joinColumns, IdTable* result,
    Implementation implementation) {
  // check for trivial cases
  if (left.empty()) {
    return;
  }

  // If we cannot determine statically whether a cheaper implementation can
  // be chosen, we try to determine this dynamically by checking all the join
  // columns for UNDEF values.
  if (implementation == Implementation::GeneralCase) {
    implementation =
        computeImplementationFromIdTables(left, right, joinColumns);
  }

  ad_utility::JoinColumnMapping joinColumnData{
      joinColumns, left.numColumns(), right.numColumns(), keepJoinColumns_};

  IdTableView<0> joinColumnsLeft =
      left.asColumnSubsetView(joinColumnData.jcsLeft());
  IdTableView<0> joinColumnsRight =
      right.asColumnSubsetView(joinColumnData.jcsRight());

  checkCancellation();

  auto leftPermuted = left.asColumnSubsetView(joinColumnData.permutationLeft());
  auto rightPermuted =
      right.asColumnSubsetView(joinColumnData.permutationRight());

  auto lessThanBoth = ql::ranges::lexicographical_compare;

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(
      joinColumns.size(), leftPermuted, rightPermuted, std::move(*result),
      cancellationHandle(), keepJoinColumns_, conditionChunkSize_);
  // With a condition, the pairs for which it doesn't hold are removed each
  // time the `rowAdder` has written a chunk of rows.
  std::optional<ConditionFilter> conditionFilter;
  if (!condition_.empty()) {
    conditionFilter.emplace(*this, leftPermuted, rowAdder);
  }
  auto rowAdderOnIterators = [&]() {
    auto addRow = [&rowAdder, beginLeft = joinColumnsLeft.begin(),
                   beginRight = joinColumnsRight.begin()](const auto& itLeft,
                                                          const auto& itRight) {
      rowAdder.addRow(itLeft - beginLeft, itRight - beginRight);
    };
    auto addRows = [&rowAdder, beginLeft = joinColumnsLeft.begin(),
                    beginRight = joinColumnsRight.begin()](
                       const auto& itLeft, const auto& endLeft,
                       const auto& itRight, const auto& endRight) {
      auto getRng = [](const auto& it, const auto& end, const auto& beg) {
        return ql::views::iota(static_cast<size_t>(it - beg),
                               static_cast<size_t>(end - beg));
      };
      rowAdder.addRows(getRng(itLeft, endLeft, beginLeft),
                       getRng(itRight, endRight, beginRight));
    };
    return ad_utility::detail::RowIndexAdder{std::move(addRow),
                                             std::move(addRows)};
  }();

  auto addOptionalRow = [&rowAdder,
                         begin = joinColumnsLeft.begin()](const auto& itLeft) {
    rowAdder.addOptionalRow(itLeft - begin);
  };

  auto findUndefDispatch = [](const auto& row, auto begin, auto end,
                              bool& outOfOrder) {
    return ad_utility::findSmallerUndefRanges(row, begin, end, outOfOrder);
  };

  const size_t numOutOfOrder = [&]() {
    auto checkCancellationLambda = cancellationChecker();
    if (implementation == Implementation::OnlyUndefInLastJoinColumnOfLeft) {
      ad_utility::specialOptionalJoin(joinColumns.size(), joinColumnsLeft,
                                      joinColumnsRight, rowAdderOnIterators,
                                      addOptionalRow, checkCancellationLambda);
      return size_t{0};
    } else if (implementation == Implementation::NoUndef) {
      if (right.size() / left.size() > GALLOP_THRESHOLD) {
        ad_utility::gallopingJoin(joinColumnsLeft, joinColumnsRight,
                                  lessThanBoth, rowAdderOnIterators,
                                  addOptionalRow, checkCancellationLambda);
      } else {
        auto shouldBeZero = ad_utility::zipperJoinWithUndef(
            joinColumnsLeft, joinColumnsRight, lessThanBoth,
            rowAdderOnIterators, ad_utility::noop, ad_utility::noop,
            addOptionalRow, checkCancellationLambda);
        AD_CORRECTNESS_CHECK(shouldBeZero == size_t{0});
      }
      return size_t{0};
    } else {
      return ad_utility::zipperJoinWithUndef(
          joinColumnsLeft, joinColumnsRight, lessThanBoth, rowAdderOnIterators,
          findUndefDispatch, findUndefDispatch, addOptionalRow,
          checkCancellationLambda);
    }
  }();
  // The column order in the result is now
  // [joinColumns, non-join-columns-a, non-join-columns-b] (which makes the
  // algorithms above easier), be the order that is expected by the rest of the
  // code is [columns-a, non-join-columns-b]. Permute the columns to fix the
  // order.
  *result = std::move(rowAdder).resultTable();
  bool conditionChangedOrder = false;
  if (conditionFilter.has_value()) {
    conditionChangedOrder = conditionFilter->finish(*result);
  }

  // TODO<joka921> We have two sorted ranges, a simple merge would suffice
  // (possibly even in-place), or we could even lazily pass them on to the
  // upstream operation.
  // Note: the merging only works if we don't have the arbitrary out of order
  // case.
  // TODO<joka921> We only have to do this if the sorting is required.
  if ((numOutOfOrder > 0 || conditionChangedOrder) && keepJoinColumns_) {
    std::vector<ColumnIndex> cols;
    for (size_t i = 0; i < joinColumns.size(); ++i) {
      cols.push_back(i);
    }
    checkCancellation();
    IdTableUtils::sort(*result, cols, sortConfig());
  }
  result->setColumnSubset(joinColumnData.permutationResult());
  checkCancellation();
}

// _____________________________________________________________________________
Result OptionalJoin::lazyOptionalJoin(std::shared_ptr<const Result> left,
                                      std::shared_ptr<const Result> right,
                                      bool requestLaziness) {
  // If both inputs are fully materialized, we can join them more
  // efficiently.
  AD_CONTRACT_CHECK(!left->isFullyMaterialized() ||
                    !right->isFullyMaterialized());
  // Currently only supports a single join column.
  AD_CORRECTNESS_CHECK(_joinColumns.size() == 1);
  ad_utility::JoinColumnMapping joinColMap{
      _joinColumns, _left->getResultWidth(), _right->getResultWidth(),
      keepJoinColumns_};

  auto resultPermutation = joinColMap.permutationResult();

  auto action = [this, left = std::move(left), right = std::move(right),
                 joinColMap = std::move(joinColMap)](
                    std::function<void(IdTable&, LocalVocab&)> yieldTable) {
    auto rowAdder = getRowAdderForJoin(*this, _joinColumns.size(),
                                       keepJoinColumns_, std::move(yieldTable));
    // With a condition the result is never lazy, so instead of yielding the
    // result the callback of the row adder is used to apply the condition.
    std::optional<ConditionFilter> conditionFilter;
    if (!condition_.empty()) {
      AD_CORRECTNESS_CHECK(left->isFullyMaterialized());
      conditionFilter.emplace(
          *this,
          left->idTableView().asColumnSubsetView(joinColMap.permutationLeft()),
          rowAdder);
    }
    auto leftRange = resultToView(*left, joinColMap.permutationLeft());
    auto rightRange = resultToView(*right, joinColMap.permutationRight());
    std::visit(
        [&rowAdder](auto& leftBlocks, auto& rightBlocks) {
          ad_utility::zipperJoinForBlocksWithPotentialUndef(
              leftBlocks, rightBlocks, std::less{}, rowAdder, {}, {},
              ad_utility::OptionalJoinTag{});
        },
        leftRange, rightRange);
    auto localVocab = std::move(rowAdder.localVocab());
    Result::IdTableVocabPair result{std::move(rowAdder).resultTable(),
                                    std::move(localVocab)};
    if (conditionFilter.has_value()) {
      conditionFilter->finishAndRestoreOrder(result.idTable_);
    }
    return result;
  };
  return createResultFromAction(requestLaziness, std::move(action),
                                resultSortedOn(), std::move(resultPermutation));
}
// _____________________________________________________________________________
Result OptionalJoin::optionalJoinWithIndexScan(
    std::shared_ptr<const Result> left, std::shared_ptr<IndexScan> rightScan,
    bool requestLaziness) {
  // Currently only supports a single join column.
  AD_CORRECTNESS_CHECK(_joinColumns.size() == 1 ||
                       implementation_ ==
                           Implementation::OnlyUndefInLastJoinColumnOfLeft);
  ad_utility::JoinColumnMapping joinColMap{
      _joinColumns, _left->getResultWidth(), _right->getResultWidth(),
      keepJoinColumns_};

  auto resultPermutation = joinColMap.permutationResult();

  using namespace ad_utility::use_value_identity;
  auto getAction = [&](auto leftIsMaterializedV) {
    static constexpr bool leftIsMaterialized = leftIsMaterializedV;
    return [this, left = std::move(left), rightScan = std::move(rightScan),
            joinColMap = std::move(joinColMap)](
               std::function<void(IdTable&, LocalVocab&)> yieldTable) {
      auto rowAdder = getRowAdderForJoin(
          *this, _joinColumns.size(), keepJoinColumns_, std::move(yieldTable));
      // See `lazyOptionalJoin` for the role of the `conditionFilter`.
      std::optional<ConditionFilter> conditionFilter;
      if (!condition_.empty()) {
        AD_CORRECTNESS_CHECK(leftIsMaterialized);
        conditionFilter.emplace(*this,
                                left->idTableView().asColumnSubsetView(
                                    joinColMap.permutationLeft()),
                                rowAdder);
      }
      auto getLeftAndRightRange = [&]<size_t numJoinCols>() {
        auto firstJoinColLeft = _joinColumns.at(0).at(0);
        if constexpr (leftIsMaterialized) {
          auto rightBlocksInternal = rightScan->lazyScanForJoinOfColumnWithScan(
              left->idTableView().getColumn(firstJoinColLeft));
          auto rightRange = convertGeneratorFromScan<numJoinCols>(
              std::move(rightBlocksInternal), *rightScan);
          auto permutationIdTable =
              ad_utility::IdTableAndFirstCols<numJoinCols, IdTableView<0>>{
                  left->idTableView().asColumnSubsetView(
                      joinColMap.permutationLeft()),
                  left->getCopyOfLocalVocab()};
          auto leftRange = std::array{std::move(permutationIdTable)};

          return std::pair{std::move(leftRange), std::move(rightRange)};
        } else {
          auto [leftJoinSide, indexScanSide] = rightScan->prefilterTables(
              left->idTables(), firstJoinColLeft, false);
          auto leftRange =
              convertGenerator<std::decay_t<decltype(leftJoinSide)>,
                               numJoinCols>(std::move(leftJoinSide),
                                            joinColMap.permutationLeft());
          auto rightRange =
              convertGenerator<std::decay_t<decltype(indexScanSide)>,
                               numJoinCols>(std::move(indexScanSide),
                                            joinColMap.permutationRight());
          return std::pair{std::move(leftRange), std::move(rightRange)};
        }
      };
      if (_joinColumns.size() == 1) {
        // Note: The `zipperJoinForBlocksWithPotentialUndef` automatically
        // switches to a more efficient implementation if there are no UNDEF
        // values in any of the inputs.
        auto [leftRange, rightRange] =
            getLeftAndRightRange.template operator()<1>();
        zipperJoinForBlocksWithPotentialUndef(
            std::move(leftRange), std::move(rightRange), std::less{}, rowAdder,
            {}, {}, ad_utility::OptionalJoinTag{});
      } else {
        AD_CORRECTNESS_CHECK(implementation_ ==
                             Implementation::OnlyUndefInLastJoinColumnOfLeft);
        auto [leftRange, rightRange] =
            getLeftAndRightRange.template operator()<2>();
        specialOptionalJoinForBlocks(
            std::move(leftRange), std::move(rightRange),
            std::integral_constant<size_t, 2>{}, rowAdder);
      }
      setScanStatusToLazilyCompleted(*rightScan);
      auto result = std::move(rowAdder).toIdTableVocabPair();
      if (conditionFilter.has_value()) {
        conditionFilter->finishAndRestoreOrder(result.idTable_);
      }
      return result;
    };
  };

  auto createResult = [&](auto isMaterialized) {
    return createResultFromAction(requestLaziness, getAction(isMaterialized),
                                  resultSortedOn(),
                                  std::move(resultPermutation));
  };
  return left->isFullyMaterialized() ? createResult(vi<true>)
                                     : createResult(vi<false>);
}

// _____________________________________________________________________________
std::unique_ptr<Operation> OptionalJoin::cloneImpl() const {
  auto copy = std::make_unique<OptionalJoin>(*this);
  copy->_left = _left->clone();
  copy->_right = _right->clone();
  return copy;
}

// _____________________________________________________________________________
bool OptionalJoin::isIndexNestedLoopJoinSuitable() const {
  auto alwaysDefined = [this]() {
    return qlever::joinHelpers::joinColumnsAreAlwaysDefined(_joinColumns, _left,
                                                            _right);
  };
  // This algorithm only works well if the left side is smaller and we can avoid
  // sorting the right side. It currently doesn't support undef.
  return condition_.empty() &&
         std::dynamic_pointer_cast<Sort>(_right->getRootOperation()) &&
         _left->getSizeEstimate() <= _right->getSizeEstimate() &&
         alwaysDefined();
}

// _____________________________________________________________________________
std::optional<Result> OptionalJoin::tryIndexNestedLoopJoinIfSuitable(
    bool requestLaziness) {
  if (!isIndexNestedLoopJoinSuitable()) {
    return std::nullopt;
  }
  auto leftRes = _left->getResult(false);
  auto rightRes = computeResultSkipChild(_right->getRootOperation(), true);

  LocalVocab localVocab = leftRes->getCopyOfLocalVocab();
  ::joinAlgorithms::indexNestedLoop::IndexNestedLoopJoin nestedLoopJoin{
      _joinColumns, std::move(leftRes), std::move(rightRes),
      cancellationHandle()};

  // This algorithm doesn't produce sorted output
  AD_CORRECTNESS_CHECK(resultSortedOn().empty());

  auto lazyResult =
      std::move(nestedLoopJoin)
          .computeOptionalJoin(!requestLaziness, getResultWidth(),
                               _right->getResultWidth(), keepJoinColumns_);
  return requestLaziness
             ? Result{std::move(lazyResult), resultSortedOn()}
             : Result{ad_utility::getSingleElement(std::move(lazyResult)),
                      resultSortedOn()};
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
OptionalJoin::makeTreeWithStrippedColumns(
    const std::set<Variable>& variables) const {
  // The children have to provide the requested variables, the variables of
  // the condition, and the join columns.
  columnStrippingHelpers::VarsRequiredFromSubtree vars{&variables};
  using AddVarsResult =
      columnStrippingHelpers::VarsRequiredFromSubtree::AddVarsResult;
  for (const auto& expression : condition_) {
    AD_CORRECTNESS_CHECK(
        vars.addVarsFromExpression(expression) == AddVarsResult::Success,
        "Aggregate functions are not allowed in a `FILTER` clause");
  }
  for (const auto& [jcl, _] : _joinColumns) {
    vars.add(_left->getVariableAndInfoByColumnIndex(jcl).first);
  }
  auto left = vars.stripSubtree(_left);
  auto right = vars.stripSubtree(_right);

  // TODO<joka921> The following could be done more efficiently in a constructor
  // (like this it is done twice).
  auto jcls = QueryExecutionTree::getJoinColumns(*_left, *_right);
  bool keepJoinColumns = ql::ranges::any_of(jcls, [&](const auto& jcl) {
    const auto& var = _left->getVariableAndInfoByColumnIndex(jcl[0]).first;
    return ad_utility::contains(variables, var);
  });
  // With a condition, the new `OptionalJoin` also exports the variables of the
  // condition that the parent did not request, the helper adds a
  // `StripColumns` operation for them.
  return columnStrippingHelpers::makeTreeWithOptionalStripOperation<
      OptionalJoin>(getExecutionContext(), variables, std::move(left),
                    std::move(right), keepJoinColumns, condition_);
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
OptionalJoin::makeTreeWithBindColumn(const parsedQuery::Bind& bind) const {
  // The `BIND` can only be pushed into the left (non-optional) child.
  //
  // NOTE: For left rows without a match, `OptionalJoin` fills all columns of
  // the right child with `UNDEF`, including a pushed-down `BIND` column,
  // instead of evaluating the `BIND` expression on the unbound input. This
  // silently changes the result for any expression that isn't `UNDEF` itself
  // on `UNDEF` input, e.g. `COALESCE`.
  return pushDownBindToChild(bind, {{_left, _right}, allocator()}, 0);
}

// _____________________________________________________________________________
std::unique_ptr<Operation> OptionalJoin::cloneWithNewChildren(
    qlm::vector<std::shared_ptr<QueryExecutionTree>> children) const {
  return std::make_unique<OptionalJoin>(
      _executionContext, std::move(children.at(0)), std::move(children.at(1)),
      keepJoinColumns_, condition_);
}

// _____________________________________________________________________________
VariableToColumnMap OptionalJoin::getInternalVariableColumns() const {
  ad_utility::JoinColumnMapping mapping{_joinColumns, _left->getResultWidth(),
                                        _right->getResultWidth(),
                                        keepJoinColumns_};
  const auto& permutation = mapping.permutationResult();
  VariableToColumnMap result = computeVariableToColumnMap();
  for (auto& info : result | ql::views::values) {
    info.columnIndex_ = permutation.at(info.columnIndex_);
  }
  return result;
}
