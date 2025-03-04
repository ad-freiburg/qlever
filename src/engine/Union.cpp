// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2018     Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//   2022-    Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#include "Union.h"

#include "engine/CallFixedSize.h"
#include "util/ChunkedForLoop.h"
#include "util/CompilerWarnings.h"

const size_t Union::NO_COLUMN = std::numeric_limits<size_t>::max();

Union::Union(QueryExecutionContext* qec,
             const std::shared_ptr<QueryExecutionTree>& t1,
             const std::shared_ptr<QueryExecutionTree>& t2,
             std::vector<ColumnIndex> targetOrder)
    : Operation(qec), targetOrder_{std::move(targetOrder)} {
  AD_CONTRACT_CHECK(t1 && t2);
  _subtrees[0] = t1;
  _subtrees[1] = t2;

  // compute the column origins
  const VariableToColumnMap& variableColumns =
      getInternallyVisibleVariableColumns();
  _columnOrigins.resize(variableColumns.size(), {NO_COLUMN, NO_COLUMN});
  const auto& t1VarCols = t1->getVariableColumns();
  const auto& t2VarCols = t2->getVariableColumns();
  for (auto it : variableColumns) {
    // look for the corresponding column in t1
    auto it1 = t1VarCols.find(it.first);
    if (it1 != t1VarCols.end()) {
      _columnOrigins[it.second.columnIndex_][0] = it1->second.columnIndex_;
    } else {
      _columnOrigins[it.second.columnIndex_][0] = NO_COLUMN;
    }

    // look for the corresponding column in t2
    const auto it2 = t2VarCols.find(it.first);
    if (it2 != t2VarCols.end()) {
      _columnOrigins[it.second.columnIndex_][1] = it2->second.columnIndex_;
    } else {
      _columnOrigins[it.second.columnIndex_][1] = NO_COLUMN;
    }
  }
  AD_CORRECTNESS_CHECK(ql::ranges::all_of(_columnOrigins, [](const auto& el) {
    return el[0] != NO_COLUMN || el[1] != NO_COLUMN;
  }));

  if (!targetOrder_.empty()) {
    auto computeSortOrder = [this](bool left) {
      vector<ColumnIndex> specificSortOrder;
      for (ColumnIndex index : targetOrder_) {
        ColumnIndex realIndex = _columnOrigins.at(index).at(!left);
        if (realIndex != NO_COLUMN) {
          specificSortOrder.push_back(realIndex);
        }
      }
      return specificSortOrder;
    };

    _subtrees[0] = QueryExecutionTree::createSortedTree(std::move(_subtrees[0]),
                                                        computeSortOrder(true));
    _subtrees[1] = QueryExecutionTree::createSortedTree(
        std::move(_subtrees[1]), computeSortOrder(false));

    // Swap children to get cheaper computation
    if (_columnOrigins.at(targetOrder_.at(0)).at(1) == NO_COLUMN) {
      std::swap(_subtrees[0], _subtrees[1]);
      ql::ranges::for_each(_columnOrigins,
                           [](auto& el) { std::swap(el[0], el[1]); });
    }
  }
}

string Union::getCacheKeyImpl() const {
  std::ostringstream os;
  os << _subtrees[0]->getCacheKey() << "\n";
  os << "UNION\n";
  os << _subtrees[1]->getCacheKey() << "\n";
  os << "sort order: ";
  for (size_t i : targetOrder_) {
    os << i << " ";
  }
  return std::move(os).str();
}

string Union::getDescriptor() const { return "Union"; }

size_t Union::getResultWidth() const {
  // The width depends on the number of unique variables (as the columns of
  // two variables from the left and right subtree will end up in the same
  // result column if they have the same name).
  return _columnOrigins.size();
}

vector<ColumnIndex> Union::resultSortedOn() const { return targetOrder_; }

// _____________________________________________________________________________
VariableToColumnMap Union::computeVariableToColumnMap() const {
  using VarAndTypeInfo = std::pair<Variable, ColumnIndexAndTypeInfo>;

  VariableToColumnMap variableColumns;

  // A variable is only guaranteed to always be bound if it exists in all the
  // subtrees and if it is guaranteed to be bound in all the subtrees.
  auto mightContainUndef = [this](const Variable& var) {
    return ql::ranges::any_of(
        _subtrees, [&](const std::shared_ptr<QueryExecutionTree>& subtree) {
          const auto& varCols = subtree->getVariableColumns();
          return !varCols.contains(var) ||
                 (varCols.at(var).mightContainUndef_ ==
                  ColumnIndexAndTypeInfo::PossiblyUndefined);
        });
  };

  // Note: it is tempting to declare `nextColumnIndex` inside the lambda
  // `addVariableColumnIfNotExists`, but that doesn't work because
  // `ql::ranges::for_each` takes the lambda by value and creates a new
  // variable at every invocation.
  size_t nextColumnIndex = 0;
  auto addVariableColumnIfNotExists =
      [&mightContainUndef, &variableColumns,
       &nextColumnIndex](const VarAndTypeInfo& varAndIndex) {
        const auto& variable = varAndIndex.first;
        if (!variableColumns.contains(variable)) {
          using enum ColumnIndexAndTypeInfo::UndefStatus;
          variableColumns[variable] = ColumnIndexAndTypeInfo{
              nextColumnIndex,
              mightContainUndef(variable) ? PossiblyUndefined : AlwaysDefined};
          nextColumnIndex++;
        }
      };

  auto addVariablesForSubtree = [&addVariableColumnIfNotExists](
                                    const auto& subtree) {
    ql::ranges::for_each(copySortedByColumnIndex(subtree->getVariableColumns()),
                         addVariableColumnIfNotExists);
  };

  ql::ranges::for_each(_subtrees, addVariablesForSubtree);
  return variableColumns;
}

bool Union::knownEmptyResult() {
  return _subtrees[0]->knownEmptyResult() && _subtrees[1]->knownEmptyResult();
}

float Union::getMultiplicity(size_t col) {
  if (col >= _columnOrigins.size()) {
    return 1;
  }
  if (_columnOrigins[col][0] != NO_COLUMN &&
      _columnOrigins[col][1] != NO_COLUMN) {
    return (_subtrees[0]->getMultiplicity(_columnOrigins[col][0]) +
            _subtrees[1]->getMultiplicity(_columnOrigins[col][1])) /
           2;
  } else if (_columnOrigins[col][0] != NO_COLUMN) {
    // Compute the number of distinct elements in the input, add one new element
    // for the unbound variables, then divide it by the number of elements in
    // the result. This is slightly off if the subresult already contained
    // an unbound result, but the error is small in most common cases.
    double numDistinct = _subtrees[0]->getSizeEstimate() /
                         static_cast<double>(_subtrees[0]->getMultiplicity(
                             _columnOrigins[col][0]));
    numDistinct += 1;
    return getSizeEstimateBeforeLimit() / numDistinct;
  } else if (_columnOrigins[col][1] != NO_COLUMN) {
    // Compute the number of distinct elements in the input, add one new element
    // for the unbound variables, then divide it by the number of elements in
    // the result. This is slightly off if the subresult already contained
    // an unbound result, but the error is small in most common cases.
    double numDistinct = _subtrees[1]->getSizeEstimate() /
                         static_cast<double>(_subtrees[1]->getMultiplicity(
                             _columnOrigins[col][1]));
    numDistinct += 1;
    return getSizeEstimateBeforeLimit() / numDistinct;
  }
  return 1;
}

uint64_t Union::getSizeEstimateBeforeLimit() {
  return _subtrees[0]->getSizeEstimate() + _subtrees[1]->getSizeEstimate();
}

size_t Union::getCostEstimate() {
  return _subtrees[0]->getCostEstimate() + _subtrees[1]->getCostEstimate() +
         getSizeEstimateBeforeLimit();
}

ProtoResult Union::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "Union result computation..." << std::endl;
  std::shared_ptr<const Result> subRes1 =
      _subtrees[0]->getResult(requestLaziness);
  std::shared_ptr<const Result> subRes2 =
      _subtrees[1]->getResult(requestLaziness);

  // If first sort column is not present in left child, we can fall back to the
  // cheap computation because it orders the left child first.
  if (!targetOrder_.empty() &&
      _columnOrigins.at(targetOrder_.at(0)).at(0) != NO_COLUMN) {
    auto generator = computeResultKeepOrder(requestLaziness, std::move(subRes1),
                                            std::move(subRes2));
    return requestLaziness ? ProtoResult{std::move(generator), resultSortedOn()}
                           : ProtoResult{getSingleElement(std::move(generator)),
                                         resultSortedOn()};
  }

  if (requestLaziness) {
    return {computeResultLazily(std::move(subRes1), std::move(subRes2)),
            resultSortedOn()};
  }

  LOG(DEBUG) << "Union subresult computation done." << std::endl;

  IdTable idTable =
      computeUnion(subRes1->idTable(), subRes2->idTable(), _columnOrigins);

  LOG(DEBUG) << "Union result computation done" << std::endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          Result::getMergedLocalVocab(*subRes1, *subRes2)};
}

// _____________________________________________________________________________
IdTable Union::computeUnion(
    const IdTable& left, const IdTable& right,
    const std::vector<std::array<size_t, 2>>& columnOrigins) const {
  IdTable res{getResultWidth(), getExecutionContext()->getAllocator()};
  res.resize(left.size() + right.size());

  // Write the column with the `inputColumnIndex` from the `inputTable` into the
  // `targetColumn`. Always copy the complete input column and start at position
  // `offset` in the target column. If the `inputColumnIndex` is `NO_COLUMN`,
  // then the corresponding range in the `targetColumn` will be filled with
  // UNDEF.
  auto writeColumn = [this](const auto& inputTable, auto& targetColumn,
                            size_t inputColumnIndex, size_t offset) {
    if (inputColumnIndex != NO_COLUMN) {
      decltype(auto) input = inputTable.getColumn(inputColumnIndex);
      ad_utility::chunkedCopy(input, targetColumn.begin() + offset, chunkSize,
                              [this]() { checkCancellation(); });
    } else {
      ad_utility::chunkedFill(
          ql::ranges::subrange{
              targetColumn.begin() + offset,
              targetColumn.begin() + offset + inputTable.size()},
          Id::makeUndefined(), chunkSize, [this]() { checkCancellation(); });
    }
  };

  // Write all the columns.
  AD_CORRECTNESS_CHECK(columnOrigins.size() == res.numColumns());
  for (size_t targetColIdx = 0; targetColIdx < columnOrigins.size();
       ++targetColIdx) {
    auto [leftCol, rightCol] = columnOrigins.at(targetColIdx);
    decltype(auto) targetColumn = res.getColumn(targetColIdx);
    writeColumn(left, targetColumn, leftCol, 0u);
    writeColumn(right, targetColumn, rightCol, left.size());
  }
  return res;
}

// _____________________________________________________________________________
template <bool left>
std::vector<ColumnIndex> Union::computePermutation() const {
  constexpr size_t treeIndex = left ? 0 : 1;
  ColumnIndex startOfUndefColumns = _subtrees[treeIndex]->getResultWidth();
  std::vector<ColumnIndex> permutation{};
  permutation.reserve(_columnOrigins.size());
  for (const auto& columnOrigin : _columnOrigins) {
    ColumnIndex originIndex = columnOrigin[treeIndex];
    if (originIndex == NO_COLUMN) {
      originIndex = startOfUndefColumns;
      startOfUndefColumns++;
    }
    permutation.push_back(originIndex);
  }
  return permutation;
}

// _____________________________________________________________________________
IdTable Union::transformToCorrectColumnFormat(
    IdTable idTable, const std::vector<ColumnIndex>& permutation) const {
  // NOTE: previously the check was for `getResultWidth()`, but that is wrong if
  // some variables in the subtree are invisible because of a subquery.
  auto maxNumRequiredColumns = ql::ranges::max(permutation) + 1;
  while (idTable.numColumns() < maxNumRequiredColumns) {
    idTable.addEmptyColumn();
    ad_utility::chunkedFill(idTable.getColumn(idTable.numColumns() - 1),
                            Id::makeUndefined(), chunkSize,
                            [this]() { checkCancellation(); });
  }

  idTable.setColumnSubset(permutation);
  return idTable;
}

// _____________________________________________________________________________
Result::Generator Union::computeResultLazily(
    std::shared_ptr<const Result> result1,
    std::shared_ptr<const Result> result2) const {
  std::vector<ColumnIndex> permutation = computePermutation<true>();
  if (result1->isFullyMaterialized()) {
    co_yield {
        transformToCorrectColumnFormat(result1->idTable().clone(), permutation),
        result1->getCopyOfLocalVocab()};
  } else {
    for (auto& [idTable, localVocab] : result1->idTables()) {
      co_yield {transformToCorrectColumnFormat(std::move(idTable), permutation),
                std::move(localVocab)};
    }
  }
  permutation = computePermutation<false>();
  if (result2->isFullyMaterialized()) {
    co_yield {
        transformToCorrectColumnFormat(result2->idTable().clone(), permutation),
        result2->getCopyOfLocalVocab()};
  } else {
    for (auto& [idTable, localVocab] : result2->idTables()) {
      co_yield {transformToCorrectColumnFormat(std::move(idTable), permutation),
                std::move(localVocab)};
    }
  }
}

// _____________________________________________________________________________
std::unique_ptr<Operation> Union::cloneImpl() const {
  auto copy = std::make_unique<Union>(*this);
  for (auto& subtree : copy->_subtrees) {
    subtree = subtree->clone();
  }
  return copy;
}

// _____________________________________________________________________________
std::shared_ptr<Operation> Union::createSortedVariant(
    const vector<ColumnIndex>& sortOrder) const {
  return std::make_shared<Union>(_executionContext, _subtrees.at(0),
                                 _subtrees.at(1), sortOrder);
}

namespace {
struct Wrapper {
  const IdTable& idTable_;
  const LocalVocab& localVocab_;
};
Result::IdTableVocabPair moveOrCopy(Result::IdTableVocabPair& element) {
  return std::move(element);
}
Result::IdTableVocabPair moveOrCopy(const Wrapper& element) {
  return {element.idTable_.clone(), element.localVocab_.clone()};
}

template <size_t SPAN_SIZE, typename Range1, typename Range2, typename Func>
struct SortedUnionImpl
    : ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
  std::shared_ptr<const Result> result1_;
  std::shared_ptr<const Result> result2_;
  Range1 range1_;
  std::optional<typename Range1::iterator> it1_ = std::nullopt;
  size_t index1_ = 0;
  Range2 range2_;
  std::optional<typename Range2::iterator> it2_ = std::nullopt;
  size_t index2_ = 0;
  IdTable resultTable_;
  LocalVocab localVocab_{};
  ad_utility::AllocatorWithLimit<Id> allocator_;
  bool requestLaziness_;
  std::vector<std::array<size_t, 2>> columnOrigins_;
  std::vector<ColumnIndex> leftPermutation_;
  std::vector<ColumnIndex> rightPermutation_;
  std::vector<std::array<size_t, 2>> targetOrder_;
  Func applyPermutation_;
  bool aggregatedTableReturned_ = false;

  SortedUnionImpl(std::shared_ptr<const Result> result1,
                  std::shared_ptr<const Result> result2, Range1 range1,
                  Range2 range2, bool requestLaziness,
                  const std::vector<std::array<size_t, 2>>& columnOrigins,
                  const ad_utility::AllocatorWithLimit<Id>& allocator,
                  std::vector<ColumnIndex> leftPermutation,
                  std::vector<ColumnIndex> rightPermutation,
                  std::span<const ColumnIndex, SPAN_SIZE> comparatorView,
                  Func applyPermutation)
      : result1_{std::move(result1)},
        result2_{std::move(result2)},
        range1_{std::move(range1)},
        range2_{std::move(range2)},
        resultTable_{columnOrigins.size(), allocator},
        allocator_{allocator},
        requestLaziness_{requestLaziness},
        columnOrigins_{columnOrigins},
        leftPermutation_{std::move(leftPermutation)},
        rightPermutation_{std::move(rightPermutation)},
        applyPermutation_{std::move(applyPermutation)} {
    if (requestLaziness) {
      resultTable_.reserve(Union::chunkSize);
    }
    targetOrder_.reserve(comparatorView.size());
    for (auto& col : comparatorView) {
      targetOrder_.push_back(columnOrigins_.at(col));
    }
  }

  // _____________________________________________________________________________
  // Always inline makes makes a huge difference on large datasets.
  AD_ALWAYS_INLINE bool isSmaller(const auto& row1, const auto& row2) const {
    using StaticRange = std::span<const std::array<size_t, 2>, SPAN_SIZE>;
    for (auto [index1, index2] : StaticRange{targetOrder_}) {
      if (index1 == Union::NO_COLUMN) {
        return true;
      }
      if (index2 == Union::NO_COLUMN) {
        return false;
      }
      if (row1[index1] != row2[index2]) {
        return row1[index1] < row2[index2];
      }
    }
    return false;
  }

  // _____________________________________________________________________________
  std::optional<Result::IdTableVocabPair> passNext(
      const std::vector<ColumnIndex>& permutation, auto& it, auto end,
      size_t& index) {
    if (it != end) {
      Result::IdTableVocabPair pair = moveOrCopy(*it);
      pair.idTable_ = applyPermutation_(std::move(pair.idTable_), permutation);

      index = 0;
      ++it;
      return pair;
    }
    return std::nullopt;
  }

  // ___________________________________________________________________________
  void appendCurrent(const std::vector<ColumnIndex>& permutation, auto& it,
                     size_t& index) {
    resultTable_.insertAtEnd(it->idTable_, index, std::nullopt, permutation,
                             Id::makeUndefined());
    localVocab_.mergeWith(std::span{&it->localVocab_, 1});
    index = 0;
    ++it;
  }

  // ___________________________________________________________________________
  void appendRemaining(const std::vector<ColumnIndex>& permutation, auto& it,
                       auto end, size_t& index) {
    while (it != end) {
      appendCurrent(permutation, it, index);
    }
  }

  // ___________________________________________________________________________
  void pushRow(bool left, const auto& row) {
    resultTable_.emplace_back();
    for (size_t column = 0; column < resultTable_.numColumns(); column++) {
      ColumnIndex origin = columnOrigins_.at(column).at(!left);
      resultTable_.at(resultTable_.size() - 1, column) =
          origin == Union::NO_COLUMN ? Id::makeUndefined() : row[origin];
    }
  }

  // ___________________________________________________________________________
  void advanceRangeIfConsumed() {
    if (index1_ == it1_.value()->idTable_.size()) {
      ++it1_.value();
      index1_ = 0;
    }
    if (index2_ == it2_.value()->idTable_.size()) {
      ++it2_.value();
      index2_ = 0;
    }
  }

  // ___________________________________________________________________________
  Result::IdTableVocabPair popResult() {
    auto result = Result::IdTableVocabPair{std::move(resultTable_),
                                           std::move(localVocab_)};
    resultTable_ = IdTable{resultTable_.numColumns(), allocator_};
    resultTable_.reserve(Union::chunkSize);
    localVocab_ = LocalVocab{};
    advanceRangeIfConsumed();
    return result;
  }

  // ___________________________________________________________________________
  std::optional<Result::IdTableVocabPair> get() override {
    if (aggregatedTableReturned_) {
      return std::nullopt;
    }
    if (!it1_.has_value()) {
      it1_ = range1_.begin();
    }
    if (!it2_.has_value()) {
      it2_ = range2_.begin();
    }
    while (it1_ != range1_.end() && it2_ != range2_.end()) {
      auto& idTable1 = it1_.value()->idTable_;
      auto& idTable2 = it2_.value()->idTable_;
      localVocab_.mergeWith(std::span{&it1_.value()->localVocab_, 1});
      localVocab_.mergeWith(std::span{&it2_.value()->localVocab_, 1});
      while (index1_ < idTable1.size() && index2_ < idTable2.size()) {
        if (isSmaller(idTable1.at(index1_), idTable2.at(index2_))) {
          pushRow(true, idTable1.at(index1_));
          index1_++;
        } else {
          pushRow(false, idTable2.at(index2_));
          index2_++;
        }
        if (requestLaziness_ && resultTable_.size() >= Union::chunkSize) {
          return popResult();
        }
      }
      advanceRangeIfConsumed();
    }
    if (requestLaziness_) {
      if (index1_ != 0) {
        appendCurrent(leftPermutation_, it1_.value(), index1_);
      }
      if (index2_ != 0) {
        appendCurrent(rightPermutation_, it2_.value(), index2_);
      }
      if (!resultTable_.empty()) {
        return Result::IdTableVocabPair{std::move(resultTable_),
                                        std::move(localVocab_)};
      }
      auto leftTable =
          passNext(leftPermutation_, it1_.value(), range1_.end(), index1_);
      return leftTable.has_value() ? std::move(leftTable)
                                   : passNext(rightPermutation_, it2_.value(),
                                              range2_.end(), index2_);
    }
    appendRemaining(leftPermutation_, it1_.value(), range1_.end(), index1_);
    appendRemaining(rightPermutation_, it2_.value(), range2_.end(), index2_);
    aggregatedTableReturned_ = true;
    return Result::IdTableVocabPair{std::move(resultTable_),
                                    std::move(localVocab_)};
  }
};
}  // namespace

// _____________________________________________________________________________
Result::LazyResult Union::computeResultKeepOrder(
    bool requestLaziness, std::shared_ptr<const Result> result1,
    std::shared_ptr<const Result> result2) const {
  using Range = std::variant<Result::LazyResult, std::array<Wrapper, 1>>;
  auto toRange = [](const auto& result) {
    return result->isFullyMaterialized()
               ? Range{std::array{
                     Wrapper{result->idTable(), result->localVocab()}}}
               : Range{std::move(result->idTables())};
  };
  Range leftRange = toRange(result1);
  Range rightRange = toRange(result2);

  auto end = ql::ranges::find_if(targetOrder_, [this](ColumnIndex index) {
    const auto& [left, right] = _columnOrigins.at(index);
    return left == NO_COLUMN || right == NO_COLUMN;
  });
  std::span trimmedTargetOrder{targetOrder_.begin(),
                               end == targetOrder_.end() ? end : end + 1};

  auto applyPermutation = [this](IdTable idTable,
                                 const std::vector<ColumnIndex>& permutation) {
    return transformToCorrectColumnFormat(std::move(idTable), permutation);
  };

  return std::visit(
      [this, requestLaziness, &result1, &result2, &trimmedTargetOrder,
       &applyPermutation]<typename LR, typename RR>(
          LR leftRange, RR rightRange) -> Result::LazyResult {
        return ad_utility::callFixedSize(
            trimmedTargetOrder.size(),
            [this, requestLaziness, &result1, &result2, &leftRange, &rightRange,
             &trimmedTargetOrder, &applyPermutation]<int COMPARATOR_WIDTH>() {
              constexpr size_t extent = COMPARATOR_WIDTH == 0
                                            ? std::dynamic_extent
                                            : COMPARATOR_WIDTH;
              return Result::LazyResult{
                  SortedUnionImpl<extent, LR, RR, decltype(applyPermutation)>(
                      std::move(result1), std::move(result2),
                      std::move(leftRange), std::move(rightRange),
                      requestLaziness, _columnOrigins, allocator(),
                      computePermutation<true>(), computePermutation<false>(),
                      std::span<const ColumnIndex, extent>{trimmedTargetOrder},
                      std::move(applyPermutation))};
            });
      },
      std::move(leftRange), std::move(rightRange));
}
