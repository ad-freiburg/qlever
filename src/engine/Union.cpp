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
    return requestLaziness
               ? ProtoResult{std::move(generator), resultSortedOn()}
               : ProtoResult{cppcoro::getSingleElement(std::move(generator)),
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
Result::IdTableVocabPair& moveOrCopy(Result::IdTableVocabPair& element) {
  return element;
}
Result::IdTableVocabPair moveOrCopy(const Wrapper& element) {
  return {element.idTable_.clone(), element.localVocab_.clone()};
}
}  // namespace

// _____________________________________________________________________________
// Always inline makes makes a huge difference on large datasets.
template <size_t SPAN_SIZE>
AD_ALWAYS_INLINE bool Union::isSmaller(const auto& row1,
                                       const auto& row2) const {
  using StaticRange = std::span<const ColumnIndex, SPAN_SIZE>;
  for (auto& col : StaticRange{targetOrder_}) {
    ColumnIndex index1 = _columnOrigins.at(col).at(0);
    ColumnIndex index2 = _columnOrigins.at(col).at(1);
    if (index1 == NO_COLUMN) {
      return true;
    }
    if (index2 == NO_COLUMN) {
      return false;
    }
    if (row1[index1] != row2[index2]) {
      return row1[index1] < row2[index2];
    }
  }
  return false;
}

// _____________________________________________________________________________
Result::Generator Union::processRemaining(std::vector<ColumnIndex> permutation,
                                          auto& it, auto end,
                                          bool requestLaziness, size_t index,
                                          IdTable& resultTable,
                                          LocalVocab& localVocab) const {
  // append the remaining elements
  while (it != end) {
    if (requestLaziness) {
      if (index != 0) {
        resultTable.insertAtEnd(it->idTable_, index, std::nullopt, permutation,
                                Id::makeUndefined());
        localVocab.mergeWith(std::span{&it->localVocab_, 1});
        co_yield Result::IdTableVocabPair{std::move(resultTable),
                                          std::move(localVocab)};
      } else {
        if (!resultTable.empty()) {
          co_yield Result::IdTableVocabPair{std::move(resultTable),
                                            std::move(localVocab)};
        }
        auto&& pair = moveOrCopy(*it);
        pair.idTable_ = transformToCorrectColumnFormat(std::move(pair.idTable_),
                                                       permutation);
        co_yield pair;
      }
    } else {
      resultTable.insertAtEnd(it->idTable_, index, std::nullopt, permutation,
                              Id::makeUndefined());
    }
    index = 0;
    ++it;
  }
}

// _____________________________________________________________________________
template <int COMPARATOR_WIDTH>
Result::Generator Union::computeResultKeepOrderImpl(
    bool requestLaziness, auto range1, auto range2,
    std::pair<std::shared_ptr<const Result>, std::shared_ptr<const Result>>)
    const {
  constexpr size_t extent =
      COMPARATOR_WIDTH == 0 ? std::dynamic_extent : COMPARATOR_WIDTH;
  IdTable resultTable{getResultWidth(), allocator()};
  if (requestLaziness) {
    resultTable.reserve(chunkSize);
  }
  LocalVocab localVocab;
  auto it1 = range1.begin();
  auto it2 = range2.begin();
  size_t index1 = 0;
  size_t index2 = 0;
  auto pushRow = [this, &resultTable](bool left, const auto& row) {
    resultTable.emplace_back();
    for (size_t column = 0; column < resultTable.numColumns(); column++) {
      ColumnIndex origin = _columnOrigins.at(column).at(!left);
      resultTable.at(resultTable.size() - 1, column) =
          origin == NO_COLUMN ? Id::makeUndefined() : row[origin];
    }
  };
  while (it1 != range1.end() && it2 != range2.end()) {
    localVocab.mergeWith(std::span{&it1->localVocab_, 1});
    localVocab.mergeWith(std::span{&it2->localVocab_, 1});
    while (index1 < it1->idTable_.size() && index2 < it2->idTable_.size()) {
      if (isSmaller<extent>(it1->idTable_.at(index1),
                            it2->idTable_.at(index2))) {
        pushRow(true, it1->idTable_.at(index1));
        index1++;
      } else {
        pushRow(false, it2->idTable_.at(index2));
        index2++;
      }
      if (requestLaziness && resultTable.size() >= chunkSize) {
        co_yield Result::IdTableVocabPair{std::move(resultTable),
                                          std::move(localVocab)};
        resultTable = IdTable{getResultWidth(), allocator()};
        resultTable.reserve(chunkSize);
        localVocab = LocalVocab{};
      }
    }
    if (index1 == it1->idTable_.size()) {
      ++it1;
      index1 = 0;
    }
    if (index2 == it2->idTable_.size()) {
      ++it2;
      index2 = 0;
    }
  }

  // append the remaining elements
  for (auto& pair :
       processRemaining(computePermutation<true>(), it1, range1.end(),
                        requestLaziness, index1, resultTable, localVocab)) {
    AD_CORRECTNESS_CHECK(requestLaziness);
    co_yield pair;
  }
  for (auto& pair :
       processRemaining(computePermutation<false>(), it2, range2.end(),
                        requestLaziness, index2, resultTable, localVocab)) {
    AD_CORRECTNESS_CHECK(requestLaziness);
    co_yield pair;
  }
  if (!requestLaziness) {
    co_yield Result::IdTableVocabPair{std::move(resultTable),
                                      std::move(localVocab)};
  }
}

// _____________________________________________________________________________
Result::Generator Union::computeResultKeepOrder(
    bool requestLaziness, std::shared_ptr<const Result> result1,
    std::shared_ptr<const Result> result2) const {
  using Range = std::variant<Result::LazyResult, std::array<Wrapper, 1>>;
  Range leftRange = result1->isFullyMaterialized()
                        ? Range{std::array{Wrapper{result1->idTable(),
                                                   result1->localVocab()}}}
                        : Range{std::move(result1->idTables())};
  Range rightRange = result2->isFullyMaterialized()
                         ? Range{std::array{Wrapper{result2->idTable(),
                                                    result2->localVocab()}}}
                         : Range{std::move(result2->idTables())};

  // Clang falsely reports that the this capture is unused here.
  DISABLE_UNUSED_LAMBDA_CAPTURE_WARNINGS
  return std::visit(
      [this, requestLaziness, &result1, &result2](auto leftRange,
                                                  auto rightRange) {
        return ad_utility::callFixedSize(
            targetOrder_.size(),
            [this, requestLaziness, &result1, &result2, &leftRange,
             &rightRange]<int COMPARATOR_WIDTH>() {
              return computeResultKeepOrderImpl<COMPARATOR_WIDTH>(
                  requestLaziness, std::move(leftRange), std::move(rightRange),
                  std::pair{std::move(result1), std::move(result2)});
            });
      },
      std::move(leftRange), std::move(rightRange));
  ENABLE_UNUSED_LAMBDA_CAPTURE_WARNINGS
}
