// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2018     Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//   2022-    Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
#include "Union.h"

#include "backports/span.h"
#include "engine/CallFixedSize.h"
#include "engine/SortedUnionImpl.h"
#include "util/ChunkedForLoop.h"

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
  // Make sure that the column origins are valid. Because later down the line we
  // might perform unchecked access using these indices.
  auto atLeastOneDefined = [](const std::array<size_t, 2>& element) {
    return element[0] != NO_COLUMN || element[1] != NO_COLUMN;
  };
  auto isValid = [](size_t column,
                    const std::shared_ptr<QueryExecutionTree>& subtree) {
    return column == NO_COLUMN || column < subtree->getResultWidth();
  };
  AD_CORRECTNESS_CHECK(ql::ranges::all_of(
      _columnOrigins, [this, &atLeastOneDefined, &isValid](const auto& el) {
        return atLeastOneDefined(el) && isValid(el[0], _subtrees[0]) &&
               isValid(el[1], _subtrees[1]);
      }));

  if (!targetOrder_.empty()) {
    auto computeSortOrder = [this](bool left) {
      std::vector<ColumnIndex> specificSortOrder;
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
      // Make sure variables are computed before swapping.
      (void)getExternallyVisibleVariableColumns();
      std::swap(_subtrees[0], _subtrees[1]);
      ql::ranges::for_each(_columnOrigins,
                           [](auto& el) { std::swap(el[0], el[1]); });
    }
  }
}

std::string Union::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "{\n";
  os << _subtrees[0]->getCacheKey() << "\n";
  os << "} UNION {\n";
  os << _subtrees[1]->getCacheKey() << "\n";
  os << "} column origins: ";
  // Since the cache keys above (of the left and right side of the UNION) do not
  // specify the selected columns, we have to add them here. This fixes #1933.
  for (auto [left, right] : _columnOrigins) {
    os << '(' << left << ", " << right << ") ";
  }
  os << " sort order: ";
  for (size_t i : targetOrder_) {
    os << i << " ";
  }
  return std::move(os).str();
}

std::string Union::getDescriptor() const { return "Union"; }

size_t Union::getResultWidth() const {
  // The width depends on the number of unique variables (as the columns of
  // two variables from the left and right subtree will end up in the same
  // result column if they have the same name).
  return _columnOrigins.size();
}

std::vector<ColumnIndex> Union::resultSortedOn() const { return targetOrder_; }

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

// _____________________________________________________________________________
size_t Union::getCostEstimate() {
  uint64_t elementsToCopy = getSizeEstimateBeforeLimit() * getResultWidth();
  // Magic value that is empirically determined (using commit 59391af) such that
  // costs of distributively applied joins roughly match the actual time.
  uint64_t timeEstimate = std::max(uint64_t{1}, elementsToCopy / 10);
  if (!targetOrder_.empty() &&
      _columnOrigins.at(targetOrder_.at(0)).at(0) != NO_COLUMN) {
    // A sorted UNION is rather expensive the factor 63 is an empirically
    // determined average factor (using commit 59391af) under the assumption
    // that the left and right side are equivalent and thus the whole range
    // completely overlaps. We assume that typically the last quarter does not
    // overlap, so we don't multiply the whole number of elements.
    constexpr uint64_t sortOverhead = 63;
    timeEstimate *= (1 + 3 * sortOverhead) / 4;
  }
  return _subtrees[0]->getCostEstimate() + _subtrees[1]->getCostEstimate() +
         timeEstimate;
}

Result Union::computeResult(bool requestLaziness) {
  AD_LOG_DEBUG << "Union result computation..." << std::endl;
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
    return requestLaziness ? Result{std::move(generator), resultSortedOn()}
                           : Result{getSingleElement(std::move(generator)),
                                    resultSortedOn()};
  }

  if (requestLaziness) {
    return {computeResultLazily(std::move(subRes1), std::move(subRes2)),
            resultSortedOn()};
  }

  AD_LOG_DEBUG << "Union subresult computation done." << std::endl;

  IdTable idTable =
      computeUnion(subRes1->idTable(), subRes2->idTable(), _columnOrigins);

  AD_LOG_DEBUG << "Union result computation done" << std::endl;
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
std::optional<ColumnIndex> Union::getOriginalColumn(
    bool leftChild, ColumnIndex unionColumn) const {
  ColumnIndex column = _columnOrigins.at(unionColumn).at(!leftChild);
  return column == NO_COLUMN ? std::nullopt : std::optional{column};
}

// _____________________________________________________________________________
IdTable Union::transformToCorrectColumnFormat(
    IdTable idTable, const std::vector<ColumnIndex>& permutation) const {
  // NOTE: previously the check was for `getResultWidth()`, but that is wrong if
  // some variables in the subtree are invisible because of a subquery.
  auto maxNumRequiredColumns =
      permutation.empty() ? ColumnIndex{0} : ql::ranges::max(permutation) + 1;
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
Result::LazyResult Union::computeResultLazily(
    std::shared_ptr<const Result> result1,
    std::shared_ptr<const Result> result2) const {
  auto transformFactory = [this](const std::vector<ColumnIndex>& permutation) {
    return [this, permutation](IdTable&& idTable, LocalVocab&& vocab) {
      return Result::IdTableVocabPair{
          this->transformToCorrectColumnFormat(std::move(idTable), permutation),
          std::move(vocab)};
    };
  };

  auto rangeFactory = [transformFactory = std::move(transformFactory)](
                          std::shared_ptr<const Result> result,
                          std::vector<ColumnIndex> permutation) {
    using namespace ad_utility;
    if (result->isFullyMaterialized()) {
      return InputRangeTypeErased(
          lazySingleValueRange([transform = transformFactory(permutation),
                                result = std::move(result)]() {
            return transform(result->idTable().clone(),
                             result->getCopyOfLocalVocab());
          }));
    }
    return InputRangeTypeErased(CachingTransformInputRange(
        result->idTables(), [transform = transformFactory(permutation)](
                                Result::IdTableVocabPair& idTableAndVocab) {
          return transform(std::move(idTableAndVocab.idTable_),
                           std::move(idTableAndVocab.localVocab_));
        }));
  };

  return Result::LazyResult{::ranges::concat_view(
      rangeFactory(std::move(result1), computePermutation<true>()),
      rangeFactory(std::move(result2), computePermutation<false>()))};
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
std::optional<std::shared_ptr<QueryExecutionTree>> Union::makeSortedTree(
    const std::vector<ColumnIndex>& sortColumns) const {
  AD_CONTRACT_CHECK(!isSortedBy(sortColumns));
  return ad_utility::makeExecutionTree<Union>(
      _executionContext, _subtrees.at(0), _subtrees.at(1), sortColumns);
}

// _____________________________________________________________________________
Result::LazyResult Union::computeResultKeepOrder(
    bool requestLaziness, std::shared_ptr<const Result> result1,
    std::shared_ptr<const Result> result2) const {
  using sortedUnion::Wrapper;
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
  ql::span trimmedTargetOrder{targetOrder_.begin(),
                              end == targetOrder_.end() ? end : end + 1};

  auto applyPermutation = [this](IdTable idTable,
                                 const std::vector<ColumnIndex>& permutation) {
    return transformToCorrectColumnFormat(std::move(idTable), permutation);
  };

  return std::visit(
      [this, requestLaziness, &result1, &result2, &trimmedTargetOrder,
       &applyPermutation](auto left, auto right) {
        return ad_utility::callFixedSizeVi(
            trimmedTargetOrder.size(),
            [this, requestLaziness, &result1, &result2, &left, &right,
             &trimmedTargetOrder, &applyPermutation](auto COMPARATOR_WIDTH) {
              constexpr size_t extent =
                  COMPARATOR_WIDTH == 0 ? ql::dynamic_extent : COMPARATOR_WIDTH;
              sortedUnion::IterationData leftData{std::move(result1),
                                                  std::move(left),
                                                  computePermutation<true>()};
              sortedUnion::IterationData rightData{std::move(result2),
                                                   std::move(right),
                                                   computePermutation<false>()};
              return Result::LazyResult{sortedUnion::SortedUnionImpl{
                  std::move(leftData), std::move(rightData), requestLaziness,
                  _columnOrigins, allocator(),
                  ql::span<const ColumnIndex, extent>{trimmedTargetOrder},
                  std::move(applyPermutation)}};
            });
      },
      std::move(leftRange), std::move(rightRange));
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
Union::makeTreeWithStrippedColumns(const std::set<Variable>& variables) const {
  // TODO<joka921> Implement this optimization for the `sortedUnion` case, we
  // have to find out the names of the variables.
  if (!targetOrder_.empty()) {
    return std::nullopt;
  }

  auto left =
      QueryExecutionTree::makeTreeWithStrippedColumns(leftChild(), variables);
  auto right =
      QueryExecutionTree::makeTreeWithStrippedColumns(rightChild(), variables);

  return ad_utility::makeExecutionTree<Union>(
      getExecutionContext(), std::move(left), std::move(right));
}
