// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "engine/Minus.h"

#include "engine/CallFixedSize.h"
#include "engine/JoinHelpers.h"
#include "engine/MinusRowHandler.h"
#include "engine/Service.h"
#include "engine/Sort.h"
#include "util/Exception.h"
#include "util/JoinAlgorithms/IndexNestedLoopJoin.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
Minus::Minus(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> left,
             std::shared_ptr<QueryExecutionTree> right)
    : Operation{qec} {
  std::tie(_left, _right, _matchedColumns) =
      QueryExecutionTree::getSortedSubtreesAndJoinColumns(std::move(left),
                                                          std::move(right));
}

// _____________________________________________________________________________
string Minus::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "MINUS\n" << _left->getCacheKey() << "\n";
  os << _right->getCacheKey() << " ";
  return std::move(os).str();
}

// _____________________________________________________________________________
string Minus::getDescriptor() const { return "Minus"; }

// _____________________________________________________________________________
Result Minus::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "Minus result computation..." << endl;

  // If the right of the RootOperations is a Service, precompute the result of
  // its sibling.
  Service::precomputeSiblingResult(_left->getRootOperation(),
                                   _right->getRootOperation(), true,
                                   requestLaziness);

  if (auto res = tryIndexNestedLoopJoinIfSuitable()) {
    return std::move(res).value();
  }

  // The lazy minus implementation does only work if there's just a single
  // join column. This might be extended in the future.
  bool lazyJoinIsSupported = _matchedColumns.size() == 1;

  auto leftResult = _left->getResult(lazyJoinIsSupported);
  auto rightResult = _right->getResult(lazyJoinIsSupported);

  if (!leftResult->isFullyMaterialized() ||
      !rightResult->isFullyMaterialized()) {
    return lazyMinusJoin(std::move(leftResult), std::move(rightResult),
                         requestLaziness);
  }

  LOG(DEBUG) << "Minus subresult computation done" << std::endl;

  LOG(DEBUG) << "Computing minus of results of size "
             << leftResult->idTable().size() << " and "
             << rightResult->idTable().size() << endl;

  IdTable idTable = computeMinus(leftResult->idTable(), rightResult->idTable(),
                                 _matchedColumns);

  LOG(DEBUG) << "Minus result computation done" << endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          Result::getMergedLocalVocab(*leftResult, *rightResult)};
}

// _____________________________________________________________________________
VariableToColumnMap Minus::computeVariableToColumnMap() const {
  return _left->getVariableColumns();
}

// _____________________________________________________________________________
size_t Minus::getResultWidth() const { return _left->getResultWidth(); }

// _____________________________________________________________________________
std::vector<ColumnIndex> Minus::resultSortedOn() const {
  return _left->resultSortedOn();
}

// _____________________________________________________________________________
float Minus::getMultiplicity(size_t col) {
  // This is an upper bound on the multiplicity as an arbitrary number
  // of rows might be deleted in this operation.
  return _left->getMultiplicity(col);
}

// _____________________________________________________________________________
uint64_t Minus::getSizeEstimateBeforeLimit() {
  // This is an upper bound on the size as an arbitrary number
  // of rows might be deleted in this operation.
  return _left->getSizeEstimate();
}

// _____________________________________________________________________________
size_t Minus::getCostEstimate() {
  size_t costEstimate = _left->getSizeEstimate() + _right->getSizeEstimate();
  return _left->getCostEstimate() + _right->getCostEstimate() + costEstimate;
}

// _____________________________________________________________________________
auto Minus::makeUndefRangesChecker(bool left, const IdTable& idTable) const {
  const auto& operation = left ? _left : _right;
  bool alwaysDefined = ql::ranges::all_of(
      _matchedColumns, [&operation, left, &idTable](const auto& cols) {
        size_t tableColumn = cols[static_cast<size_t>(!left)];
        const auto& [_, info] =
            operation->getVariableAndInfoByColumnIndex(tableColumn);
        bool colAlwaysDefined =
            info.mightContainUndef_ ==
            ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined;
        return colAlwaysDefined ||
               ql::ranges::none_of(idTable.getColumn(tableColumn),
                                   &Id::isUndefined);
      });
  // Use expensive operation if one of the columns might contain undef.
  using RT = std::variant<ad_utility::Noop, ad_utility::FindSmallerUndefRanges>;
  return alwaysDefined ? RT{ad_utility::Noop{}}
                       : RT{ad_utility::FindSmallerUndefRanges{}};
}

// _____________________________________________________________________________
template <typename T>
IdTable Minus::copyMatchingRows(
    const IdTable& left, T reference,
    const std::vector<T, ad_utility::AllocatorWithLimit<T>>& keepEntry) const {
  IdTable result{getResultWidth(), left.getAllocator()};
  AD_CORRECTNESS_CHECK(result.numColumns() == left.numColumns());

  // Transform into dense vector of indices.
  std::vector<size_t> nonMatchingIndices;
  for (size_t row = 0; row < left.numRows(); ++row) {
    if (keepEntry.at(row) == reference) {
      nonMatchingIndices.push_back(row);
    }
  }
  result.resize(nonMatchingIndices.size());

  for (const auto& [outputCol, inputCol] :
       ::ranges::views::zip(ad_utility::OwningView{result.getColumns()},
                            ad_utility::OwningView{left.getColumns()})) {
    ad_utility::chunkedCopy(
        ql::views::transform(nonMatchingIndices,
                             [&inputCol](size_t row) { return inputCol[row]; }),
        outputCol.begin(), qlever::joinHelpers::CHUNK_SIZE,
        [this]() { checkCancellation(); });
  }

  return result;
}
// _____________________________________________________________________________
IdTable Minus::computeMinus(
    const IdTable& left, const IdTable& right,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns) const {
  if (left.empty()) {
    return IdTable{getResultWidth(), getExecutionContext()->getAllocator()};
  }

  if (right.empty() || joinColumns.empty()) {
    return left.clone();
  }

  ad_utility::JoinColumnMapping joinColumnData{joinColumns, left.numColumns(),
                                               right.numColumns()};

  IdTableView<0> joinColumnsLeft =
      left.asColumnSubsetView(joinColumnData.jcsLeft());
  IdTableView<0> joinColumnsRight =
      right.asColumnSubsetView(joinColumnData.jcsRight());

  checkCancellation();

  auto leftPermuted = left.asColumnSubsetView(joinColumnData.permutationLeft());
  auto rightPermuted =
      right.asColumnSubsetView(joinColumnData.permutationRight());

  // Keep all entries by default, set to false when matching.
  std::vector keepEntry(left.size(), true, allocator().as<bool>());

  auto markForRemoval = [&keepEntry, &joinColumnsLeft](const auto& leftIt) {
    keepEntry.at(ql::ranges::distance(joinColumnsLeft.begin(), leftIt)) = false;
  };

  auto handleCompatibleRow = [&markForRemoval](const auto& leftIt,
                                               const auto& rightIt) {
    const auto& leftRow = *leftIt;
    const auto& rightRow = *rightIt;
    bool onlyMatchesBecauseOfUndef = ql::ranges::all_of(
        ::ranges::views::zip(leftRow, rightRow), [](const auto& tuple) {
          const auto& [leftId, rightId] = tuple;
          return leftId.isUndefined() || rightId.isUndefined();
        });
    if (!onlyMatchesBecauseOfUndef) {
      markForRemoval(leftIt);
    }
  };

  std::visit(
      [this, &joinColumnsLeft, &joinColumnsRight,
       handleCompatibleRow = std::move(handleCompatibleRow)](
          auto findUndefLeft, auto findUndefRight) {
        [[maybe_unused]] auto outOfOrder = ad_utility::zipperJoinWithUndef(
            joinColumnsLeft, joinColumnsRight,
            ql::ranges::lexicographical_compare, handleCompatibleRow,
            std::move(findUndefLeft), std::move(findUndefRight), {},
            [this]() { checkCancellation(); });
      },
      makeUndefRangesChecker(true, left), makeUndefRangesChecker(false, right));

  return copyMatchingRows(left, true, keepEntry);
}

// _____________________________________________________________________________
std::unique_ptr<Operation> Minus::cloneImpl() const {
  auto copy = std::make_unique<Minus>(*this);
  copy->_left = _left->clone();
  copy->_right = _right->clone();
  return copy;
}

// _____________________________________________________________________________
std::optional<Result> Minus::tryIndexNestedLoopJoinIfSuitable() {
  auto alwaysDefined = [this]() {
    return qlever::joinHelpers::joinColumnsAreAlwaysDefined(_matchedColumns,
                                                            _left, _right);
  };
  // This algorithm only works well if the left side is smaller and we can avoid
  // sorting the right side. It currently doesn't support undef.
  auto sort = std::dynamic_pointer_cast<Sort>(_right->getRootOperation());
  if (!sort || _left->getSizeEstimate() > _right->getSizeEstimate() ||
      !alwaysDefined()) {
    return std::nullopt;
  }

  auto leftRes = _left->getResult(false);
  const IdTable& leftTable = leftRes->idTable();
  auto rightRes = qlever::joinHelpers::computeResultSkipChild(sort);

  LocalVocab localVocab = leftRes->getCopyOfLocalVocab();
  joinAlgorithms::indexNestedLoop::IndexNestedLoopJoin nestedLoopJoin{
      _matchedColumns, std::move(leftRes), std::move(rightRes)};

  auto nonMatchingEntries = nestedLoopJoin.computeExistance();
  return std::optional{Result{
      copyMatchingRows(leftTable, static_cast<char>(false), nonMatchingEntries),
      resultSortedOn(), std::move(localVocab)}};
}

// _____________________________________________________________________________
bool Minus::columnOriginatesFromGraphOrUndef(const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  return _left->getRootOperation()->columnOriginatesFromGraphOrUndef(variable);
}

// _____________________________________________________________________________
Result Minus::lazyMinusJoin(std::shared_ptr<const Result> left,
                            std::shared_ptr<const Result> right,
                            bool requestLaziness) {
  // If both inputs are fully materialized, we can join them more
  // efficiently.
  AD_CONTRACT_CHECK(!left->isFullyMaterialized() ||
                    !right->isFullyMaterialized());
  // Currently only supports a single join column.
  AD_CORRECTNESS_CHECK(_matchedColumns.size() == 1);

  std::vector<ColumnIndex> permutation;
  permutation.resize(_left->getResultWidth());
  ql::ranges::copy(ad_utility::integerRange(permutation.size()),
                   permutation.begin());
  // Create a permutation that swaps the join column with the first column. (And
  // swaps it back afterwards, which is the same permutation.)
  ColumnIndex leftJoinColumn = _matchedColumns.at(0).at(0);
  std::swap(permutation.at(0), permutation.at(leftJoinColumn));

  auto action =
      [this, left = std::move(left), right = std::move(right),
       permutation](std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        ad_utility::MinusRowHandler rowAdder{
            _matchedColumns.size(), IdTable{getResultWidth(), allocator()},
            cancellationHandle_, std::move(yieldTable)};
        auto leftRange = qlever::joinHelpers::resultToView(*left, permutation);
        auto rightRange = qlever::joinHelpers::resultToView(
            *right, {_matchedColumns.at(0).at(1)});
        std::visit(
            [&rowAdder](auto& leftBlocks, auto& rightBlocks) {
              ad_utility::zipperJoinForBlocksWithPotentialUndef(
                  leftBlocks, rightBlocks, std::less{}, rowAdder, {}, {},
                  std::true_type{}, std::true_type{});
            },
            leftRange, rightRange);
        auto localVocab = std::move(rowAdder.localVocab());
        return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                        std::move(localVocab)};
      };

  if (requestLaziness) {
    return {qlever::joinHelpers::runLazyJoinAndConvertToGenerator(
                std::move(action), std::move(permutation)),
            resultSortedOn()};
  } else {
    auto [idTable, localVocab] = action(ad_utility::noop);
    qlever::joinHelpers::applyPermutation(idTable, permutation);
    return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
  }
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
Minus::makeTreeWithStrippedColumns(const std::set<Variable>& variables) const {
  std::set<Variable> newVariables;
  const auto* vars = &variables;
  for (const auto& [jcl, _] : _matchedColumns) {
    const auto& var = _left->getVariableAndInfoByColumnIndex(jcl).first;
    if (!variables.contains(var)) {
      if (vars == &variables) {
        newVariables = variables;
      }
      newVariables.insert(var);
      vars = &newVariables;
    }
  }

  auto left = QueryExecutionTree::makeTreeWithStrippedColumns(_left, *vars);
  auto right = QueryExecutionTree::makeTreeWithStrippedColumns(_right, *vars);

  // TODO<joka921> The following could be done more efficiently in a constructor
  // (like this it is done twice).
  // TODO<joka921> apply the `keepJoinColumn` optimization.
  auto jcls = QueryExecutionTree::getJoinColumns(*_left, *_right);
  [[maybe_unused]] bool keepJoinColumns =
      ql::ranges::any_of(jcls, [&](const auto& jcl) {
        const auto& var = _left->getVariableAndInfoByColumnIndex(jcl[0]).first;
        return variables.contains(var);
      });
  return ad_utility::makeExecutionTree<Minus>(
      getExecutionContext(), std::move(left), std::move(right));
}
