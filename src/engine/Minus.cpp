// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "Minus.h"

#include "engine/CallFixedSize.h"
#include "engine/Service.h"
#include "util/Exception.h"
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
Result Minus::computeResult([[maybe_unused]] bool requestLaziness) {
  LOG(DEBUG) << "Minus result computation..." << endl;

  // If the right of the RootOperations is a Service, precompute the result of
  // its sibling.
  Service::precomputeSiblingResult(_left->getRootOperation(),
                                   _right->getRootOperation(), true,
                                   requestLaziness);

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

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
vector<ColumnIndex> Minus::resultSortedOn() const {
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
  std::vector keepEntry(left.size(), true);

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

  IdTable result{getResultWidth(), getExecutionContext()->getAllocator()};
  AD_CORRECTNESS_CHECK(result.numColumns() == left.numColumns());

  // Transform into dense vector of indices.
  std::vector<size_t> nonMatchingIndices;
  for (size_t row = 0; row < left.numRows(); ++row) {
    if (keepEntry.at(row)) {
      nonMatchingIndices.push_back(row);
    }
  }
  result.resize(nonMatchingIndices.size());

  for (ColumnIndex col = 0; col < result.numColumns(); col++) {
    ql::ranges::transform(
        nonMatchingIndices, result.getColumn(col).begin(),
        [inputCol = left.getColumn(col)](size_t row) { return inputCol[row]; });
  }

  return result;
}

// _____________________________________________________________________________
std::unique_ptr<Operation> Minus::cloneImpl() const {
  auto copy = std::make_unique<Minus>(*this);
  copy->_left = _left->clone();
  copy->_right = _right->clone();
  return copy;
}

// _____________________________________________________________________________
bool Minus::columnOriginatesFromGraphOrUndef(const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  return _left->getRootOperation()->columnOriginatesFromGraphOrUndef(variable);
}
