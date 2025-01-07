//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/ExistsScan.h"

#include "util/JoinAlgorithms/JoinAlgorithms.h"

// _____________________________________________________________________________
ExistsScan::ExistsScan(QueryExecutionContext* qec,
                       std::shared_ptr<QueryExecutionTree> left,
                       std::shared_ptr<QueryExecutionTree> right,
                       Variable existsVariable)
    : Operation{qec},
      left_{std::move(left)},
      right_{std::move(right)},
      joinColumns_{QueryExecutionTree::getJoinColumns(*left_, *right_)},
      existsVariable_{std::move(existsVariable)} {}

// _____________________________________________________________________________
string ExistsScan::getCacheKeyImpl() const {
  return absl::StrCat("EXISTS SCAN left: ", left_->getCacheKey(),
                      " right: ", right_->getCacheKey());
}

// _____________________________________________________________________________
string ExistsScan::getDescriptor() const { return "EXISTS scan"; }

// ____________________________________________________________________________
VariableToColumnMap ExistsScan::computeVariableToColumnMap() const {
  auto res = left_->getVariableColumns();
  AD_CONTRACT_CHECK(
      !res.contains(existsVariable_),
      "The target variable of an exists scan must be a new variable");
  res[existsVariable_] = makeAlwaysDefinedColumn(getResultWidth() - 1);
  return res;
}

// ____________________________________________________________________________
size_t ExistsScan::getResultWidth() const {
  // We add one column to the input.
  return left_->getResultWidth() + 1;
}

// ____________________________________________________________________________
vector<ColumnIndex> ExistsScan::resultSortedOn() const {
  return left_->resultSortedOn();
}

// ____________________________________________________________________________
float ExistsScan::getMultiplicity(size_t col) {
  if (col < getResultWidth() - 1) {
    return left_->getMultiplicity(col);
  }
  // The multiplicity of the boolean column can be a dummy value, as it should
  // be never used for joins etc.
  return 1;
}

// ____________________________________________________________________________
uint64_t ExistsScan::getSizeEstimateBeforeLimit() {
  return left_->getSizeEstimate();
}

// ____________________________________________________________________________
size_t ExistsScan::getCostEstimate() {
  return left_->getCostEstimate() + right_->getCostEstimate() +
         left_->getSizeEstimate() + right_->getSizeEstimate();
}

// ____________________________________________________________________________
ProtoResult ExistsScan::computeResult([[maybe_unused]] bool requestLaziness) {
  auto leftRes = left_->getResult();
  auto rightRes = right_->getResult();
  const auto& left = leftRes->idTable();
  const auto& right = rightRes->idTable();

  ad_utility::JoinColumnMapping joinColumnData{joinColumns_, left.numColumns(),
                                               right.numColumns()};

  IdTableView<0> joinColumnsLeft =
      left.asColumnSubsetView(joinColumnData.jcsLeft());
  IdTableView<0> joinColumnsRight =
      right.asColumnSubsetView(joinColumnData.jcsRight());

  checkCancellation();

  auto noopRowAdder = [](auto&&...) {};

  // TODO<joka921> Memory limit.
  std::vector<size_t> notExistsIndices;
  auto actionForNotExisting =
      [&notExistsIndices, begin = joinColumnsLeft.begin()](const auto& itLeft) {
        notExistsIndices.push_back(itLeft - begin);
      };

  // TODO<joka921> Handle UNDEF values correctly (and efficiently)
  auto findUndefDispatch = []<typename It>([[maybe_unused]] const auto& row,
                                           [[maybe_unused]] It begin,
                                           [[maybe_unused]] auto end,
                                           [[maybe_unused]] bool& outOfOrder) {
    return std::array<It, 0>{};
  };

  auto checkCancellationLambda = [this] { checkCancellation(); };
  [[maybe_unused]] auto numOutOfOrder = ad_utility::zipperJoinWithUndef(
      joinColumnsLeft, joinColumnsRight, ql::ranges::lexicographical_compare,
      noopRowAdder, findUndefDispatch, findUndefDispatch, actionForNotExisting,
      checkCancellationLambda);

  // Set up the result;
  IdTable result = left.clone();
  result.addEmptyColumn();
  decltype(auto) existsCol = result.getColumn(getResultWidth() - 1);
  ql::ranges::fill(existsCol, Id::makeFromBool(true));
  for (size_t notExistsIndex : notExistsIndices) {
    existsCol[notExistsIndex] = Id::makeFromBool(false);
  }
  return {std::move(result), resultSortedOn(), leftRes->getCopyOfLocalVocab()};
}
