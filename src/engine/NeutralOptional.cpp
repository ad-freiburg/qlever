//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/NeutralOptional.h"

// _____________________________________________________________________________
NeutralOptional::NeutralOptional(QueryExecutionContext* qec,
                                 std::shared_ptr<QueryExecutionTree> tree)
    : Operation{qec}, tree_{std::move(tree)} {}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> NeutralOptional::getChildren() {
  return {tree_.get()};
}

// _____________________________________________________________________________
std::string NeutralOptional::getCacheKeyImpl() const {
  return absl::StrCat("NeutralOptional#", tree_->getCacheKey());
}

// _____________________________________________________________________________
std::string NeutralOptional::getDescriptor() const { return "Optional"; }

// _____________________________________________________________________________
size_t NeutralOptional::getResultWidth() const {
  return tree_->getResultWidth();
}

// _____________________________________________________________________________
size_t NeutralOptional::getCostEstimate() {
  // Cloning is expensive, so we estimate the cost as the cost of the child.
  // Does not apply to the lazy case.
  return std::max<size_t>(tree_->getCostEstimate() * 2, 1);
}

// _____________________________________________________________________________
uint64_t NeutralOptional::getSizeEstimateBeforeLimit() {
  return std::max<size_t>(tree_->getSizeEstimate(), 1);
}

// _____________________________________________________________________________
float NeutralOptional::getMultiplicity(size_t col) {
  return tree_->getMultiplicity(col);
}

// _____________________________________________________________________________
bool NeutralOptional::knownEmptyResult() { return false; }

// _____________________________________________________________________________
bool NeutralOptional::supportsLimit() const { return true; }

// _____________________________________________________________________________
std::unique_ptr<Operation> NeutralOptional::cloneImpl() const {
  return std::make_unique<NeutralOptional>(getExecutionContext(),
                                           tree_->clone());
}

// _____________________________________________________________________________
std::vector<ColumnIndex> NeutralOptional::resultSortedOn() const {
  return tree_->resultSortedOn();
}

namespace {
// State machine that forwards a lazy result and ensures that at least one row
// is returned when no limit is present.
struct WrapperWithEnsuredRow
    : ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
  Result::LazyResult originalRange_;
  IdTable rowFallback_;
  std::optional<Result::LazyResult::iterator> iterator_ = std::nullopt;
  bool hasRows_ = false;
  bool done_ = false;

  explicit WrapperWithEnsuredRow(Result::LazyResult originalRange,
                                 IdTable rowFallback)
      : originalRange_{std::move(originalRange)},
        rowFallback_{std::move(rowFallback)} {}

  std::optional<Result::IdTableVocabPair> get() override {
    if (done_) {
      return std::nullopt;
    }
    if (iterator_) {
      ++iterator_.value();
    } else {
      iterator_ = originalRange_.begin();
    }
    if (*iterator_ == originalRange_.end()) {
      done_ = true;
      return hasRows_ ? std::nullopt
                      : std::optional{Result::IdTableVocabPair{
                            std::move(rowFallback_), {}}};
    }
    hasRows_ |= !iterator_.value()->idTable_.empty();
    return std::move(*iterator_.value());
  }
};
}  // namespace

// _____________________________________________________________________________
bool NeutralOptional::singleRowCroppedByLimit() const {
  const auto& limit = getLimit();
  return limit._offset > 0 || limit.limitOrDefault() == 0;
}

// _____________________________________________________________________________
Result NeutralOptional::computeResult(bool requestLaziness) {
  const auto& limit = getLimit();
  tree_->setLimit(limit);

  auto childResult = tree_->getResult(requestLaziness);

  IdTable singleRowTable{getResultWidth(), allocator()};
  singleRowTable.push_back(std::vector(getResultWidth(), Id::makeUndefined()));
  if (childResult->isFullyMaterialized()) {
    if (childResult->idTable().empty()) {
      return {singleRowCroppedByLimit() ? IdTable{getResultWidth(), allocator()}
                                        : std::move(singleRowTable),
              resultSortedOn(),
              {}};
    }
    return {childResult->idTable().clone(), childResult->sortedBy(),
            childResult->getSharedLocalVocab()};
  }
  if (singleRowCroppedByLimit()) {
    return {std::move(childResult->idTables()), childResult->sortedBy()};
  }
  return {Result::LazyResult{WrapperWithEnsuredRow{
              std::move(childResult->idTables()), std::move(singleRowTable)}},
          childResult->sortedBy()};
}

// _____________________________________________________________________________
VariableToColumnMap NeutralOptional::computeVariableToColumnMap() const {
  auto variableColumns = tree_->getVariableColumns();
  // Because the optional operation might not return any rows, and we add
  // placeholder undefined rows in that case, we need to mark all columns as
  // possibly undefined.
  for (ColumnIndexAndTypeInfo& info : variableColumns | ql::views::values) {
    info.mightContainUndef_ = ColumnIndexAndTypeInfo::PossiblyUndefined;
  }
  return variableColumns;
}
