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
  return std::max<size_t>(tree_->getCostEstimate(), 1);
}

// _____________________________________________________________________________
uint64_t NeutralOptional::getSizeEstimateBeforeLimit() {
  auto originalLimit = tree_->getRootOperation()->getLimit();
  tree_->setLimit({});
  absl::Cleanup resetLimit{
      [this, &originalLimit]() { tree_->setLimit(originalLimit); }};
  return std::max<size_t>(tree_->getSizeEstimate(), 1);
}

// _____________________________________________________________________________
float NeutralOptional::getMultiplicity(size_t) { return 1; }

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

// _____________________________________________________________________________
ProtoResult NeutralOptional::computeResult(bool requestLaziness) {
  const auto& limit = getLimit();
  tree_->setLimit(limit);

  auto childResult = tree_->getResult(requestLaziness);

  bool singleRowCroppedByLimit =
      limit._offset > 0 || limit.limitOrDefault() == 0;

  IdTable singleRowTable{getResultWidth(), allocator()};
  singleRowTable.push_back(std::vector(getResultWidth(), Id::makeUndefined()));
  if (childResult->isFullyMaterialized()) {
    if (childResult->idTable().empty()) {
      return {singleRowCroppedByLimit ? IdTable{getResultWidth(), allocator()}
                                      : std::move(singleRowTable),
              resultSortedOn(),
              {}};
    }
    return {childResult->idTable().clone(), childResult->sortedBy(),
            childResult->getSharedLocalVocab()};
  }
  if (singleRowCroppedByLimit) {
    return {std::move(childResult->idTables()), childResult->sortedBy()};
  }
  auto generator = [](auto idTables, auto singleRowTable) -> Result::Generator {
    size_t counter = 0;
    for (auto& tableWithVocab : idTables) {
      counter += tableWithVocab.idTable_.size();
      co_yield tableWithVocab;
    }
    if (counter == 0) {
      co_yield {std::move(singleRowTable), {}};
    }
  }(std::move(childResult->idTables()), std::move(singleRowTable));
  return {std::move(generator), childResult->sortedBy()};
}

// _____________________________________________________________________________
VariableToColumnMap NeutralOptional::computeVariableToColumnMap() const {
  auto variableColumns = tree_->getVariableColumns();
  for (ColumnIndexAndTypeInfo& info : variableColumns | ql::views::values) {
    info.mightContainUndef_ = ColumnIndexAndTypeInfo::PossiblyUndefined;
  }
  return variableColumns;
}
