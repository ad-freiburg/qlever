// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
//   2026      Mark Veser (mark.veser87@gmail.com)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/Join.h"
#include "engine/JoinImpl.h"

using std::string;

// _____________________________________________________________________________
Join::Join(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
           std::shared_ptr<QueryExecutionTree> t2, ColumnIndex t1JoinCol,
           ColumnIndex t2JoinCol, bool keepJoinColumn,
           bool allowSwappingChildrenOnlyForTesting)
    : Operation(qec),
      impl_(std::make_unique<JoinImpl>(qec, std::move(t1), std::move(t2),
                                       t1JoinCol, t2JoinCol, keepJoinColumn,
                                       allowSwappingChildrenOnlyForTesting)) {}

// _____________________________________________________________________________
Join::Join(QueryExecutionContext* qec, std::unique_ptr<JoinImpl>&& impl)
    : Operation(qec), impl_(std::move(impl)) {}

// _____________________________________________________________________________
Join::~Join() = default;
Join::Join(Join&&) = default;
Join& Join::operator=(Join&&) = default;

// _____________________________________________________________________________
string Join::getDescriptor() const { return impl_->getDescriptor(); }

// _____________________________________________________________________________
size_t Join::getResultWidth() const { return impl_->getResultWidth(); }

// _____________________________________________________________________________
std::vector<ColumnIndex> Join::resultSortedOn() const {
  return impl_->resultSortedOn();
}

// _____________________________________________________________________________
size_t Join::getCostEstimate() { return impl_->getCostEstimate(); }

// _____________________________________________________________________________
bool Join::knownEmptyResult() { return impl_->knownEmptyResult(); }

// _____________________________________________________________________________
float Join::getMultiplicity(size_t col) { return impl_->getMultiplicity(col); }

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> Join::getChildren() {
  return impl_->getChildren();
}

// _____________________________________________________________________________
bool Join::columnOriginatesFromGraphOrUndef(const Variable& variable) const {
  return impl_->columnOriginatesFromGraphOrUndef(variable);
}

// _____________________________________________________________________________
string Join::getCacheKeyImpl() const { return impl_->getCacheKeyImpl(); }

// _____________________________________________________________________________
std::unique_ptr<Operation> Join::cloneImpl() const {
  auto ptr = std::unique_ptr<JoinImpl>{
      static_cast<JoinImpl*>(impl_->cloneImpl().release())};
  return std::make_unique<Join>(impl_->getExecutionContext(), std::move(ptr));
}

// _____________________________________________________________________________
Result Join::computeResult(bool requestLaziness) {
  return impl_->computeResult(requestLaziness);
}

// _____________________________________________________________________________
VariableToColumnMap Join::computeVariableToColumnMap() const {
  return impl_->computeVariableToColumnMap();
}

// _____________________________________________________________________________
uint64_t Join::getSizeEstimateBeforeLimit() {
  return impl_->getSizeEstimateBeforeLimit();
}

// ____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
Join::makeTreeWithStrippedColumns(const std::set<Variable>& variables) const {
  return impl_->makeTreeWithStrippedColumns(variables);
}

// ____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>> Join::makeTreeWithBindColumn(
    const parsedQuery::Bind& bind) const {
  return impl_->makeTreeWithBindColumn(bind);
}
