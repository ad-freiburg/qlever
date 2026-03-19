// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/GroupBy.h"

#include "engine/GroupByImpl.h"

// _____________________________________________________________________________
GroupBy::GroupBy(QueryExecutionContext* qec,
                 std::unique_ptr<GroupByImpl>&& impl)
    : Operation{qec}, _impl{std::move(impl)} {}

// _____________________________________________________________________________
GroupBy::GroupBy(QueryExecutionContext* qec,
                 std::vector<Variable> groupByVariables,
                 std::vector<Alias> aliases,
                 std::shared_ptr<QueryExecutionTree> subtree)
    : Operation{qec},
      _impl{std::make_unique<GroupByImpl>(qec, std::move(groupByVariables),
                                          std::move(aliases),
                                          std::move(subtree))} {}

// _____________________________________________________________________________
GroupBy::~GroupBy() = default;
GroupBy::GroupBy(GroupBy&&) = default;
GroupBy& GroupBy::operator=(GroupBy&&) = default;

// _____________________________________________________________________________
std::string GroupBy::getDescriptor() const { return _impl->getDescriptor(); }

// _____________________________________________________________________________
std::string GroupBy::getCacheKeyImpl() const {
  return _impl->getCacheKeyImpl();
}

// _____________________________________________________________________________
size_t GroupBy::getResultWidth() const { return _impl->getResultWidth(); }

// _____________________________________________________________________________
std::vector<ColumnIndex> GroupBy::resultSortedOn() const {
  return _impl->resultSortedOn();
}

// _____________________________________________________________________________
bool GroupBy::knownEmptyResult() { return _impl->knownEmptyResult(); }

// _____________________________________________________________________________
float GroupBy::getMultiplicity(size_t col) {
  return _impl->getMultiplicity(col);
}

// _____________________________________________________________________________
uint64_t GroupBy::getSizeEstimateBeforeLimit() {
  return _impl->getSizeEstimateBeforeLimit();
}

// _____________________________________________________________________________
size_t GroupBy::getCostEstimate() { return _impl->getCostEstimate(); }

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> GroupBy::getChildren() {
  return _impl->getChildren();
}

// _____________________________________________________________________________
VariableToColumnMap GroupBy::computeVariableToColumnMap() const {
  return _impl->computeVariableToColumnMap();
}

// _____________________________________________________________________________
Result GroupBy::computeResult(bool requestLaziness) {
  return _impl->computeResult(requestLaziness);
}

// _____________________________________________________________________________
const std::vector<Variable>& GroupBy::groupByVariables() const {
  return _impl->groupByVariables();
}

// _____________________________________________________________________________
const std::vector<Alias>& GroupBy::aliases() const { return _impl->aliases(); }

// _____________________________________________________________________________
std::unique_ptr<Operation> GroupBy::cloneImpl() const {
  // We need to return a `unique_ptr<GroupBy>` to let the unit tests for `clone`
  // pass and to make `GroupByImpl` a true hidden implementation.
  auto ptr = std::unique_ptr<GroupByImpl>{
      static_cast<GroupByImpl*>(_impl->cloneImpl().release())};
  return std::make_unique<GroupBy>(_impl->getExecutionContext(),
                                   std::move(ptr));
}

// _____________________________________________________________________________
const GroupByImpl& GroupBy::getImpl() const { return *_impl; }

// _____________________________________________________________________________
GroupByImpl& GroupBy::getImpl() { return *_impl; }
