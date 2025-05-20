// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/GroupBy.h"

#include "engine/GroupByImpl.h"

// A thin wrapper around a `unique_ptr<GroupByImpl>`.
class GroupBy::Impl {
 public:
  std::unique_ptr<GroupByImpl> groupBy_;

  Impl(QueryExecutionContext* qec, std::vector<Variable> groupByVariables,
       std::vector<Alias> aliases, std::shared_ptr<QueryExecutionTree> subtree)
      : groupBy_(std::make_unique<GroupByImpl>(qec, std::move(groupByVariables),
                                               std::move(aliases),
                                               std::move(subtree))) {}

  Impl(std::unique_ptr<GroupByImpl> impl) : groupBy_{std::move(impl)} {}

  Impl clone() {
    return {std::unique_ptr<GroupByImpl>(
        static_cast<GroupByImpl*>(groupBy_->clone().release()))};
  }
};

// _____________________________________________________________________________
GroupBy::GroupBy(QueryExecutionContext* qec, Impl&& impl)
    : Operation{qec}, _impl{std::make_unique<Impl>(std::move(impl))} {}

// _____________________________________________________________________________
GroupBy::GroupBy(QueryExecutionContext* qec,
                 std::vector<Variable> groupByVariables,
                 std::vector<Alias> aliases,
                 std::shared_ptr<QueryExecutionTree> subtree)
    : Operation{qec},
      _impl{std::make_unique<Impl>(qec, std::move(groupByVariables),
                                   std::move(aliases), std::move(subtree))} {}

// _____________________________________________________________________________
GroupBy::~GroupBy() = default;
GroupBy::GroupBy(GroupBy&&) = default;
GroupBy& GroupBy::operator=(GroupBy&&) = default;

// _____________________________________________________________________________
std::string GroupBy::getDescriptor() const {
  return _impl->groupBy_->getDescriptor();
}

// _____________________________________________________________________________
std::string GroupBy::getCacheKeyImpl() const {
  return _impl->groupBy_->getCacheKeyImpl();
}

// _____________________________________________________________________________
size_t GroupBy::getResultWidth() const {
  return _impl->groupBy_->getResultWidth();
}

// _____________________________________________________________________________
std::vector<ColumnIndex> GroupBy::resultSortedOn() const {
  return _impl->groupBy_->resultSortedOn();
}

// _____________________________________________________________________________
bool GroupBy::knownEmptyResult() { return _impl->groupBy_->knownEmptyResult(); }

// _____________________________________________________________________________
float GroupBy::getMultiplicity(size_t col) {
  return _impl->groupBy_->getMultiplicity(col);
}

// _____________________________________________________________________________
uint64_t GroupBy::getSizeEstimateBeforeLimit() {
  return _impl->groupBy_->getSizeEstimateBeforeLimit();
}

// _____________________________________________________________________________
size_t GroupBy::getCostEstimate() { return _impl->groupBy_->getCostEstimate(); }

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> GroupBy::getChildren() {
  return _impl->groupBy_->getChildren();
}

// _____________________________________________________________________________
VariableToColumnMap GroupBy::computeVariableToColumnMap() const {
  return _impl->groupBy_->computeVariableToColumnMap();
}

// _____________________________________________________________________________
Result GroupBy::computeResult(bool requestLaziness) {
  return _impl->groupBy_->computeResult(requestLaziness);
}

// _____________________________________________________________________________
const std::vector<Variable>& GroupBy::groupByVariables() const {
  return _impl->groupBy_->groupByVariables();
}

// _____________________________________________________________________________
const std::vector<Alias>& GroupBy::aliases() const {
  return _impl->groupBy_->aliases();
}

// _____________________________________________________________________________
std::unique_ptr<Operation> GroupBy::cloneImpl() const {
  return std::make_unique<GroupBy>(_impl->groupBy_->getExecutionContext(),
                                   _impl->clone());
}

// _____________________________________________________________________________
const GroupByImpl& GroupBy::getImpl() const { return *_impl->groupBy_; }

// _____________________________________________________________________________
GroupByImpl& GroupBy::getImpl() { return *_impl->groupBy_; }
