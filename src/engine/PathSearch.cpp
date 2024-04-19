// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "PathSearch.h"

#include <memory>
#include <sstream>

#include "util/Exception.h"

// _____________________________________________________________________________
PathSearch::PathSearch(QueryExecutionContext* qec,
                       std::shared_ptr<QueryExecutionTree> subtree)
    : Operation(qec), subtree_(subtree), graph_() {
  AD_CORRECTNESS_CHECK(qec != nullptr);
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> PathSearch::getChildren() {
  std::vector<QueryExecutionTree*> res;
  res.push_back(subtree_.get());
  return res;
};

// _____________________________________________________________________________
std::string PathSearch::getCacheKeyImpl() const {
  std::ostringstream os;
  AD_CORRECTNESS_CHECK(subtree_);
  os << "Subtree:\n" << subtree_->getCacheKey() << '\n';
  return std::move(os).str();
};

// _____________________________________________________________________________
string PathSearch::getDescriptor() const {
  std::ostringstream os;
  os << "PathSearch";
  return std::move(os).str();
};

// _____________________________________________________________________________
size_t PathSearch::getResultWidth() const { return resultWidth_; };

// _____________________________________________________________________________
void PathSearch::setTextLimit(size_t limit) {
  for (auto child : getChildren()) {
    child->setTextLimit(limit);
  }
};

// _____________________________________________________________________________
size_t PathSearch::getCostEstimate() {
  // TODO: Figure out a smart way to estimate cost
  return 1000;
};

// _____________________________________________________________________________
uint64_t PathSearch::getSizeEstimateBeforeLimit() {
  // TODO: Figure out a smart way to estimate size
  return 1000;
};

// _____________________________________________________________________________
float PathSearch::getMultiplicity(size_t col) {
  (void)col;
  return 1;
};

// _____________________________________________________________________________
bool PathSearch::knownEmptyResult() { return subtree_->knownEmptyResult(); };

// _____________________________________________________________________________
vector<ColumnIndex> PathSearch::resultSortedOn() const { return {}; };

// _____________________________________________________________________________
ResultTable PathSearch::computeResult() {
  shared_ptr<const ResultTable> subRes = subtree_->getResult();
  IdTable idTable{allocator()};
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
};

// _____________________________________________________________________________
VariableToColumnMap PathSearch::computeVariableToColumnMap() const {
  return variableColumns_;
};
