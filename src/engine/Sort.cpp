// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "./Sort.h"

#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/QueryExecutionTree.h"
#include "global/RuntimeParameters.h"

// _____________________________________________________________________________
size_t Sort::getResultWidth() const { return subtree_->getResultWidth(); }

// _____________________________________________________________________________
Sort::Sort(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           std::vector<ColumnIndex> sortColumnIndices)
    : Operation{qec},
      subtree_{std::move(subtree)},
      sortColumnIndices_{std::move(sortColumnIndices)} {}

// _____________________________________________________________________________
std::string Sort::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "SORT(internal) on columns:";

  for (const auto& sortCol : sortColumnIndices_) {
    os << "asc(" << sortCol << ") ";
  }
  os << "\n" << subtree_->getCacheKey();
  return std::move(os).str();
}

// _____________________________________________________________________________
std::string Sort::getDescriptor() const {
  std::string orderByVars;
  const auto& varCols = subtree_->getVariableColumns();
  for (auto sortColumn : sortColumnIndices_) {
    for (const auto& [var, varIndex] : varCols) {
      if (sortColumn == varIndex.columnIndex_) {
        orderByVars += " " + var.name();
      }
    }
  }

  return "Sort (internal order) on" + orderByVars;
}

// _____________________________________________________________________________
Result Sort::computeResult([[maybe_unused]] bool requestLaziness) {
  using std::endl;
  LOG(DEBUG) << "Getting sub-result for Sort result computation..." << endl;
  std::shared_ptr<const Result> subRes = subtree_->getResult();

  // TODO<joka921> proper timeout for sorting operations
  const auto& subTable = subRes->idTable();
  getExecutionContext()->getSortPerformanceEstimator().throwIfEstimateTooLong(
      subTable.numRows(), subTable.numColumns(), deadline_, "Sort operation");

  LOG(DEBUG) << "Sort result computation..." << endl;
  ad_utility::Timer t{ad_utility::timer::Timer::InitialStatus::Started};
  IdTable idTable = subRes->idTable().clone();
  runtimeInfo().addDetail("time-cloning", t.msecs());
  Engine::sort(idTable, sortColumnIndices_);

  // Don't report missed timeout check because sort is not cancellable
  cancellationHandle_->resetWatchDogState();
  checkCancellation();

  LOG(DEBUG) << "Sort result computation done." << endl;
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>> Sort::makeSortedTree(
    const std::vector<ColumnIndex>& sortColumns) const {
  AD_CONTRACT_CHECK(!isSortedBy(sortColumns));
  AD_LOG_DEBUG
      << "Tried to re-sort a subtree that is already sorted by `Sort` with a "
         "different sort order. This indicates a flaw during query planning."
      << std::endl;
  return ad_utility::makeExecutionTree<Sort>(_executionContext, subtree_,
                                             sortColumns);
}

// _____________________________________________________________________________
std::unique_ptr<Operation> Sort::cloneImpl() const {
  return std::make_unique<Sort>(_executionContext, subtree_->clone(),
                                sortColumnIndices_);
}
