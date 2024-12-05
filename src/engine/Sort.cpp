// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "./Sort.h"

#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Filter.h"
#include "engine/IndexScan.h"
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
ProtoResult Sort::computeResult([[maybe_unused]] bool requestLaziness) {
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
size_t Sort::getCostEstimate() {
  size_t size = getSizeEstimateBeforeLimit();
  size_t logSize =
      size < 4 ? 2 : static_cast<size_t>(logb(static_cast<double>(size)));
  size_t nlogn = size * logSize;
  size_t subcost = subtree_->getCostEstimate();
  // Return  at least 1, s.t. the query planner will never emit an unnecessary
  // sort of an empty `IndexScan`. This makes the testing of the query
  // planner much easier.

  // Don't return plain `n log n` but also incorporate the number of columns and
  // a constant multiplicator for the inherent complexity of sorting.
  auto result = std::max(1UL, 20 * getResultWidth() * (nlogn + subcost));

  // Determine if the subtree is a FILTER of an INDEX SCAN. This case can be
  // useful if the FILTER can be applied via binary search and the result is
  // then so small that the SORT doesn't hurt anymore. But in case the FILTER
  // doesn't filter out much, and the result size is beyond a configurable
  // threshold, we want to heavily discourage the plan with the binary filter +
  // sorting, because it breaks the lazy evaluation.
  auto sizeEstimateOfFilteredScan = [&]() -> size_t {
    if (auto filter =
            dynamic_cast<const Filter*>(subtree_->getRootOperation().get())) {
      if (dynamic_cast<const IndexScan*>(
              filter->getSubtree()->getRootOperation().get())) {
        return subtree_->getSizeEstimate();
      }
    }
    return 0;
  }();
  size_t maxSizeFilteredScan =
      RuntimeParameters()
          .get<"max-materialization-size-filtered-scan">()
          .getBytes() /
      sizeof(Id) / subtree_->getResultWidth();
  if (sizeEstimateOfFilteredScan > maxSizeFilteredScan) {
    // If the filtered result is larger than the defined threshold, make the
    // cost estimate much larger, s.t. the query planner will prefer a plan
    // without the `SORT`.
    result *= 10'000;
  }
  return result;
}
