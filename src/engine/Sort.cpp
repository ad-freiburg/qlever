// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "./Sort.h"

#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/CompressedExternalIdTable.h"
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

template <size_t NUM_COLS>
Result Sort::lazySort(std::shared_ptr<const Result> res, size_t col) {
  auto sortByCol = [col](const auto& a, const auto& b) {
    return a[col] < b[col];
  };
  // TODO<joka921> random name.
  auto sorterPtr = std::make_shared<ad_utility::CompressedExternalIdTableSorter<
      decltype(sortByCol), NUM_COLS>>(
      "lazy-sorter-todo-rename.dat", getResultWidth(),
      ad_utility::MemorySize::gigabytes(5), allocator(),
      ad_utility::DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE, sortByCol);
  auto& sorter = *sorterPtr;
  // TODO<joka921> For this to work properly we need a memory limit on the local
  // vocab...
  LocalVocab localVocab{};
  for (auto& [table, lv] : res->idTables()) {
    localVocab.mergeWith(lv);
    // TODO<joka921> This probably really inefficient, why don't we have a
    // blockwise interface here?
    for (const auto& row : table) {
      sorter.push(row);
    }
  }

  auto toIdTable = [sorterPtr,
                    lv = std::move(localVocab)](IdTableStatic<0>& table) {
    return Result::IdTableVocabPair{IdTable{std::move(table)}, lv.clone()};
  };

  // TODO<joka921> There is still one generator inside the
  // `CompressedExternalIdTable`.

  auto transformer = ad_utility::CachingTransformInputRange(
      ad_utility::OwningView{sorter.template getSortedBlocks<0>()},
      std::move(toIdTable));
  Result::LazyResult lazyRes{std::move(transformer)};

  return {std::move(lazyRes), resultSortedOn()};
}

// _____________________________________________________________________________
Result Sort::computeResult([[maybe_unused]] bool requestLaziness) {
  using std::endl;
  LOG(DEBUG) << "Getting sub-result for Sort result computation..." << endl;
  std::shared_ptr<const Result> subRes;
  if (sortColumnIndices_.size() == 1 && requestLaziness &&
      RuntimeParameters().get<"external-sort-highly-experimental">()) {
    subRes = subtree_->getResult(true);
    if (!subRes->isFullyMaterialized()) {
      auto doLazySort = [&](auto WIDTH) {
        return lazySort<WIDTH>(std::move(subRes), sortColumnIndices_[0]);
      };
      return ad_utility::callFixedSizeVi(getResultWidth(), doLazySort);
    }
  } else {
    subRes = subtree_->getResult(false);
  }
  AD_CORRECTNESS_CHECK(subRes != nullptr);

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

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
Sort::makeTreeWithStrippedColumns(
    const ad_utility::HashSet<Variable>& variables) const {
  ad_utility::HashSet<Variable> newVariables;
  std::vector<Variable> sortVars;
  const auto* vars = &variables;
  for (const auto& jcl : sortColumnIndices_) {
    const auto& var = subtree_->getVariableAndInfoByColumnIndex(jcl).first;
    sortVars.push_back(var);
    if (!variables.contains(var)) {
      if (vars == &variables) {
        newVariables = variables;
      }
      newVariables.insert(var);
      vars = &newVariables;
    }
  }

  // TODO<joka921> Code duplication including a former copy-paste bug.
  auto subtree =
      QueryExecutionTree::makeTreeWithStrippedColumns(subtree_, *vars);
  std::vector<ColumnIndex> sortColumnIndices;
  for (const auto& var : sortVars) {
    sortColumnIndices.push_back(subtree->getVariableColumn(var));
  }

  return ad_utility::makeExecutionTree<Sort>(
      getExecutionContext(), std::move(subtree), sortColumnIndices);
}
