// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "engine/Sort.h"

#include <sstream>
#include <thread>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/RuntimeParameters.h"
#include "index/ExternalSortFunctors.h"
#include "util/Algorithm.h"

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
Result Sort::computeResult(bool requestLaziness) {
  using std::endl;

  // Check if we should use external sorting.
  bool useExternalSort =
      getRuntimeParameter<&RuntimeParameters::sortExternal_>();

  // Get the input lazily if we want to do external sorting, and materialized
  // otherwise.
  std::shared_ptr<const Result> subRes = subtree_->getResult(useExternalSort);

  // Do the sorting, either external or in-memory.
  if (useExternalSort) {
    return computeResultExternalSort(subRes, requestLaziness);
  } else {
    const auto& subTable = subRes->idTable();
    getExecutionContext()->getSortPerformanceEstimator().throwIfEstimateTooLong(
        subTable.numRows(), subTable.numColumns(), deadline_, "Sort operation");
    return computeResultInMemory(subRes);
  }
}

// _____________________________________________________________________________
Result Sort::computeResultInMemory(std::shared_ptr<const Result> subRes) {
  runtimeInfo().addDetail("is-external", "false");

  // First clone the input table, then sort it in place.
  ad_utility::Timer t{ad_utility::timer::Timer::InitialStatus::Started};
  IdTable idTable = subRes->idTable().clone();
  runtimeInfo().addDetail("time-cloning", t.msecs());
  Engine::sort(idTable, sortColumnIndices_);

  // Don't report missed timeout check because the in-memory sort is currently
  // not cancellable.
  //
  // TODO: make the in-memory sort cancellable.
  cancellationHandle_->resetWatchDogState();
  checkCancellation();

  // Return the sorted result.
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
Result Sort::computeResultExternalSort(std::shared_ptr<const Result> subRes,
                                       bool requestLaziness) {
  runtimeInfo().addDetail("is-external", "true");

  // Create a unique temporary filename for the external sorter in the index
  // directory.
  const std::string& onDiskBase =
      getExecutionContext()->getIndex().getOnDiskBase();
  std::ostringstream oss;
  oss << onDiskBase << ".sort-" << reinterpret_cast<uintptr_t>(this) << "-"
      << std::this_thread::get_id() << ".dat";
  std::string tempFilename = oss.str();

  // Get memory limit from the same runtime parameters that we use for the
  // materialized view writer.
  //
  // TODO: Give this runime parameter a more general name.
  ad_utility::MemorySize memoryLimit =
      getRuntimeParameter<&RuntimeParameters::materializedViewWriterMemory_>();

  // Get the number of columns from the subtree (this works for both lazy and
  // materialized results).
  size_t numColumns = subtree_->getResultWidth();

  // Create the external sorter with our dynamic column comparator. We use a
  // shared pointer so the sorter survives when the lambda below captures it.
  using Sorter = ad_utility::CompressedExternalIdTableSorter<SortByColumns, 0>;
  auto sorter = std::make_shared<Sorter>(
      tempFilename, numColumns, memoryLimit, allocator(),
      ad_utility::DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE,
      SortByColumns{sortColumnIndices_});

  // Push all rows into the sorter, handling both lazy and materialized results.
  // For materialized results, we copy the local vocab, for lazy results we
  // merge the local vocabs from the individual blocks.
  auto mergedLocalVocab = std::make_shared<LocalVocab>();
  if (subRes->isFullyMaterialized()) {
    sorter->pushBlock(subRes->idTable());
    *mergedLocalVocab = subRes->getCopyOfLocalVocab();
  } else {
    for (Result::IdTableVocabPair& pair : subRes->idTables()) {
      checkCancellation();
      sorter->pushBlock(pair.idTable_);
      mergedLocalVocab->mergeWith(pair.localVocab_);
    }
  }

  // If laziness is not requested, materialize the result. The `sorter` the
  // size of the result, so we can reserve exactly the right amount of space
  // in advance.
  if (!requestLaziness) {
    IdTable result{numColumns, allocator()};
    result.reserve(sorter->size());
    for (auto& block : sorter->getSortedBlocks<0>()) {
      checkCancellation();
      result.insertAtEnd(block);
    }
    cancellationHandle_->resetWatchDogState();
    checkCancellation();
    return {std::move(result), resultSortedOn(), std::move(*mergedLocalVocab)};
  }

  // Otherwise, return a lazy result that yields sorted blocks. Each block gets
  // a clone of the merged local vocab because consumers may read only a subset
  // of blocks (e.g., for join prefiltering).
  auto sortedBlocks = sorter->getSortedBlocks<0>();
  return {Result::LazyResult{
              ad_utility::OwningView{ad_utility::CachingTransformInputRange{
                  std::move(sortedBlocks),
                  [sorter, mergedLocalVocab,
                   this](IdTableStatic<0>& block) mutable {
                    checkCancellation();
                    IdTable table = std::move(block).toDynamic();
                    return Result::IdTableVocabPair{std::move(table),
                                                    mergedLocalVocab->clone()};
                  }}}},
          resultSortedOn()};
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
Sort::makeTreeWithStrippedColumns(const std::set<Variable>& variables) const {
  std::set<Variable> newVariables;
  std::vector<Variable> sortVars;
  const auto* vars = &variables;
  for (const auto& jcl : sortColumnIndices_) {
    const auto& var = subtree_->getVariableAndInfoByColumnIndex(jcl).first;
    sortVars.push_back(var);
    if (!ad_utility::contains(variables, var)) {
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
