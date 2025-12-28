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

  // Request lazy results only for external sorting.
  AD_LOG_DEBUG << "Getting sub-result for Sort result computation..." << endl;
  std::shared_ptr<const Result> subRes = subtree_->getResult(useExternalSort);

  if (useExternalSort) {
    return computeResultExternalSort(subRes, requestLaziness);
  }

  // In-memory sorting: requires fully materialized input.
  // TODO<joka921> proper timeout for sorting operations
  const auto& subTable = subRes->idTable();
  getExecutionContext()->getSortPerformanceEstimator().throwIfEstimateTooLong(
      subTable.numRows(), subTable.numColumns(), deadline_, "Sort operation");

  return computeResultInMemory(subRes);
}

// _____________________________________________________________________________
Result Sort::computeResultInMemory(std::shared_ptr<const Result> subRes) {
  using std::endl;
  AD_LOG_DEBUG << "Sort result computation (in-memory)..." << endl;
  runtimeInfo().addDetail("is-external", "false");
  ad_utility::Timer t{ad_utility::timer::Timer::InitialStatus::Started};
  IdTable idTable = subRes->idTable().clone();
  runtimeInfo().addDetail("time-cloning", t.msecs());
  Engine::sort(idTable, sortColumnIndices_);

  // Don't report missed timeout check because sort is not cancellable
  cancellationHandle_->resetWatchDogState();
  checkCancellation();

  AD_LOG_DEBUG << "Sort result computation done." << endl;
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
Result Sort::computeResultExternalSort(std::shared_ptr<const Result> subRes,
                                       bool requestLaziness) {
  using std::endl;
  AD_LOG_DEBUG << "Sort result computation (external)..." << endl;
  runtimeInfo().addDetail("is-external", "true");

  // Create a unique temporary filename for the external sorter in the index
  // directory (same as materialized views).
  const std::string& onDiskBase =
      getExecutionContext()->getIndex().getOnDiskBase();
  std::ostringstream oss;
  oss << onDiskBase << ".sort-" << reinterpret_cast<uintptr_t>(this) << "-"
      << std::this_thread::get_id() << ".dat";
  std::string tempFilename = oss.str();

  // Get memory limit from runtime parameters (same as materialized views).
  ad_utility::MemorySize memoryLimit =
      getRuntimeParameter<&RuntimeParameters::materializedViewWriterMemory_>();

  // Get the number of columns from the subtree (works for both lazy and
  // materialized results).
  size_t numColumns = subtree_->getResultWidth();

  // Create the external sorter with our dynamic column comparator.
  // We use a shared_ptr so the sorter survives when the lambda captures it.
  using Sorter = ad_utility::CompressedExternalIdTableSorter<SortByColumns, 0>;
  auto sorter = std::make_shared<Sorter>(
      tempFilename, numColumns, memoryLimit, allocator(),
      ad_utility::DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE,
      SortByColumns{sortColumnIndices_});

  // Push all rows into the sorter, handling both lazy and materialized results.
  // We also merge all local vocabs.
  auto mergedLocalVocab = std::make_shared<LocalVocab>();
  if (subRes->isFullyMaterialized()) {
    AD_LOG_DEBUG << "Pushing " << subRes->idTable().numRows()
                 << " rows into external sorter..." << endl;
    sorter->pushBlock(subRes->idTable());
    *mergedLocalVocab = subRes->getCopyOfLocalVocab();
  } else {
    AD_LOG_DEBUG << "Pushing rows lazily into external sorter..." << endl;
    for (Result::IdTableVocabPair& pair : subRes->idTables()) {
      checkCancellation();
      sorter->pushBlock(pair.idTable_);
      mergedLocalVocab->mergeWith(pair.localVocab_);
    }
  }

  AD_LOG_DEBUG << "Merging sorted blocks..." << endl;

  // If laziness was not requested, materialize the result.
  if (!requestLaziness) {
    IdTable result{numColumns, allocator()};
    for (auto& block : sorter->getSortedBlocks<0>()) {
      checkCancellation();
      for (const auto& row : block) {
        result.push_back(row);
      }
    }
    cancellationHandle_->resetWatchDogState();
    checkCancellation();
    AD_LOG_DEBUG << "Sort result computation done." << endl;
    return {std::move(result), resultSortedOn(), std::move(*mergedLocalVocab)};
  }

  // Return a lazy result that yields sorted blocks.
  // The first block gets the merged local vocab, subsequent blocks get empty
  // local vocabs.
  auto sortedBlocks = sorter->getSortedBlocks<0>();
  bool isFirst = true;
  return {Result::LazyResult{
              ad_utility::OwningView{ad_utility::CachingTransformInputRange{
                  std::move(sortedBlocks),
                  [sorter, mergedLocalVocab, isFirst,
                   this](IdTableStatic<0>& block) mutable {
                    checkCancellation();
                    // Convert to dynamic IdTable.
                    IdTable table = std::move(block).toDynamic();
                    // The first block carries the merged
                    // local vocab.
                    LocalVocab localVocab =
                        isFirst ? std::move(*mergedLocalVocab) : LocalVocab{};
                    isFirst = false;
                    return Result::IdTableVocabPair{std::move(table),
                                                    std::move(localVocab)};
                  }}} |
              ql::views::filter(
                  [](const auto& pair) { return !pair.idTable_.empty(); })},
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
