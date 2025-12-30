// Copyright 2015 - 2025 The QLever Authors, in particular:
//
// 2015 - 2017 Björn Buchhold <buchhold@cs.uni-freiburg.de>, UFR
// 2023 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/Sort.h"

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/QueryExecutionTree.h"
#include "global/RuntimeParameters.h"
#include "util/Algorithm.h"
#include "util/Random.h"

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
  size_t numColumns = subtree_->getResultWidth();
  // Maximum number of rows that can be sorted in memory.
  size_t maxNumRowsToBeSortedInMemory =
      getRuntimeParameter<&RuntimeParameters::sortInMemoryThreshold_>()
          .getBytes() /
      (numColumns * sizeof(Id));

  // Always request lazy input to avoid premature materialization.
  std::shared_ptr<const Result> subRes = subtree_->getResult(true);

  // For fully materialized input, we know the size upfront.
  if (subRes->isFullyMaterialized()) {
    const IdTable& table = subRes->idTable();
    if (table.numRows() <= maxNumRowsToBeSortedInMemory) {
      return computeResultInMemory(table.clone(),
                                   subRes->getSharedLocalVocab());
    } else {
      std::vector<IdTable> blocks;
      blocks.push_back(table.clone());
      std::vector<Result::IdTableVocabPair> noRemainingBlocks;
      return computeResultExternal(
          std::move(blocks), subRes->getCopyOfLocalVocab(),
          noRemainingBlocks.begin(), noRemainingBlocks.end(), requestLaziness);
    }
  }

  // For lazy input, collect blocks until we exceed the threshold. Note that we
  // may exceed the threshold by the size of one block.
  std::vector<IdTable> collectedBlocks;
  LocalVocab mergedLocalVocab;
  size_t totalRows = 0;
  auto idTables = subRes->idTables();
  auto it = idTables.begin();
  while (it != idTables.end() && totalRows <= maxNumRowsToBeSortedInMemory) {
    checkCancellation();
    auto& idTableAndLocalVocab = *it;
    totalRows += idTableAndLocalVocab.idTable_.numRows();
    collectedBlocks.push_back(std::move(idTableAndLocalVocab.idTable_));
    mergedLocalVocab.mergeWith(idTableAndLocalVocab.localVocab_);
    ++it;
  }

  // If we exceeded the threshold (by one block), use external sort.
  if (totalRows > maxNumRowsToBeSortedInMemory) {
    return computeResultExternal(std::move(collectedBlocks),
                                 std::move(mergedLocalVocab), std::move(it),
                                 idTables.end(), requestLaziness);
  }

  // Stayed under threshold: concatenate and sort in-memory.
  IdTable combined{numColumns, allocator()};
  combined.reserve(totalRows);
  for (auto& block : collectedBlocks) {
    combined.insertAtEnd(std::move(block));
  }
  return computeResultInMemory(std::move(combined),
                               std::move(mergedLocalVocab));
}

// _____________________________________________________________________________
template <typename LocalVocabT>
Result Sort::computeResultInMemory(IdTable idTable,
                                   LocalVocabT localVocab) const {
  runtimeInfo().addDetail("is-external", "false");

  getExecutionContext()->getSortPerformanceEstimator().throwIfEstimateTooLong(
      idTable.numRows(), idTable.numColumns(), deadline_, "Sort operation");

  Engine::sort(idTable, sortColumnIndices_);

  // Don't report missed timeout check because the in-memory sort is currently
  // not cancellable.
  cancellationHandle_->resetWatchDogState();
  checkCancellation();

  return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
}

// _____________________________________________________________________________
template <typename Iterator, typename Sentinel>
Result Sort::computeResultExternal(std::vector<IdTable> collectedBlocks,
                                   LocalVocab mergedLocalVocab, Iterator it,
                                   Sentinel end, bool requestLaziness) const {
  runtimeInfo().addDetail("is-external", "true");

  // Create a unique temporary filename in the index directory.
  const std::string& onDiskBase =
      getExecutionContext()->getIndex().getOnDiskBase();
  ad_utility::UuidGenerator uuidGen;
  std::string tempFilename =
      absl::StrCat(onDiskBase, ".sort.", uuidGen(), ".dat");

  ad_utility::MemorySize memoryLimit =
      getRuntimeParameter<&RuntimeParameters::sortInMemoryThreshold_>();
  size_t numColumns = subtree_->getResultWidth();

  // We use a `unique_ptr` because the `Sorter` is not moveable (contains
  // `std::atomic`), but the `unique_ptr` can be moved into the lambda below.
  auto sorter = std::make_unique<Sorter>(
      tempFilename, numColumns, memoryLimit, allocator(),
      ad_utility::DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE,
      SortByColumns{sortColumnIndices_});

  // Push already collected blocks.
  for (auto& block : collectedBlocks) {
    sorter->pushBlock(std::move(block));
  }

  // Process remaining blocks.
  while (it != end) {
    checkCancellation();
    auto& idTableAndLocalVocab = *it;
    sorter->pushBlock(std::move(idTableAndLocalVocab.idTable_));
    mergedLocalVocab.mergeWith(idTableAndLocalVocab.localVocab_);
    ++it;
  }

  // If laziness is not requested, materialize the result. The sorter knows the
  // size of the result, so we can reserve exactly the right amount of space.
  if (!requestLaziness) {
    IdTable result{numColumns, allocator()};
    result.reserve(sorter->size());
    for (auto& block : sorter->getSortedBlocks<0>()) {
      checkCancellation();
      result.insertAtEnd(std::move(block));
    }
    cancellationHandle_->resetWatchDogState();
    checkCancellation();
    return {std::move(result), resultSortedOn(), std::move(mergedLocalVocab)};
  }

  // Return a lazy result that yields sorted blocks. Each block gets a clone of
  // the merged local vocab because consumers may read only a subset of blocks.
  auto sortedBlocks = sorter->getSortedBlocks<0>();
  return {Result::LazyResult{ad_utility::CachingTransformInputRange{
              std::move(sortedBlocks),
              [sorter = std::move(sorter),
               mergedLocalVocab = std::move(mergedLocalVocab),
               this](IdTableStatic<0>& block) {
                checkCancellation();
                IdTable idTable = std::move(block).toDynamic();
                return Result::IdTableVocabPair{std::move(idTable),
                                                mergedLocalVocab.clone()};
              }}},
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
