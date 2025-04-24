// Copyright 2015 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2015 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de> [2017 - 2024]

#include "./QueryExecutionTree.h"

#include <array>
#include <memory>
#include <ranges>
#include <string>
#include <vector>

#include "engine/Sort.h"
#include "engine/Union.h"
#include "global/RuntimeParameters.h"

using std::string;

using parsedQuery::SelectClause;

// _____________________________________________________________________________
QueryExecutionTree::QueryExecutionTree(QueryExecutionContext* const qec)
    : qec_(qec) {}

// _____________________________________________________________________________
std::string QueryExecutionTree::getCacheKey() const {
  return cacheKey_.value();
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getVariableColumn(const Variable& variable) const {
  auto optIdx = getVariableColumnOrNullopt(variable);
  if (!optIdx.has_value()) {
    AD_THROW("Variable could not be mapped to result column. Var: " +
             variable.name());
  }
  return optIdx.value();
}
// _____________________________________________________________________________
std::optional<size_t> QueryExecutionTree::getVariableColumnOrNullopt(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(rootOperation_);
  const auto& varCols = getVariableColumns();
  if (!varCols.contains(variable)) {
    return std::nullopt;
  }
  return varCols.at(variable).columnIndex_;
}

// ___________________________________________________________________________
QueryExecutionTree::ColumnIndicesAndTypes
QueryExecutionTree::selectedVariablesToColumnIndices(
    const SelectClause& selectClause, bool includeQuestionMark) const {
  ColumnIndicesAndTypes exportColumns;

  auto variables = selectClause.getSelectedVariables();
  for (const auto& var : variables) {
    std::string varString = var.name();
    if (getVariableColumns().contains(var)) {
      auto columnIndex = getVariableColumns().at(var).columnIndex_;
      // Remove the question mark from the variable name if requested.
      if (!includeQuestionMark && varString.starts_with('?')) {
        varString = varString.substr(1);
      }
      exportColumns.push_back(
          VariableAndColumnIndex{std::move(varString), columnIndex});
    } else {
      exportColumns.emplace_back(std::nullopt);
      LOG(WARN) << "The variable \"" << varString
                << "\" was found in the original query, but not in the "
                   "execution tree. This is likely a bug"
                << std::endl;
    }
  }
  return exportColumns;
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getCostEstimate() {
  // If the result is cached and `zero-cost-estimate-for-cached-subtrees` is set
  // to `true`, we set the cost estimate to zero.
  if (cachedResult_ &&
      RuntimeParameters().get<"zero-cost-estimate-for-cached-subtree">()) {
    return 0;
  }

  // Otherwise, we return the cost estimate of the root operation. For index
  // scans, we assume one unit of work per result row.
  if (getRootOperation()->isIndexScanWithNumVariables(1)) {
    return getSizeEstimate();
  } else {
    return rootOperation_->getCostEstimate();
  }
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getSizeEstimate() {
  if (!sizeEstimate_.has_value()) {
    // Note: Previously we used the exact size instead of the estimate for
    // results that were already in the cache. This however often lead to poor
    // planning, because the query planner compared exact sizes with estimates,
    // which lead to worse plans than just conistently choosing the estimate.
    sizeEstimate_ = rootOperation_->getSizeEstimate();
  }
  return sizeEstimate_.value();
}

//_____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
QueryExecutionTree::setPrefilterGetUpdatedQueryExecutionTree(
    std::vector<Operation::PrefilterVariablePair> prefilterPairs) const {
  AD_CONTRACT_CHECK(rootOperation_);
  VariableToColumnMap varToColMap = getVariableColumns();
  std::erase_if(prefilterPairs, [&varToColMap](const auto& pair) {
    return !varToColMap.contains(pair.second);
  });

  if (prefilterPairs.empty()) {
    return std::nullopt;
  } else {
    return rootOperation_->setPrefilterGetUpdatedQueryExecutionTree(
        prefilterPairs);
  }
}

// _____________________________________________________________________________
bool QueryExecutionTree::knownEmptyResult() {
  if (cachedResult_) {
    AD_CORRECTNESS_CHECK(cachedResult_->isFullyMaterialized());
    return cachedResult_->idTable().size() == 0;
  }
  return rootOperation_->knownEmptyResult();
}

// _____________________________________________________________________________
bool QueryExecutionTree::isVariableCovered(Variable variable) const {
  AD_CONTRACT_CHECK(rootOperation_);
  return getVariableColumns().contains(variable);
}

// _______________________________________________________________________
void QueryExecutionTree::readFromCache() {
  if (!qec_) {
    return;
  }
  auto& cache = qec_->getQueryTreeCache();
  auto res = cache.getIfContained(
      {getCacheKey(), qec_->locatedTriplesSnapshot().index_});
  if (res.has_value()) {
    cachedResult_ = res->_resultPointer->resultTablePtr();
  }
}

// ________________________________________________________________________________________________________________
std::shared_ptr<QueryExecutionTree>
QueryExecutionTree::createSortedTreeAnyPermutation(
    std::shared_ptr<QueryExecutionTree> qet,
    const vector<ColumnIndex>& sortColumns) {
  const auto& sortedOn = qet->resultSortedOn();
  std::span relevantSortedCols{sortedOn.begin(),
                               std::min(sortedOn.size(), sortColumns.size())};
  bool isSorted = ql::ranges::all_of(
      sortColumns, [relevantSortedCols](ColumnIndex distinctCol) {
        return ad_utility::contains(relevantSortedCols, distinctCol);
      });
  return isSorted ? qet : createSortedTree(std::move(qet), sortColumns);
}

// ________________________________________________________________________________________________________________
std::shared_ptr<QueryExecutionTree> QueryExecutionTree::createSortedTree(
    std::shared_ptr<QueryExecutionTree> qet,
    const vector<ColumnIndex>& sortColumns) {
  auto inputSortedOn = qet->resultSortedOn();
  bool inputSorted = sortColumns.size() <= inputSortedOn.size();
  for (size_t i = 0; inputSorted && i < sortColumns.size(); ++i) {
    inputSorted = sortColumns[i] == inputSortedOn[i];
  }
  if (sortColumns.empty() || inputSorted) {
    return qet;
  }

  // Unwrap sort to avoid stacking sorts on top of each other.
  if (auto sort = std::dynamic_pointer_cast<Sort>(qet->getRootOperation())) {
    AD_LOG_DEBUG << "Tried to re-sort a subtree that will already be sorted "
                    "with `Sort` with a different sort order. This is "
                    "indicates a flaw during query planning."
                 << std::endl;
    qet = sort->getSubtree();
  }

  // Push down sort into Union.
  QueryExecutionContext* qec = qet->getRootOperation()->getExecutionContext();
  if (auto unionOperation =
          std::dynamic_pointer_cast<Union>(qet->getRootOperation())) {
    return std::make_shared<QueryExecutionTree>(
        qec, unionOperation->createSortedVariant(sortColumns));
  }

  auto sort = std::make_shared<Sort>(qec, std::move(qet), sortColumns);
  return std::make_shared<QueryExecutionTree>(qec, std::move(sort));
}

// _____________________________________________________________________________
std::vector<std::array<ColumnIndex, 2>> QueryExecutionTree::getJoinColumns(
    const QueryExecutionTree& qetA, const QueryExecutionTree& qetB) {
  std::vector<std::array<ColumnIndex, 2>> jcs;
  const auto& aVarCols = qetA.getVariableColumns();
  const auto& bVarCols = qetB.getVariableColumns();
  for (const auto& aVarCol : aVarCols) {
    auto it = bVarCols.find(aVarCol.first);
    if (it != bVarCols.end()) {
      jcs.push_back(std::array<ColumnIndex, 2>{
          {aVarCol.second.columnIndex_, it->second.columnIndex_}});
    }
  }

  ql::ranges::sort(jcs, ql::ranges::lexicographical_compare);
  return jcs;
}

// ____________________________________________________________________________
std::pair<std::shared_ptr<QueryExecutionTree>,
          std::shared_ptr<QueryExecutionTree>>
QueryExecutionTree::createSortedTrees(
    std::shared_ptr<QueryExecutionTree> qetA,
    std::shared_ptr<QueryExecutionTree> qetB,
    const vector<std::array<ColumnIndex, 2>>& sortColumns) {
  std::vector<ColumnIndex> sortColumnsA, sortColumnsB;
  for (auto [sortColumnA, sortColumnB] : sortColumns) {
    sortColumnsA.push_back(sortColumnA);
    sortColumnsB.push_back(sortColumnB);
  }

  return {createSortedTree(std::move(qetA), sortColumnsA),
          createSortedTree(std::move(qetB), sortColumnsB)};
}

// ____________________________________________________________________________
auto QueryExecutionTree::getSortedSubtreesAndJoinColumns(
    std::shared_ptr<QueryExecutionTree> qetA,
    std::shared_ptr<QueryExecutionTree> qetB) -> SortedTreesAndJoinColumns {
  AD_CORRECTNESS_CHECK(qetA && qetB);
  auto joinCols = getJoinColumns(*qetA, *qetB);
  AD_CONTRACT_CHECK(!joinCols.empty());
  auto [leftSorted, rightSorted] =
      createSortedTrees(std::move(qetA), std::move(qetB), joinCols);
  return {std::move(leftSorted), std::move(rightSorted), std::move(joinCols)};
}

// _____________________________________________________________________________
const VariableToColumnMap::value_type&
QueryExecutionTree::getVariableAndInfoByColumnIndex(ColumnIndex colIdx) const {
  const auto& varColMap = getVariableColumns();
  auto it = ql::ranges::find_if(varColMap, [leftCol = colIdx](const auto& el) {
    return el.second.columnIndex_ == leftCol;
  });
  AD_CONTRACT_CHECK(it != varColMap.end());
  return *it;
}
