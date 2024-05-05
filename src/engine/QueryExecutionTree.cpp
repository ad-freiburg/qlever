// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./QueryExecutionTree.h"

#include <array>
#include <memory>
#include <ranges>
#include <string>
#include <vector>

#include "engine/Sort.h"
#include "parser/RdfEscaping.h"

using std::string;

using parsedQuery::SelectClause;

// _____________________________________________________________________________
QueryExecutionTree::QueryExecutionTree(QueryExecutionContext* const qec)
    : qec_(qec) {}

// _____________________________________________________________________________
std::string QueryExecutionTree::getCacheKey() const {
  return rootOperation_->getCacheKey();
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getVariableColumn(const Variable& variable) const {
  AD_CONTRACT_CHECK(rootOperation_);
  const auto& varCols = getVariableColumns();
  if (!varCols.contains(variable)) {
    AD_THROW("Variable could not be mapped to result column. Var: " +
             variable.name());
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
  if (cachedResult_) {
    // result is pinned in cache. Nothing to compute
    return 0;
  }
  if (getRootOperation()->isIndexScanWithNumVariables(1)) {
    return getSizeEstimate();
  } else {
    return rootOperation_->getCostEstimate();
  }
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getSizeEstimate() {
  if (!sizeEstimate_.has_value()) {
    if (cachedResult_) {
      sizeEstimate_ = cachedResult_->size();
    } else {
      // if we are in a unit test setting and there is no QueryExecutionContest
      // specified it is the rootOperation_'s obligation to handle this case
      // correctly
      sizeEstimate_ = rootOperation_->getSizeEstimate();
    }
  }
  return sizeEstimate_.value();
}

// _____________________________________________________________________________
bool QueryExecutionTree::knownEmptyResult() {
  if (cachedResult_) {
    return cachedResult_->size() == 0;
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
  auto res = cache.getIfContained(getCacheKey());
  if (res.has_value()) {
    cachedResult_ = res->_resultPointer->resultTable();
  }
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

  QueryExecutionContext* qec = qet->getRootOperation()->getExecutionContext();
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

  std::ranges::sort(jcs, std::ranges::lexicographical_compare);
  return jcs;
}

// ____________________________________________________________________________
std::pair<std::shared_ptr<QueryExecutionTree>, shared_ptr<QueryExecutionTree>>
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
  auto it = std::ranges::find_if(varColMap, [leftCol = colIdx](const auto& el) {
    return el.second.columnIndex_ == leftCol;
  });
  AD_CONTRACT_CHECK(it != varColMap.end());
  return *it;
}
