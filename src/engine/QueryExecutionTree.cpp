// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./QueryExecutionTree.h"

#include <algorithm>
#include <sstream>
#include <string>
#include <utility>

#include "engine/Bind.h"
#include "engine/CartesianProductJoin.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/Distinct.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/Filter.h"
#include "engine/GroupBy.h"
#include "engine/HasPredicateScan.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Minus.h"
#include "engine/MultiColumnJoin.h"
#include "engine/NeutralElementOperation.h"
#include "engine/OptionalJoin.h"
#include "engine/OrderBy.h"
#include "engine/Service.h"
#include "engine/Sort.h"
#include "engine/TextOperationWithFilter.h"
#include "engine/TextOperationWithoutFilter.h"
#include "engine/TransitivePath.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "engine/ValuesForTesting.h"
#include "parser/RdfEscaping.h"

using std::string;

using parsedQuery::SelectClause;

// _____________________________________________________________________________
QueryExecutionTree::QueryExecutionTree(QueryExecutionContext* const qec)
    : qec_(qec) {}

// _____________________________________________________________________________
string QueryExecutionTree::getCacheKey() const {
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
  if (type_ == QueryExecutionTree::SCAN && getResultWidth() == 1) {
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

template <typename Op>
void QueryExecutionTree::setOperation(std::shared_ptr<Op> operation) {
  if constexpr (std::is_same_v<Op, IndexScan>) {
    type_ = SCAN;
  } else if constexpr (std::is_same_v<Op, Union>) {
    type_ = UNION;
  } else if constexpr (std::is_same_v<Op, Bind>) {
    type_ = BIND;
  } else if constexpr (std::is_same_v<Op, Sort>) {
    type_ = SORT;
  } else if constexpr (std::is_same_v<Op, Distinct>) {
    type_ = DISTINCT;
  } else if constexpr (std::is_same_v<Op, Values>) {
    type_ = VALUES;
  } else if constexpr (std::is_same_v<Op, Service>) {
    type_ = SERVICE;
  } else if constexpr (std::is_same_v<Op, TransitivePath>) {
    type_ = TRANSITIVE_PATH;
  } else if constexpr (std::is_same_v<Op, OrderBy>) {
    type_ = ORDER_BY;
  } else if constexpr (std::is_same_v<Op, GroupBy>) {
    type_ = GROUP_BY;
  } else if constexpr (std::is_same_v<Op, HasPredicateScan>) {
    type_ = HAS_PREDICATE_SCAN;
  } else if constexpr (std::is_same_v<Op, Filter>) {
    type_ = FILTER;
  } else if constexpr (std::is_same_v<Op, NeutralElementOperation>) {
    type_ = NEUTRAL_ELEMENT;
  } else if constexpr (std::is_same_v<Op, Join>) {
    type_ = JOIN;
  } else if constexpr (std::is_same_v<Op, TextOperationWithFilter>) {
    type_ = TEXT_WITH_FILTER;
  } else if constexpr (std::is_same_v<Op, TextOperationWithoutFilter>) {
    type_ = TEXT_WITHOUT_FILTER;
  } else if constexpr (std::is_same_v<Op, CountAvailablePredicates>) {
    type_ = COUNT_AVAILABLE_PREDICATES;
  } else if constexpr (std::is_same_v<Op, Minus>) {
    type_ = MINUS;
  } else if constexpr (std::is_same_v<Op, OptionalJoin>) {
    type_ = OPTIONAL_JOIN;
  } else if constexpr (std::is_same_v<Op, MultiColumnJoin>) {
    type_ = MULTICOLUMN_JOIN;
  } else if constexpr (std::is_same_v<Op, ValuesForTesting> ||
                       std::is_same_v<Op, ValuesForTestingNoKnownEmptyResult>) {
    type_ = DUMMY;
  } else if constexpr (std::is_same_v<Op, CartesianProductJoin>) {
    type_ = CARTESIAN_PRODUCT_JOIN;
  } else {
    static_assert(ad_utility::alwaysFalse<Op>,
                  "New type of operation that was not yet registered");
  }
  rootOperation_ = std::move(operation);
  readFromCache();
}

template void QueryExecutionTree::setOperation(std::shared_ptr<IndexScan>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Union>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Bind>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Sort>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Distinct>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Values>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Service>);
template void QueryExecutionTree::setOperation(std::shared_ptr<TransitivePath>);
template void QueryExecutionTree::setOperation(std::shared_ptr<OrderBy>);
template void QueryExecutionTree::setOperation(std::shared_ptr<GroupBy>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<HasPredicateScan>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Filter>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<NeutralElementOperation>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Join>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<TextOperationWithFilter>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<TextOperationWithoutFilter>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<CountAvailablePredicates>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Minus>);
template void QueryExecutionTree::setOperation(std::shared_ptr<OptionalJoin>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<MultiColumnJoin>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<ValuesForTesting>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<ValuesForTestingNoKnownEmptyResult>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<CartesianProductJoin>);

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
