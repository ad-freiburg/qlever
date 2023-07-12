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

#include "absl/strings/str_join.h"
#include "engine/Bind.h"
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
#include "engine/TransitivePath.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "engine/ValuesForTesting.h"
#include "parser/RdfEscaping.h"

using std::string;

using parsedQuery::SelectClause;

// _____________________________________________________________________________
QueryExecutionTree::QueryExecutionTree(QueryExecutionContext* const qec)
    : _qec(qec),
      _rootOperation(nullptr),
      _type(OperationType::UNDEFINED),
      _asString(),
      _sizeEstimate(std::numeric_limits<size_t>::max()) {}

// _____________________________________________________________________________
string QueryExecutionTree::asString(size_t indent) {
  if (indent == _indent && !_asString.empty()) {
    return _asString;
  }
  string indentStr;
  for (size_t i = 0; i < indent; ++i) {
    indentStr += " ";
  }
  if (_rootOperation) {
    std::ostringstream os;
    os << indentStr << "{\n"
       << _rootOperation->asString(indent + 2) << "\n"
       << indentStr << "  qet-width: " << getResultWidth() << " ";
    os << '\n' << indentStr << '}';
    _asString = std::move(os).str();
  } else {
    _asString = "<Empty QueryExecutionTree>";
  }
  _indent = indent;
  return _asString;
}

// _____________________________________________________________________________
void QueryExecutionTree::setOperation(QueryExecutionTree::OperationType type,
                                      std::shared_ptr<Operation> op) {
  _type = type;
  _rootOperation = std::move(op);
  _asString = "";
  _sizeEstimate = std::numeric_limits<size_t>::max();
  // with setting the operation the initialization is done and we can try to
  // find our result in the cache.
  readFromCache();
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getVariableColumn(const Variable& variable) const {
  AD_CONTRACT_CHECK(_rootOperation);
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

  for (const auto& var : selectClause.getSelectedVariables()) {
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
  if (_cachedResult) {
    // result is pinned in cache. Nothing to compute
    return 0;
  }
  if (_type == QueryExecutionTree::SCAN && getResultWidth() == 1) {
    return getSizeEstimate();
  } else {
    return _rootOperation->getCostEstimate();
  }
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getSizeEstimate() {
  if (_sizeEstimate == std::numeric_limits<size_t>::max()) {
    if (_cachedResult) {
      _sizeEstimate = _cachedResult->size();
    } else {
      // if we are in a unit test setting and there is no QueryExecutionContest
      // specified it is the _rootOperation's obligation to handle this case
      // correctly
      _sizeEstimate = _rootOperation->getSizeEstimate();
    }
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
bool QueryExecutionTree::knownEmptyResult() {
  if (_cachedResult) {
    return _cachedResult->size() == 0;
  }
  return _rootOperation->knownEmptyResult();
}

// _____________________________________________________________________________
bool QueryExecutionTree::isVariableCovered(Variable variable) const {
  AD_CONTRACT_CHECK(_rootOperation);
  return getVariableColumns().contains(variable);
}

// _______________________________________________________________________
void QueryExecutionTree::readFromCache() {
  if (!_qec) {
    return;
  }
  auto& cache = _qec->getQueryTreeCache();
  auto res = cache.getIfContained(asString());
  if (res.has_value()) {
    _cachedResult = res->_resultPointer->resultTable();
  }
}

bool QueryExecutionTree::isIndexScan() const { return _type == SCAN; }

template <typename Op>
void QueryExecutionTree::setOperation(std::shared_ptr<Op> operation) {
  if constexpr (std::is_same_v<Op, IndexScan>) {
    _type = SCAN;
  } else if constexpr (std::is_same_v<Op, Union>) {
    _type = UNION;
  } else if constexpr (std::is_same_v<Op, Bind>) {
    _type = BIND;
  } else if constexpr (std::is_same_v<Op, Sort>) {
    _type = SORT;
  } else if constexpr (std::is_same_v<Op, Distinct>) {
    _type = DISTINCT;
  } else if constexpr (std::is_same_v<Op, Values>) {
    _type = VALUES;
  } else if constexpr (std::is_same_v<Op, Service>) {
    _type = SERVICE;
  } else if constexpr (std::is_same_v<Op, TransitivePath>) {
    _type = TRANSITIVE_PATH;
  } else if constexpr (std::is_same_v<Op, OrderBy>) {
    _type = ORDER_BY;
  } else if constexpr (std::is_same_v<Op, GroupBy>) {
    _type = GROUP_BY;
  } else if constexpr (std::is_same_v<Op, HasPredicateScan>) {
    _type = HAS_PREDICATE_SCAN;
  } else if constexpr (std::is_same_v<Op, Filter>) {
    _type = FILTER;
  } else if constexpr (std::is_same_v<Op, NeutralElementOperation>) {
    _type = NEUTRAL_ELEMENT;
  } else if constexpr (std::is_same_v<Op, Join>) {
    _type = JOIN;
  } else if constexpr (std::is_same_v<Op, TextOperationWithFilter>) {
    _type = TEXT_WITH_FILTER;
  } else if constexpr (std::is_same_v<Op, CountAvailablePredicates>) {
    _type = COUNT_AVAILABLE_PREDICATES;
  } else if constexpr (std::is_same_v<Op, Minus>) {
    _type = MINUS;
  } else if constexpr (std::is_same_v<Op, OptionalJoin>) {
    _type = OPTIONAL_JOIN;
  } else if constexpr (std::is_same_v<Op, MultiColumnJoin>) {
    _type = MULTICOLUMN_JOIN;
  } else if constexpr (std::is_same_v<Op, ValuesForTesting> ||
                       std::is_same_v<Op, ValuesForTestingNoKnownEmptyResult>) {
    _type = DUMMY;
  } else {
    static_assert(ad_utility::alwaysFalse<Op>,
                  "New type of operation that was not yet registered");
  }
  _rootOperation = std::move(operation);
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
    std::shared_ptr<CountAvailablePredicates>);
template void QueryExecutionTree::setOperation(std::shared_ptr<Minus>);
template void QueryExecutionTree::setOperation(std::shared_ptr<OptionalJoin>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<MultiColumnJoin>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<ValuesForTesting>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<ValuesForTestingNoKnownEmptyResult>);

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

// ____________________________________________________________________________
std::array<std::shared_ptr<QueryExecutionTree>, 2>
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
