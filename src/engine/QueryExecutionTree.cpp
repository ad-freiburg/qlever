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
#include "engine/NeutralElementOperation.h"
#include "engine/OrderBy.h"
#include "engine/Sort.h"
#include "engine/TextOperationWithFilter.h"
#include "engine/TransitivePath.h"
#include "engine/Union.h"
#include "engine/Values.h"
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
  AD_CHECK(_rootOperation);
  const auto& varCols = getVariableColumns();
  if (!varCols.contains(variable)) {
    AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
             "Variable could not be mapped to result column. Var: " +
                 variable.name());
  }
  return varCols.at(variable);
}

// ___________________________________________________________________________
QueryExecutionTree::ColumnIndicesAndTypes
QueryExecutionTree::selectedVariablesToColumnIndices(
    const SelectClause& selectClause, const ResultTable& resultTable,
    bool includeQuestionMark) const {
  ColumnIndicesAndTypes exportColumns;

  for (const auto& var : selectClause.getSelectedVariables()) {
    std::string varString = var.name();
    if (getVariableColumns().contains(var)) {
      auto columnIndex = getVariableColumns().at(var);
      // Remove the question mark from the variable name if requested.
      if (!includeQuestionMark && varString.starts_with('?')) {
        varString = varString.substr(1);
      }
      exportColumns.push_back(
          VariableAndColumnIndex{std::move(varString), columnIndex,
                                 resultTable.getResultType(columnIndex)});
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
nlohmann::json QueryExecutionTree::writeResultAsQLeverJson(
    const SelectClause& selectClause, size_t limit, size_t offset,
    shared_ptr<const ResultTable> resultTable) const {
  // They may trigger computation (but does not have to).
  if (!resultTable) {
    resultTable = getResult();
  }
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  ColumnIndicesAndTypes validIndices =
      selectedVariablesToColumnIndices(selectClause, *resultTable, true);
  if (validIndices.empty()) {
    return {std::vector<std::string>()};
  }

  return ExportQueryExecutionTrees::writeQLeverJsonTable(
      *this, offset, limit, validIndices, std::move(resultTable));
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
  AD_CHECK(_rootOperation);
  return getVariableColumns().contains(variable);
}

// _______________________________________________________________________
void QueryExecutionTree::readFromCache() {
  if (!_qec) {
    return;
  }
  auto& cache = _qec->getQueryTreeCache();
  std::shared_ptr<const CacheValue> res = cache.resultAt(asString());
  if (res) {
    _cachedResult = res->_resultTable;
  }
}

// _____________________________________________________________________________
template <QueryExecutionTree::ExportSubFormat format>
ad_utility::streams::stream_generator QueryExecutionTree::generateResults(
    const SelectClause& selectClause, size_t limit, size_t offset) const {
  static_assert(format == ExportSubFormat::BINARY ||
                format == ExportSubFormat::CSV ||
                format == ExportSubFormat::TSV);

  // This call triggers the possibly expensive computation of the query result
  // unless the result is already cached.
  shared_ptr<const ResultTable> resultTable = getResult();
  resultTable->logResultSize();
  LOG(DEBUG) << "Converting result IDs to their corresponding strings ..."
             << std::endl;
  auto selectedColumnIndices =
      selectedVariablesToColumnIndices(selectClause, *resultTable, true);

  const auto& idTable = resultTable->_idTable;
  size_t upperBound = std::min<size_t>(offset + limit, idTable.size());

  // special case : binary export of IdTable
  if constexpr (format == ExportSubFormat::BINARY) {
    for (size_t i = offset; i < upperBound; ++i) {
      for (const auto& columnIndex : selectedColumnIndices) {
        if (columnIndex.has_value()) {
          co_yield std::string_view{reinterpret_cast<const char*>(&idTable(
                                        i, columnIndex.value()._columnIndex)),
                                    sizeof(Id)};
        }
      }
    }
    co_return;
  }

  static constexpr char sep = format == ExportSubFormat::TSV ? '\t' : ',';
  constexpr std::string_view sepView{&sep, 1};
  // Print header line
  const auto& variables = selectClause.getSelectedVariablesAsStrings();
  co_yield absl::StrJoin(variables, sepView);
  co_yield '\n';

  constexpr auto& escapeFunction = format == ExportSubFormat::TSV
                                       ? RdfEscaping::escapeForTsv
                                       : RdfEscaping::escapeForCsv;

  for (size_t i = offset; i < upperBound; ++i) {
    for (size_t j = 0; j < selectedColumnIndices.size(); ++j) {
      if (selectedColumnIndices[j].has_value()) {
        const auto& val = selectedColumnIndices[j].value();
        Id id = idTable(i, val._columnIndex);
        switch (id.getDatatype()) {
          case Datatype::Undefined:
            break;
          case Datatype::Double:
            co_yield id.getDouble();
            break;
          case Datatype::Int:
            co_yield id.getInt();
            break;
          case Datatype::VocabIndex:
            co_yield escapeFunction(
                _qec->getIndex()
                    .getVocab()
                    .indexToOptionalString(id.getVocabIndex())
                    .value_or(""));
            break;
          case Datatype::LocalVocabIndex:
            co_yield escapeFunction(
                resultTable->_localVocab->getWord(id.getLocalVocabIndex()));
            break;
          case Datatype::TextRecordIndex:
            co_yield escapeFunction(
                _qec->getIndex().getTextExcerpt(id.getTextRecordIndex()));
            break;
          default:
            AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                     "Cannot deduce output type.");
        }
      }
      co_yield j + 1 < selectedColumnIndices.size() ? sep : '\n';
    }
  }
  LOG(DEBUG) << "Done creating readable result.\n";
}

// Instantiate template function for all enum types

template ad_utility::streams::stream_generator
QueryExecutionTree::generateResults<QueryExecutionTree::ExportSubFormat::CSV>(
    const SelectClause& selectClause, size_t limit, size_t offset) const;

template ad_utility::streams::stream_generator
QueryExecutionTree::generateResults<QueryExecutionTree::ExportSubFormat::TSV>(
    const SelectClause& selectClause, size_t limit, size_t offset) const;

template ad_utility::streams::stream_generator QueryExecutionTree::
    generateResults<QueryExecutionTree::ExportSubFormat::BINARY>(
        const SelectClause& selectClause, size_t limit, size_t offset) const;

// _____________________________________________________________________________

// _____________________________________________________________________________
template <QueryExecutionTree::ExportSubFormat format>
ad_utility::streams::stream_generator
QueryExecutionTree::writeRdfGraphSeparatedValues(
    const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
    size_t offset, std::shared_ptr<const ResultTable> resultTable) const {
  static_assert(format == ExportSubFormat::BINARY ||
                format == ExportSubFormat::CSV ||
                format == ExportSubFormat::TSV);
  if constexpr (format == ExportSubFormat::BINARY) {
    throw std::runtime_error{
        "Binary export is not supported for CONSTRUCT queries"};
  }
  resultTable->logResultSize();
  constexpr auto& escapeFunction = format == ExportSubFormat::TSV
                                       ? RdfEscaping::escapeForTsv
                                       : RdfEscaping::escapeForCsv;
  constexpr char sep = format == ExportSubFormat::TSV ? '\t' : ',';
  auto generator = ExportQueryExecutionTrees::generateRdfGraph(
      *this, constructTriples, limit, offset, resultTable);
  for (auto& triple : generator) {
    co_yield escapeFunction(std::move(triple._subject));
    co_yield sep;
    co_yield escapeFunction(std::move(triple._predicate));
    co_yield sep;
    co_yield escapeFunction(std::move(triple._object));
    co_yield "\n";
  }
}

// Instantiate template function for all enum types

template ad_utility::streams::stream_generator QueryExecutionTree::
    writeRdfGraphSeparatedValues<QueryExecutionTree::ExportSubFormat::CSV>(
        const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
        size_t offset, std::shared_ptr<const ResultTable> res) const;

template ad_utility::streams::stream_generator QueryExecutionTree::
    writeRdfGraphSeparatedValues<QueryExecutionTree::ExportSubFormat::TSV>(
        const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
        size_t offset, std::shared_ptr<const ResultTable> res) const;

template ad_utility::streams::stream_generator QueryExecutionTree::
    writeRdfGraphSeparatedValues<QueryExecutionTree::ExportSubFormat::BINARY>(
        const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
        size_t offset, std::shared_ptr<const ResultTable> res) const;

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

// ________________________________________________________________________________________________________________
std::shared_ptr<QueryExecutionTree> QueryExecutionTree::createSortedTree(
    std::shared_ptr<QueryExecutionTree> qet,
    const vector<size_t>& sortColumns) {
  auto inputSortedOn = qet->resultSortedOn();
  bool inputSorted = sortColumns.size() <= inputSortedOn.size();
  for (size_t i = 0; inputSorted && i < sortColumns.size(); ++i) {
    inputSorted = sortColumns[i] == inputSortedOn[i];
  }
  if (sortColumns.empty() || inputSorted) {
    return qet;
  }

  QueryExecutionContext* qec = qet->getRootOperation()->getExecutionContext();
  if (sortColumns.size() == 1) {
    auto sort = std::make_shared<Sort>(qec, std::move(qet), sortColumns[0]);
    return std::make_shared<QueryExecutionTree>(qec, std::move(sort));
  } else {
    std::vector<std::pair<size_t, bool>> sortColsForOrderBy;
    for (auto i : sortColumns) {
      // The second argument set to `false` means sort in ascending order.
      // TODO<joka921> fix this.
      sortColsForOrderBy.emplace_back(i, false);
    }
    auto sort = std::make_shared<OrderBy>(qec, std::move(qet),
                                          std::move(sortColsForOrderBy));
    return std::make_shared<QueryExecutionTree>(qec, std::move(sort));
  }
}
