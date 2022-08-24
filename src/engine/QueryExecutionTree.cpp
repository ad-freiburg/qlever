// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./QueryExecutionTree.h"

#include <absl/strings/str_join.h>
#include <engine/Bind.h>
#include <engine/Distinct.h>
#include <engine/Filter.h>
#include <engine/GroupBy.h>
#include <engine/HasPredicateScan.h>
#include <engine/IndexScan.h>
#include <engine/Join.h>
#include <engine/NeutralElementOperation.h>
#include <engine/OrderBy.h>
#include <engine/Sort.h>
#include <engine/TextOperationWithFilter.h>
#include <engine/TransitivePath.h>
#include <engine/TwoColumnJoin.h>
#include <engine/Union.h>
#include <engine/Values.h>
#include <parser/RdfEscaping.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <utility>

using std::string;

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
size_t QueryExecutionTree::getVariableColumn(const string& variable) const {
  AD_CHECK(_rootOperation);
  // TODO<joka921> unnecessary copies here...
  const auto& varCols = _rootOperation->getVariableColumns();
  if (!varCols.contains(variable)) {
    AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
             "Variable could not be mapped to result column. Var: " + variable);
  }
  return varCols.find(variable)->second;
}

// ___________________________________________________________________________
QueryExecutionTree::ColumnIndicesAndTypes
QueryExecutionTree::selectedVariablesToColumnIndices(
    const SelectClause& selectClause, const ResultTable& resultTable,
    bool includeQuestionMark) const {
  ColumnIndicesAndTypes exportColumns;

  for (const auto& var : selectClause.getSelectedVariables()) {
    std::string varString = var.name();
    if (getVariableColumns().contains(varString)) {
      auto columnIndex = getVariableColumns().at(varString);
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

  return writeQLeverJsonTable(offset, limit, validIndices,
                              std::move(resultTable));
}

// _____________________________________________________________________________
nlohmann::json QueryExecutionTree::writeResultAsSparqlJson(
    const SelectClause& selectClause, size_t limit, size_t offset,
    shared_ptr<const ResultTable> resultTable) const {
  using nlohmann::json;

  // This might trigger the actual query processing.
  if (!resultTable) {
    resultTable = getResult();
  }
  LOG(DEBUG) << "Finished computing the query result in the ID space. "
                "Resolving strings in result...\n";

  // Don't include the question mark in the variable names.
  ColumnIndicesAndTypes columns =
      selectedVariablesToColumnIndices(selectClause, *resultTable, false);

  std::erase(columns, std::nullopt);

  if (columns.empty()) {
    return {std::vector<std::string>()};
  }

  const IdTable& idTable = resultTable->_idTable;

  json result;
  std::vector<std::string> selectedVars =
      selectClause.getSelectedVariablesAsStrings();
  // Strip the leading '?' from the variables, it is not part of the SPARQL JSON
  // output format.
  for (auto& var : selectedVars) {
    if (std::string_view{var}.starts_with('?')) {
      var = var.substr(1);
    }
  }
  result["head"]["vars"] = selectedVars;

  json bindings = json::array();

  const auto upperBound = std::min(idTable.size(), limit + offset);

  // Take a string from the vocabulary, deduce the type and
  // return a json dict that describes the binding
  auto stringToBinding = [](std::string_view entitystr) -> nlohmann::json {
    nlohmann::ordered_json b;
    // The string is an IRI or literal.
    if (entitystr.starts_with('<')) {
      // Strip the <> surrounding the iri.
      b["value"] = entitystr.substr(1, entitystr.size() - 2);
      // Even if they are technically IRIs, the format needs the type to be
      // "uri".
      b["type"] = "uri";
    } else if (entitystr.starts_with("_:")) {
      b["value"] = entitystr.substr(2);
      b["type"] = "bnode";
    } else {
      size_t quote_pos = entitystr.rfind('"');
      if (quote_pos == std::string::npos) {
        // TEXT entries are currently not surrounded by quotes
        b["value"] = entitystr;
        b["type"] = "literal";
      } else {
        b["value"] = entitystr.substr(1, quote_pos - 1);
        b["type"] = "literal";
        // Look for a language tag or type.
        if (quote_pos < entitystr.size() - 1 &&
            entitystr[quote_pos + 1] == '@') {
          b["xml:lang"] = entitystr.substr(quote_pos + 2);
        } else if (quote_pos < entitystr.size() - 2 &&
                   entitystr[quote_pos + 1] == '^') {
          AD_CHECK(entitystr[quote_pos + 2] == '^');
          std::string_view datatype{entitystr};
          // remove the < angledBrackets> around the datatype IRI
          AD_CHECK(datatype.size() >= quote_pos + 5);
          datatype.remove_prefix(quote_pos + 4);
          datatype.remove_suffix(1);
          b["datatype"] = datatype;
          ;
        }
      }
    }
    return b;
  };

  for (size_t rowIndex = offset; rowIndex < upperBound; ++rowIndex) {
    // TODO: ordered_json` entries are ordered alphabetically, but insertion
    // order would be preferable.
    nlohmann::ordered_json binding;
    for (const auto& column : columns) {
      const auto& currentId = idTable(rowIndex, column->_columnIndex);
      const auto& optionalValue = idToStringAndType(currentId, *resultTable);
      if (!optionalValue.has_value()) {
        continue;
      }
      const auto& [stringValue, xsdType] = optionalValue.value();
      nlohmann::ordered_json b;
      if (!xsdType) {
        // no xsdType, this means that `stringValue` is a plain string literal
        // or entity
        b = stringToBinding(stringValue);
      } else {
        b["value"] = stringValue;
        b["type"] = "literal";
        b["datatype"] = xsdType;
      }
      binding[column->_variable] = std::move(b);
    }
    bindings.emplace_back(std::move(binding));
  }
  result["results"]["bindings"] = std::move(bindings);
  return result;
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
bool QueryExecutionTree::varCovered(string var) const {
  AD_CHECK(_rootOperation);
  return _rootOperation->getVariableColumns().contains(var);
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

// ___________________________________________________________________________
std::optional<std::pair<std::string, const char*>>
QueryExecutionTree::idToStringAndType(Id id,
                                      const ResultTable& resultTable) const {
  // TODO<joka921> This is one of the central methods which we have to rewrite
  switch (id.getDatatype()) {
    case Datatype::Undefined:
      return std::nullopt;
    case Datatype::Double: {
      // Format as integer if fractional part is zero, let C++ decide otherwise.
      std::stringstream ss;
      double d = id.getDouble();
      double dIntPart;
      if (std::modf(d, &dIntPart) == 0.0) {
        ss << std::fixed << std::setprecision(0) << id.getDouble();
      } else {
        ss << d;
      }
      return std::pair{std::move(ss).str(), XSD_DECIMAL_TYPE};
    }
    case Datatype::Int:
      return std::pair{std::to_string(id.getInt()), XSD_INT_TYPE};
    case Datatype::VocabIndex: {
      std::optional<string> entity = _qec->getIndex().idToOptionalString(id);
      if (!entity.has_value()) {
        return std::nullopt;
      }
      return std::pair{std::move(entity.value()), nullptr};
    }
    case Datatype::LocalVocabIndex: {
      auto optionalString =
          resultTable.indexToOptionalString(id.getLocalVocabIndex());
      if (!optionalString.has_value()) {
        return std::nullopt;
      }
      return std::pair{optionalString.value(), nullptr};
    }
    case Datatype::TextRecordIndex:
      return std::pair{_qec->getIndex().getTextExcerpt(id.getTextRecordIndex()),
                       nullptr};
  }
  AD_FAIL();
}

// __________________________________________________________________________________________________________
nlohmann::json QueryExecutionTree::writeQLeverJsonTable(
    size_t from, size_t limit, const ColumnIndicesAndTypes& columns,
    shared_ptr<const ResultTable> resultTable) const {
  if (!resultTable) {
    resultTable = getResult();
  }
  const IdTable& data = resultTable->_idTable;
  nlohmann::json json = nlohmann::json::array();

  const auto upperBound = std::min(data.size(), limit + from);

  for (size_t rowIndex = from; rowIndex < upperBound; ++rowIndex) {
    json.emplace_back();
    auto& row = json.back();
    for (const auto& opt : columns) {
      if (!opt) {
        row.emplace_back(nullptr);
        continue;
      }
      const auto& currentId = data(rowIndex, opt->_columnIndex);
      const auto& optionalStringAndXsdType =
          idToStringAndType(currentId, *resultTable);
      if (!optionalStringAndXsdType.has_value()) {
        row.emplace_back(nullptr);
        continue;
      }
      const auto& [stringValue, xsdType] = optionalStringAndXsdType.value();
      if (xsdType) {
        row.emplace_back('"' + stringValue + "\"^^<" + xsdType + '>');
      } else {
        row.emplace_back(stringValue);
      }
    }
  }
  return json;
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
                resultTable->indexToOptionalString(id.getLocalVocabIndex())
                    .value_or(""));
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
      co_yield(j + 1 < selectedColumnIndices.size() ? sep : '\n');
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

cppcoro::generator<QueryExecutionTree::StringTriple>
QueryExecutionTree::generateRdfGraph(
    const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
    size_t offset, std::shared_ptr<const ResultTable> res) const {
  size_t upperBound = std::min<size_t>(offset + limit, res->_idTable.size());
  auto variableColumns = getVariableColumns();
  for (size_t i = offset; i < upperBound; i++) {
    Context context{i, *res, variableColumns, _qec->getIndex()};
    for (const auto& triple : constructTriples) {
      auto subject = triple[0].evaluate(context, SUBJECT);
      auto predicate = triple[1].evaluate(context, PREDICATE);
      auto object = triple[2].evaluate(context, OBJECT);
      if (!subject.has_value() || !predicate.has_value() ||
          !object.has_value()) {
        continue;
      }
      co_yield {std::move(subject.value()), std::move(predicate.value()),
                std::move(object.value())};
    }
  }
}

// _____________________________________________________________________________
ad_utility::streams::stream_generator QueryExecutionTree::writeRdfGraphTurtle(
    const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
    size_t offset, std::shared_ptr<const ResultTable> resultTable) const {
  resultTable->logResultSize();
  auto generator =
      generateRdfGraph(constructTriples, limit, offset, resultTable);
  for (const auto& triple : generator) {
    co_yield triple._subject;
    co_yield ' ';
    co_yield triple._predicate;
    co_yield ' ';
    co_yield triple._object;
    co_yield " .\n";
  }
}

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
  auto generator =
      generateRdfGraph(constructTriples, limit, offset, resultTable);
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

// _____________________________________________________________________________
nlohmann::json QueryExecutionTree::writeRdfGraphJson(
    const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
    size_t offset, std::shared_ptr<const ResultTable> res) const {
  auto generator =
      generateRdfGraph(constructTriples, limit, offset, std::move(res));
  std::vector<std::array<std::string, 3>> jsonArray;
  for (auto& triple : generator) {
    jsonArray.push_back({std::move(triple._subject),
                         std::move(triple._predicate),
                         std::move(triple._object)});
  }
  return jsonArray;
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
  } else if constexpr (std::is_same_v<Op, TwoColumnJoin>) {
    _type = TWO_COL_JOIN;
  } else if constexpr (std::is_same_v<Op, TextOperationWithFilter>) {
    _type = TEXT_WITH_FILTER;
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
template void QueryExecutionTree::setOperation(std::shared_ptr<TwoColumnJoin>);
template void QueryExecutionTree::setOperation(
    std::shared_ptr<TextOperationWithFilter>);

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
