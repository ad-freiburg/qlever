// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./QueryExecutionTree.h"

#include <algorithm>
#include <sstream>
#include <string>

#include "./Distinct.h"
#include "./Filter.h"
#include "./IndexScan.h"
#include "./Join.h"
#include "./OrderBy.h"
#include "./Sort.h"
#include "TextOperationWithFilter.h"
#include "TextOperationWithoutFilter.h"
#include "TwoColumnJoin.h"

using std::string;

// _____________________________________________________________________________
QueryExecutionTree::QueryExecutionTree(QueryExecutionContext* const qec)
    : _qec(qec),
      _variableColumnMap(),
      _rootOperation(nullptr),
      _type(OperationType::UNDEFINED),
      _contextVars(),
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
    _asString = os.str();
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
void QueryExecutionTree::setVariableColumn(const string& variable,
                                           size_t column) {
  _variableColumnMap[variable] = column;
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getVariableColumn(const string& variable) const {
  if (_variableColumnMap.count(variable) == 0) {
    AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
             "Variable could not be mapped to result column. Var: " + variable);
  }
  return _variableColumnMap.find(variable)->second;
}

// _____________________________________________________________________________
void QueryExecutionTree::setVariableColumns(
    ad_utility::HashMap<string, size_t> const& map) {
  _variableColumnMap = map;
}

// _____________________________________________________________________________
void QueryExecutionTree::writeResultToStream(std::ostream& out,
                                             const vector<string>& selectVars,
                                             size_t limit, size_t offset,
                                             char sep) const {
  auto generator = generateResults(selectVars, limit, offset, sep);
  while (generator.hasNext()) {
    out << generator.next();
  }
}

// _____________________________________________________________________________
ad_utility::stream_generator::stream_generator
QueryExecutionTree::generateResults(const vector<string>& selectVars,
                                    size_t limit, size_t offset,
                                    char sep) const {
  // They may trigger computation (but does not have to).
  shared_ptr<const ResultTable> res = getResult();
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  vector<std::optional<pair<size_t, ResultTable::ResultType>>> validIndices;
  for (auto var : selectVars) {
    if (ad_utility::startsWith(var, "TEXT(")) {
      var = var.substr(5, var.rfind(')') - 5);
    }
    auto it = getVariableColumns().find(var);
    if (it != getVariableColumns().end()) {
      validIndices.push_back(pair<size_t, ResultTable::ResultType>(
          it->second, res->getResultType(it->second)));
    } else {
      validIndices.push_back(std::nullopt);
    }
  }
  if (validIndices.size() == 0) {
    return {};
  }

  const IdTable& data = res->_idTable;
  size_t upperBound = std::min<size_t>(offset + limit, data.size());
  return writeTable(data, sep, offset, upperBound, std::move(validIndices));
}

// ___________________________________________________________________________
QueryExecutionTree::ColumnIndicesAndTypes
QueryExecutionTree::selectedVariablesToColumnIndices(
    const std::vector<string>& selectVariables,
    const ResultTable& resultTable) const {
  ColumnIndicesAndTypes exportColumns;
  for (auto var : selectVariables) {
    if (ad_utility::startsWith(var, "TEXT(")) {
      var = var.substr(5, var.rfind(')') - 5);
    }
    if (getVariableColumns().contains(var)) {
      auto columnIndex = getVariableColumns().at(var);
      exportColumns.push_back(VariableAndColumnIndex{
          var, columnIndex, resultTable.getResultType(columnIndex)});
    } else {
      exportColumns.emplace_back(std::nullopt);
    }
  }
  return exportColumns;
}

// _____________________________________________________________________________
nlohmann::json QueryExecutionTree::writeResultAsQLeverJson(
    const vector<string>& selectVars, size_t limit, size_t offset) const {
  // They may trigger computation (but does not have to).
  shared_ptr<const ResultTable> res = getResult();
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  ColumnIndicesAndTypes validIndices =
      selectedVariablesToColumnIndices(selectVars, *res);
  if (validIndices.size() == 0) {
    return nlohmann::json(std::vector<std::string>());
  }

  return writeQLeverJsonTable(res->_idTable, offset, limit, validIndices);
}

// _____________________________________________________________________________
nlohmann::json QueryExecutionTree::writeResultAsSparqlJson(
    const vector<string>& selectVars, size_t limit, size_t offset) const {
  using nlohmann::json;

  // This might trigger the actual query processing.
  shared_ptr<const ResultTable> queryResult = getResult();
  LOG(DEBUG) << "Finished computing the query result in the ID space. "
                "Resolving strings in result...\n";
  ColumnIndicesAndTypes columns =
      selectedVariablesToColumnIndices(selectVars, *queryResult);

  std::erase(columns, std::nullopt);

  if (columns.empty()) {
    return {std::vector<std::string>()};
  }

  const IdTable& idTable = queryResult->_idTable;

  json result;
  result["head"]["vars"] = selectVars;

  json bindings = json::array();

  const auto upperBound = std::min(idTable.size(), limit + offset);

  // Take a string from the vocabulary, deduce the type and
  // return a json dict that describes the binding
  auto stringToBinding = [](const std::string& entitystr) -> nlohmann::json {
    nlohmann::ordered_json b;
    // The string is an iri or literal
    if (entitystr[0] == '<') {
      // Strip the <> surrounding the iri
      b["value"] = entitystr.substr(1, entitystr.size() - 2);
      b["type"] = "iri";
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
    nlohmann::ordered_json binding;
    for (const auto& column : columns) {
      const auto& currentId = idTable(rowIndex, column->_columnIndex);
      const auto& optionalValue =
          toStringAndXsdType(currentId, column->_resultType, *queryResult);
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
  return _variableColumnMap.count(var) > 0;
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
QueryExecutionTree::toStringAndXsdType(Id id, ResultTable::ResultType type,
                                       const ResultTable& resultTable) const {
  switch (type) {
    case ResultTable::ResultType::KB: {
      std::optional<string> entity = _qec->getIndex().idToOptionalString(id);
      if (!entity.has_value()) {
        return std::nullopt;
      }
      if (ad_utility::startsWith(entity.value(), VALUE_PREFIX)) {
        return ad_utility::convertIndexWordToLiteralAndType(entity.value());
      }
      return std::pair{std::move(entity.value()), nullptr};
    }
    case ResultTable::ResultType::VERBATIM:
      return std::pair{std::to_string(id), XSD_INT_TYPE};
    case ResultTable::ResultType::TEXT:
      return std::pair{_qec->getIndex().getTextExcerpt(id), nullptr};
    case ResultTable::ResultType::FLOAT: {
      float f;
      // TODO<LLVM 14> std::bit_cast
      std::memcpy(&f, &id, sizeof(float));
      std::stringstream s;
      s << f;
      return std::pair{s.str(), XSD_DECIMAL_TYPE};
    }
    case ResultTable::ResultType::LOCAL_VOCAB: {
      auto optionalString = resultTable.idToOptionalString(id);
      if (!optionalString.has_value()) {
        return std::nullopt;
      }
      return std::pair{optionalString.value(), nullptr};
    }
    default:
      AD_CHECK(false);
  }
}

// __________________________________________________________________________________________________________
nlohmann::json QueryExecutionTree::writeQLeverJsonTable(
    const IdTable& data, size_t from, size_t limit,
    const ColumnIndicesAndTypes& columns) const {
  shared_ptr<const ResultTable> res = getResult();
  nlohmann::json json = nlohmann::json::parse("[]");

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
          toStringAndXsdType(currentId, opt->_resultType, *res);
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

// _________________________________________________________________________________________________________
ad_utility::stream_generator::stream_generator QueryExecutionTree::writeTable(
    const IdTable& data, char sep, size_t from, size_t upperBound,
    const vector<std::optional<pair<size_t, ResultTable::ResultType>>>
        validIndices) const {
  shared_ptr<const ResultTable> res = getResult();
  // special case : binary export of IdTable
  if (sep == 'b') {
    for (size_t i = from; i < upperBound; ++i) {
      for (size_t j = 0; j < validIndices.size(); ++j) {
        if (validIndices[j]) {
          const auto& val = *validIndices[j];
          co_yield std::string_view{
              reinterpret_cast<const char*>(&data(i, val.first)), sizeof(Id)};
        }
      }
    }
    co_return;
  }

  for (size_t i = from; i < upperBound; ++i) {
    for (size_t j = 0; j < validIndices.size(); ++j) {
      if (validIndices[j]) {
        const auto& val = *validIndices[j];
        switch (val.second) {
          case ResultTable::ResultType::KB: {
            string entity = _qec->getIndex()
                                .idToOptionalString(data(i, val.first))
                                .value_or("");
            if (ad_utility::startsWith(entity, VALUE_PREFIX)) {
              co_yield ad_utility::convertIndexWordToValueLiteral(entity);
            } else {
              co_yield entity;
            }
            break;
          }
          case ResultTable::ResultType::VERBATIM:
            co_yield data(i, val.first);
            break;
          case ResultTable::ResultType::TEXT:
            co_yield _qec->getIndex().getTextExcerpt(data(i, val.first));
            break;
          case ResultTable::ResultType::FLOAT: {
            float f;
            std::memcpy(&f, &data(i, val.first), sizeof(float));
            co_yield f;
            break;
          }
          case ResultTable::ResultType::LOCAL_VOCAB: {
            co_yield res->idToOptionalString(data(i, val.first)).value_or("");
            break;
          }
          default:
            AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                     "Cannot deduce output type.");
        }
      }
      co_yield(j + 1 < validIndices.size() ? sep : '\n');
    }
  }
  LOG(DEBUG) << "Done creating readable result.\n";
}
