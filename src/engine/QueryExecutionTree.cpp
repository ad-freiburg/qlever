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

  const IdTable& data = res->_data;
  size_t upperBound = std::min<size_t>(offset + limit, data.size());
  return writeTable(data, sep, offset, upperBound, std::move(validIndices));
}

// ___________________________________________________________________________
QueryExecutionTree::ExportColumnIndicesAndTypes
QueryExecutionTree::selectedVariablesToColumnIndices(
    const std::vector<string>& selectVariables,
    const ResultTable& resultTable) const {
  vector<std::optional<pair<size_t, ResultTable::ResultType>>> exportColumns;
  for (auto var : selectVariables) {
    if (ad_utility::startsWith(var, "TEXT(")) {
      var = var.substr(5, var.rfind(')') - 5);
    }
    auto vc = getVariableColumns().find(var);
    if (getVariableColumns().contains(var)) {
      auto columnIndex = getVariableColumns().at(var);
      exportColumns.emplace_back(
          std::pair{columnIndex, resultTable.getResultType(columnIndex)});
    } else {
      exportColumns.emplace_back(std::nullopt);
    }
  }
  return exportColumns;
}

// TODO<joka921> proper type + comment.
std::vector<std::tuple<size_t, size_t, ResultTable::ResultType>>
toVariableColumnToResultColumnMap(
    const QueryExecutionTree::ExportColumnIndicesAndTypes& columns) {
  std::vector<std::tuple<size_t, size_t, ResultTable::ResultType>> result;
  for (size_t i = 0; i < columns.size(); ++i) {
    const auto& opt = columns[i];
    if (opt.has_value()) {
      result.emplace_back(i, opt->first, opt->second);
    }
  }
  return result;
}

// _____________________________________________________________________________
nlohmann::json QueryExecutionTree::writeResultAsJson(
    const vector<string>& selectVars, size_t limit, size_t offset) const {
  // They may trigger computation (but does not have to).
  shared_ptr<const ResultTable> res = getResult();
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  ExportColumnIndicesAndTypes validIndices =
      selectedVariablesToColumnIndices(selectVars, *res);
  if (validIndices.size() == 0) {
    return nlohmann::json(std::vector<std::string>());
  }

  return writeJsonTable(res->_data, offset, limit, validIndices);
}

// _____________________________________________________________________________
nlohmann::json QueryExecutionTree::writeResultAsSparqlJson(
    const vector<string>& selectVars, size_t limit, size_t offset) const {
  using nlohmann::json;

  // They may trigger computation (but does not have to).
  shared_ptr<const ResultTable> res = getResult();
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  ExportColumnIndicesAndTypes validIndices =
      selectedVariablesToColumnIndices(selectVars, *res);
  auto inToOutColumns = toVariableColumnToResultColumnMap(validIndices);

  if (inToOutColumns.empty()) {
    return {std::vector<std::string>()};
  }

  const IdTable& data = res->_data;

  json head;
  head["vars"] = selectVars;

  json encoded;
  encoded["head"] = head;

  json results;
  json bindings = json::array();

  const auto upperBound = std::min(data.size(), limit + offset);

  // Take a string from the vocabulary, deduce the type and
  // return a json dict that describes the binding
  auto strToBinding = [](const std::string& entitystr) -> nlohmann::json {
    json b;
    // The string is an iri or literal
    if (entitystr[0] == '<') {
      b["type"] = "iri";
      // Strip the <> surrounding the iri
      b["value"] = entitystr.substr(1, entitystr.size() - 2);
    } else {
      b["type"] = "literal";
      size_t quote_pos = entitystr.rfind('"');
      AD_CHECK(quote_pos != std::string::npos);
      b["value"] = entitystr.substr(1, quote_pos - 1);
      // Look for a language tag
      if (quote_pos < entitystr.size() - 1 && entitystr[quote_pos + 1] == '@') {
        b["xml:lang"] = entitystr.substr(quote_pos + 2);
      }
    }
    return b;
  };

  for (size_t i = offset; i < upperBound; ++i) {
    json binding;
    for (const auto& [variableIdx, columnIdx, type] : inToOutColumns) {
      const auto& currentId = data(i, columnIdx);
      const auto& [optionalValue, datatype] =
          idToStringAndDatatype(currentId, type, *res);
      if (!optionalValue.has_value()) {
        continue;
      }
      json b;
      if (!datatype) {
        // no datatype, this means that there is a plain string literal or
        // entity
        b = strToBinding(optionalValue.value());
      } else {
        b["value"] = optionalValue.value();
        b["type"] = "literal";
        b["datatype"] = datatype;
      }
      binding[selectVars[variableIdx]] = std::move(b);
    }
    bindings.emplace_back(std::move(binding));
  }
  results["bindings"] = bindings;
  encoded["results"] = results;
  return encoded;
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
std::pair<std::optional<std::string>, const char*>
QueryExecutionTree::idToStringAndDatatype(
    Id id, ResultTable::ResultType type, const ResultTable& resultTable) const {
  switch (type) {
    case ResultTable::ResultType::KB: {
      std::optional<string> entity = _qec->getIndex().idToOptionalString(id);
      if (entity) {
        const string& entitystr = entity.value();
        if (ad_utility::startsWith(entitystr, VALUE_PREFIX)) {
          return ad_utility::convertIndexWordToLiteralAndType(entitystr);
        }
      } else {
        return {std::move(entity), nullptr};
      }
    }
    case ResultTable::ResultType::VERBATIM:
      return {std::to_string(id), XSD_INT_TYPE};
    case ResultTable::ResultType::TEXT:
      return {_qec->getIndex().getTextExcerpt(id), nullptr};
    case ResultTable::ResultType::FLOAT: {
      float f;
      // TODO<LLVM 14> std::bit_cast
      std::memcpy(&f, &id, sizeof(float));
      std::stringstream s;
      s << f;
      return {s.str(), XSD_DECIMAL_TYPE};
    }
    case ResultTable::ResultType::LOCAL_VOCAB: {
      return {resultTable.idToOptionalString(id), nullptr};
    }
    default:
      AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
               "Cannot deduce output type.");
  }
}

// __________________________________________________________________________________________________________
nlohmann::json QueryExecutionTree::writeJsonTable(
    const IdTable& data, size_t from, size_t limit,
    const vector<std::optional<pair<size_t, ResultTable::ResultType>>>&
        validIndices) const {
  shared_ptr<const ResultTable> res = getResult();
  nlohmann::json json = nlohmann::json::parse("[]");
  auto toJson = [](const std::pair<std::optional<std::string>, const char*>&
                       valueAndDatatype) -> nlohmann::json {
    const auto& [value, datatype] = valueAndDatatype;
    if (!value.has_value()) {
      return nullptr;
    }
    if (!datatype) {
      return value.value();
    }
    return '"' + value.value() + "\"^^<" + datatype + '>';
  };

  const auto upperBound = std::min(data.size(), limit + from);

  for (size_t i = from; i < upperBound; ++i) {
    json.emplace_back();
    auto& row = json.back();
    for (const auto& opt : validIndices) {
      if (!opt) {
        row.emplace_back(nullptr);
        continue;
      }
      const auto& [columnIndex, type] = *opt;
      const auto& currentId = data(i, columnIndex);
      const auto& [value, datatype] =
          idToStringAndDatatype(currentId, type, *res);
      if (!value.has_value()) {
        row.emplace_back(nullptr);
      } else if (!datatype) {
        row.emplace_back(value.value());
      } else {
        row.emplace_back('"' + value.value() + "\"^^<" + datatype + '>');
      }
      row.emplace_back(toJson(idToStringAndDatatype(i, type, *res)));
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
