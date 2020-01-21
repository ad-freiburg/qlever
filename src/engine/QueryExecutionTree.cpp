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
  _rootOperation = op;
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
  // They may trigger computation (but does not have to).
  shared_ptr<const ResultTable> res = getResult();
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  vector<pair<size_t, ResultTable::ResultType>> validIndices;
  for (auto var : selectVars) {
    if (ad_utility::startsWith(var, "TEXT(")) {
      var = var.substr(5, var.rfind(')') - 5);
    }
    auto it = getVariableColumns().find(var);
    if (it != getVariableColumns().end()) {
      validIndices.push_back(pair<size_t, ResultTable::ResultType>(
          it->second, res->getResultType(it->second)));
    }
  }
  if (validIndices.size() == 0) {
    return;
  }

  const IdTable& data = res->_data;
  size_t upperBound = std::min<size_t>(offset + limit, data.size());
  writeTable(data, sep, offset, upperBound, validIndices, out);

  LOG(DEBUG) << "Done creating readable result.\n";
}

// _____________________________________________________________________________
nlohmann::json QueryExecutionTree::writeResultAsJson(
    const vector<string>& selectVars, size_t limit, size_t offset) const {
  // They may trigger computation (but does not have to).
  shared_ptr<const ResultTable> res = getResult();
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  vector<pair<size_t, ResultTable::ResultType>> validIndices;
  for (auto var : selectVars) {
    if (ad_utility::startsWith(var, "TEXT(")) {
      var = var.substr(5, var.rfind(')') - 5);
    }
    auto vc = getVariableColumns().find(var);
    if (vc != getVariableColumns().end()) {
      validIndices.push_back(pair<size_t, ResultTable::ResultType>(
          vc->second, res->getResultType(vc->second)));
    }
  }
  if (validIndices.size() == 0) {
    return nlohmann::json(std::vector<std::string>());
  }

  const IdTable& data = res->_data;
  return writeJsonTable(data, offset, limit, validIndices);
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getCostEstimate() {
  if (_cachedResult && _cachedResult->status() == ResultTable::FINISHED) {
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
    if (_cachedResult && _cachedResult->status() == ResultTable::FINISHED) {
      _sizeEstimate = _cachedResult->size();
    } else if (_qec) {
      _sizeEstimate = _rootOperation->getSizeEstimate();
    } else {
      // For test cases without index only:
      // Make it deterministic by using the asString.
      _sizeEstimate =
          1000 + std::hash<string>{}(_rootOperation->asString()) % 1000;
    }
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
bool QueryExecutionTree::knownEmptyResult() {
  if (_cachedResult && _cachedResult->status() == ResultTable::FINISHED) {
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
  std::shared_ptr<const CacheValue> res = cache[asString()];
  if (res) {
    _cachedResult = cache[asString()]->_resTable;
  }
}

// __________________________________________________________________________________________________________
nlohmann::json QueryExecutionTree::writeJsonTable(
    const IdTable& data, size_t from, size_t limit,
    const vector<pair<size_t, ResultTable::ResultType>>& validIndices) const {
  shared_ptr<const ResultTable> res = getResult();
  nlohmann::json json{std::vector<std::string>()};
  auto optToJson = [](const auto& opt) -> nlohmann::json {
    if (opt) {
      return opt.value();
    }
    return nullptr;
  };

  const auto upperBound = std::min(data.size(), limit + from);
  for (size_t i = from; i < upperBound; ++i) {
    json.emplace_back();
    auto& row = json.back();
    for (const auto& idx : validIndices) {
      const auto& currentId = data(i, idx.first);
      switch (idx.second) {
        case ResultTable::ResultType::KB: {
          std::optional<string> entity =
              _qec->getIndex().idToOptionalString(currentId);
          if (entity) {
            string entitystr = entity.value();
            if (ad_utility::startsWith(entitystr, VALUE_PREFIX)) {
              entity = ad_utility::convertIndexWordToValueLiteral(entitystr);
            }
          }
          row.emplace_back(optToJson(entity));
          break;
        }
        case ResultTable::ResultType::VERBATIM:
          row.emplace_back(std::to_string(currentId));
          break;
        case ResultTable::ResultType::TEXT:
          row.emplace_back(_qec->getIndex().getTextExcerpt(currentId));
          break;
        case ResultTable::ResultType::FLOAT: {
          float f;
          std::memcpy(&f, &currentId, sizeof(float));
          row.push_back(std::to_string(f));
          break;
        }
        case ResultTable::ResultType::LOCAL_VOCAB: {
          std::optional<string> entity = res->idToOptionalString(currentId);
          row.emplace_back(optToJson(entity));
          break;
        }
        default:
          AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                   "Cannot deduce output type.");
      }
    }
  }
  return json;
}

// _________________________________________________________________________________________________________
void QueryExecutionTree::writeTable(
    const IdTable& data, char sep, size_t from, size_t upperBound,
    const vector<pair<size_t, ResultTable::ResultType>>& validIndices,
    std::ostream& out) const {
  shared_ptr<const ResultTable> res = getResult();
  for (size_t i = from; i < upperBound; ++i) {
    for (size_t j = 0; j < validIndices.size(); ++j) {
      switch (validIndices[j].second) {
        case ResultTable::ResultType::KB: {
          string entity =
              _qec->getIndex()
                  .idToOptionalString(data(i, validIndices[j].first))
                  .value_or("");
          if (ad_utility::startsWith(entity, VALUE_PREFIX)) {
            out << ad_utility::convertIndexWordToValueLiteral(entity);
          } else {
            out << entity;
          }
          break;
        }
        case ResultTable::ResultType::VERBATIM:
          out << data(i, validIndices[j].first);
          break;
        case ResultTable::ResultType::TEXT:
          out << _qec->getIndex().getTextExcerpt(
              data(i, validIndices[j].first));
          break;
        case ResultTable::ResultType::FLOAT: {
          float f;
          std::memcpy(&f, &data(i, validIndices[j].first), sizeof(float));
          out << f;
          break;
        }
        case ResultTable::ResultType::LOCAL_VOCAB: {
          out << res->idToOptionalString(data(i, validIndices[j].first))
                     .value_or("");
          break;
        }
        default:
          AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                   "Cannot deduce output type.");
      }
      out << (j + 1 < validIndices.size() ? sep : '\n');
    }
  }
}
