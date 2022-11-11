// Copyright 2019 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Florian Kramer <kramerf@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "Values.h"

#include <absl/strings/str_join.h>

#include <sstream>

#include "engine/CallFixedSize.h"
#include "util/Exception.h"
#include "util/HashSet.h"

Values::Values(QueryExecutionContext* qec, SparqlValues values)
    : Operation(qec) {
  _values = sanitizeValues(std::move(values));
}

string Values::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "VALUES (";
  for (size_t i = 0; i < _values._variables.size(); i++) {
    os << _values._variables[i];
    if (i + 1 < _values._variables.size()) {
      os << " ";
    }
  }
  os << ") {";
  for (size_t i = 0; i < _values._values.size(); i++) {
    const vector<TripleComponent>& v = _values._values[i];
    os << "(";
    for (size_t j = 0; j < v.size(); j++) {
      os << v[j];
      if (j + 1 < v.size()) {
        os << " ";
      }
    }
    os << ")";
    if (i + 1 < _values._variables.size()) {
      os << " ";
    }
  }
  os << "}";
  return std::move(os).str();
}

string Values::getDescriptor() const {
  std::ostringstream os;
  os << "Values with variables ";
  for (size_t i = 0; i < _values._variables.size(); i++) {
    os << _values._variables[i];
    if (i + 1 < _values._variables.size()) {
      os << " ";
    }
  }
  os << " and values ";
  for (size_t i = 0; i < _values._values.size(); i++) {
    const vector<TripleComponent>& v = _values._values[i];
    os << "(";
    for (size_t j = 0; j < v.size(); j++) {
      os << v[j];
      if (j + 1 < v.size()) {
        os << " ";
      }
    }
    os << ")";
    if (i + 1 < _values._variables.size()) {
      os << " ";
    }
  }
  return std::move(os).str();
}

size_t Values::getResultWidth() const { return _values._variables.size(); }

vector<size_t> Values::resultSortedOn() const { return {}; }

VariableToColumnMap Values::computeVariableToColumnMap() const {
  VariableToColumnMap map;
  for (size_t i = 0; i < _values._variables.size(); i++) {
    // TODO<joka921> Make the `_variables` also `Variable`s
    map[Variable{_values._variables[i]}] = i;
  }
  return map;
}

float Values::getMultiplicity(size_t col) {
  if (_multiplicities.empty()) {
    computeMultiplicities();
  }
  if (col < _multiplicities.size()) {
    return _multiplicities[col];
  }
  return 1;
}

size_t Values::getSizeEstimate() { return _values._values.size(); }

size_t Values::getCostEstimate() { return _values._values.size(); }

void Values::computeMultiplicities() {
  if (_values._variables.empty()) {
    // If the result is empty we still add a column to the multiplicities to
    // mark them as computed.
    _multiplicities.resize(1, 1);
    return;
  }
  _multiplicities.resize(_values._variables.size());
  ad_utility::HashSet<TripleComponent> values;
  for (size_t col = 0; col < _values._variables.size(); col++) {
    values.clear();
    size_t count = 0;
    size_t distinct = 0;
    for (size_t j = 0; j < _values._values.size(); j++) {
      const TripleComponent& v = _values._values[j][col];
      count++;
      if (values.count(v) == 0) {
        distinct++;
        values.insert(v);
      }
    }
    _multiplicities[col] = double(count) / distinct;
  }
}

void Values::computeResult(ResultTable* result) {
  const Index& index = getIndex();

  result->_sortedBy = resultSortedOn();
  result->_idTable.setCols(getResultWidth());
  result->_resultTypes.resize(_values._variables.size(),
                              ResultTable::ResultType::KB);

  size_t resWidth = getResultWidth();
  CALL_FIXED_SIZE_1(resWidth, writeValues, &result->_idTable, index, _values,
                    result->_localVocab);
}

auto Values::sanitizeValues(SparqlValues&& values) -> SparqlValues {
  std::vector<std::pair<std::size_t, std::size_t>> variableBound;
  for (size_t i = 0; i < values._variables.size(); ++i) {
    variableBound.emplace_back(i, 0);  // variable i is unbound so far;
  }
  std::vector<std::size_t> emptyValues;
  for (std::size_t i = 0; i < values._values.size(); ++i) {
    AD_CHECK(values._values[i].size() == values._variables.size());
    auto& v = values._values[i];
    if (std::all_of(v.begin(), v.end(),
                    [](const auto& s) { return s == "UNDEF"; })) {
      emptyValues.push_back(i);
    }
    for (std::size_t k = 0; k < v.size(); ++k) {
      if (v[k] != "UNDEF") {
        variableBound[k].second = 1;
      }
    }
  }
  // erase all the values where every variable is "UNDEF"
  for (auto it = emptyValues.rbegin(); it != emptyValues.rend(); ++it) {
    getWarnings().push_back(
        "Found a value line, where no variable is bound, ignoring it");
    values._values.erase(values._values.begin() + *it);
  }

  // completely erase all the variables which are never bound.
  for (auto it = variableBound.rbegin(); it != variableBound.rend(); ++it) {
    if (it->second) {
      continue;
    }
    getWarnings().push_back(
        "Found a VALUES variable , which is never bound in the value clause "
        ",ignoring it");
    values._variables.erase(values._variables.begin() + it->first);
    for (auto& val : values._values) {
      val.erase(val.begin() + it->first);
    }
  }
  if (values._values.empty() || values._variables.empty()) {
    AD_THROW(
        ad_semsearch::Exception::BAD_INPUT,
        "After removing undefined values and variables, a VALUES clause was "
        "found to be empty. This"
        "(the neutral element for JOIN) is currently not supported by QLever");
  }
  return std::move(values);
}

template <size_t I>
void Values::writeValues(IdTable* res, const Index& index,
                         const SparqlValues& values,
                         std::shared_ptr<LocalVocab> localVocab) {
  IdTableStatic<I> result = res->moveToStatic<I>();
  result.resize(values._values.size());
  size_t numRows = 0;
  std::vector<size_t> numLocalVocabPerColumn(result.cols());
  for (const auto& row : values._values) {
    for (size_t colIdx = 0; colIdx < result.cols(); colIdx++) {
      const TripleComponent& tc = row[colIdx];
      std::optional<Id> id = tc.toValueId(index.getVocab());
      if (!id) {
        AD_CHECK(tc.isString());
        auto& newWord = tc.getString();
        ++numLocalVocabPerColumn[colIdx];
        id = Id::makeFromLocalVocabIndex(
            localVocab->getIndexAndAddIfNotContained(std::move(newWord)));
      }
      result(numRows, colIdx) = id.value();
    }
    numRows++;
  }
  AD_CHECK(numRows == values._values.size());
  LOG(INFO) << "Total number of rows in VALUES clause: " << numRows
            << std::endl;
  LOG(DEBUG) << "Number of entries in local vocabulary per column: "
             << absl::StrJoin(numLocalVocabPerColumn, ", ") << std::endl;
  result.resize(numRows);
  *res = result.moveToDynamic();
}
