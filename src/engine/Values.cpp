// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "Values.h"

#include <sstream>
#include "../util/Exception.h"
#include "../util/HashSet.h"
#include "CallFixedSize.h"

Values::Values(QueryExecutionContext* qec, const SparqlValues& values)
    : Operation(qec), _values(values) {}

string Values::asString(size_t indent) const {
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
    const vector<string>& v = _values._values[i];
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
  return os.str();
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
    const vector<string>& v = _values._values[i];
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
  return os.str();
}

size_t Values::getResultWidth() const { return _values._variables.size(); }

vector<size_t> Values::resultSortedOn() const { return {}; }

ad_utility::HashMap<string, size_t> Values::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> map;
  for (size_t i = 0; i < _values._variables.size(); i++) {
    map[_values._variables[i]] = i;
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
  ad_utility::HashSet<string> values;
  for (size_t col = 0; col < _values._variables.size(); col++) {
    values.clear();
    size_t count = 0;
    size_t distinct = 0;
    for (size_t j = 0; j < _values._values.size(); j++) {
      const std::string& v = _values._values[j][col];
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
  result->_data.setCols(getResultWidth());
  result->_resultTypes.resize(_values._variables.size(),
                              ResultTable::ResultType::KB);

  size_t resWidth = getResultWidth();
  CALL_FIXED_SIZE_1(resWidth, writeValues, &result->_data, index, _values);
}

template <size_t I>
void Values::writeValues(IdTable* res, const Index& index,
                         const SparqlValues& values) {
  IdTableStatic<I> result = res->moveToStatic<I>();
  result.resize(values._values.size());
  size_t numActuallyWritten = 0;
  size_t numSkipped = 0;
  for (const auto& row : values._values) {
    for (size_t colIdx = 0; colIdx < result.cols(); colIdx++) {
      size_t id;
      if (!index.getVocab().getId(row[colIdx], &id)) {
        getWarnings().push_back("The word " + row[colIdx] +
                                " is not part of the vocabulary.");
        numSkipped++;
        goto skipRow;
      }
      result(numActuallyWritten, colIdx) = id;
    }
    numActuallyWritten++;
  skipRow:;
  }
  AD_CHECK(numActuallyWritten + numSkipped == values._values.size());
  if (numSkipped) {
    getWarnings().push_back("Ignored " + std::to_string(numSkipped) +
                            " rows of a value clause because they contained "
                            "entries that are not part of the vocabulary");
  }
  result.resize(numActuallyWritten);
  *res = result.moveToDynamic();
}
