// Copyright 2019 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Florian Kramer <kramerf@cs.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "Values.h"

#include <sstream>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "engine/CallFixedSize.h"
#include "util/Exception.h"
#include "util/HashSet.h"

// ____________________________________________________________________________
Values::Values(QueryExecutionContext* qec, SparqlValues values)
    : Operation(qec) {
  _values = sanitizeValues(std::move(values));
}

// ____________________________________________________________________________
string Values::asStringImpl(size_t indent) const {
  return absl::StrCat(std::string(indent, ' '), "VALUES (",
                      _values.variablesToString(), ") { ",
                      _values.valuesToString(), " }");
}

// ____________________________________________________________________________
string Values::getDescriptor() const {
  return absl::StrCat("Values with variables ", _values.variablesToString());
}

// ____________________________________________________________________________
size_t Values::getResultWidth() const { return _values._variables.size(); }

// ____________________________________________________________________________
vector<size_t> Values::resultSortedOn() const { return {}; }

// ____________________________________________________________________________
VariableToColumnMap Values::computeVariableToColumnMap() const {
  VariableToColumnMap map;
  for (size_t i = 0; i < _values._variables.size(); i++) {
    // TODO<joka921> Make the `_variables` also `Variable`s
    map[Variable{_values._variables[i]}] = i;
  }
  return map;
}

// ____________________________________________________________________________
float Values::getMultiplicity(size_t col) {
  if (_multiplicities.empty()) {
    computeMultiplicities();
  }
  if (col < _multiplicities.size()) {
    return _multiplicities[col];
  }
  return 1;
}

// ____________________________________________________________________________
size_t Values::getSizeEstimate() { return _values._values.size(); }

// ____________________________________________________________________________
size_t Values::getCostEstimate() { return _values._values.size(); }

// ____________________________________________________________________________
void Values::computeMultiplicities() {
  if (_values._variables.empty()) {
    // If the result is empty we still add a column to the multiplicities to
    // mark them as computed.
    _multiplicities.resize(1, 1);
    return;
  }
  _multiplicities.resize(_values._variables.size());
  ad_utility::HashSet<TripleComponent> distinctValues;
  size_t numValues = _values._values.size();
  for (size_t col = 0; col < _values._variables.size(); col++) {
    distinctValues.clear();
    for (const auto& valuesTuple : _values._values) {
      distinctValues.insert(valuesTuple[col]);
    }
    size_t numDistinctValues = distinctValues.size();
    _multiplicities[col] = float(numValues) / float(numDistinctValues);
  }
}

// ____________________________________________________________________________
void Values::computeResult(ResultTable* result) {
  const Index& index = getIndex();

  result->_sortedBy = resultSortedOn();
  result->_idTable.setNumColumns(getResultWidth());
  result->_resultTypes.resize(_values._variables.size(),
                              ResultTable::ResultType::KB);

  size_t resWidth = getResultWidth();
  CALL_FIXED_SIZE(resWidth, &Values::writeValues, this, &result->_idTable,
                  index, _values, result->_localVocab);
}

// ____________________________________________________________________________
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

// ____________________________________________________________________________
template <size_t I>
void Values::writeValues(IdTable* res, const Index& index,
                         const SparqlValues& values,
                         std::shared_ptr<LocalVocab> localVocab) {
  IdTableStatic<I> result = std::move(*res).toStatic<I>();
  result.resize(values._values.size());
  size_t numTuples = 0;
  std::vector<size_t> numLocalVocabPerColumn(result.numColumns());
  for (const auto& row : values._values) {
    for (size_t colIdx = 0; colIdx < result.numColumns(); colIdx++) {
      const TripleComponent& tc = row[colIdx];
      std::optional<Id> id = tc.toValueId(index.getVocab());
      if (!id) {
        AD_CHECK(tc.isString());
        auto& newWord = tc.getString();
        ++numLocalVocabPerColumn[colIdx];
        id = Id::makeFromLocalVocabIndex(
            localVocab->getIndexAndAddIfNotContained(std::move(newWord)));
      }
      result(numTuples, colIdx) = id.value();
    }
    numTuples++;
  }
  AD_CHECK(numTuples == values._values.size());
  LOG(INFO) << "Number of tuples in VALUES clause: " << numTuples << std::endl;
  LOG(DEBUG) << "Number of entries in local vocabulary per column: "
             << absl::StrJoin(numLocalVocabPerColumn, ", ") << std::endl;
  result.resize(numTuples);
  *res = std::move(result).toDynamic();
}
