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
Values::Values(QueryExecutionContext* qec, SparqlValues parsedValues)
    : Operation(qec) {
  parsedValues_ = sanitizeValues(std::move(parsedValues));
}

// ____________________________________________________________________________
string Values::asStringImpl(size_t indent) const {
  return absl::StrCat(std::string(indent, ' '), "VALUES (",
                      parsedValues_.variablesToString(), ") { ",
                      parsedValues_.valuesToString(), " }");
}

// ____________________________________________________________________________
string Values::getDescriptor() const {
  return absl::StrCat("Values with variables ",
                      parsedValues_.variablesToString());
}

// ____________________________________________________________________________
size_t Values::getResultWidth() const {
  return parsedValues_._variables.size();
}

// ____________________________________________________________________________
vector<size_t> Values::resultSortedOn() const { return {}; }

// ____________________________________________________________________________
VariableToColumnMap Values::computeVariableToColumnMap() const {
  VariableToColumnMap map;
  for (size_t i = 0; i < parsedValues_._variables.size(); i++) {
    // TODO<joka921> Make the `_variables` also `Variable`s
    map[Variable{parsedValues_._variables[i]}] = i;
  }
  return map;
}

// ____________________________________________________________________________
float Values::getMultiplicity(size_t col) {
  if (multiplicities_.empty()) {
    computeMultiplicities();
  }
  if (col < multiplicities_.size()) {
    return multiplicities_[col];
  }
  return 1;
}

// ____________________________________________________________________________
size_t Values::getSizeEstimate() { return parsedValues_._values.size(); }

// ____________________________________________________________________________
size_t Values::getCostEstimate() { return parsedValues_._values.size(); }

// ____________________________________________________________________________
void Values::computeMultiplicities() {
  if (parsedValues_._variables.empty()) {
    // If the result is empty we still add a column to the multiplicities to
    // mark them as computed.
    multiplicities_.resize(1, 1);
    return;
  }
  multiplicities_.resize(parsedValues_._variables.size());
  ad_utility::HashSet<TripleComponent> distinctValues;
  size_t numValues = parsedValues_._values.size();
  for (size_t col = 0; col < parsedValues_._variables.size(); col++) {
    distinctValues.clear();
    for (const auto& valuesTuple : parsedValues_._values) {
      distinctValues.insert(valuesTuple[col]);
    }
    size_t numDistinctValues = distinctValues.size();
    multiplicities_[col] = float(numValues) / float(numDistinctValues);
  }
}

// ____________________________________________________________________________
auto Values::sanitizeValues(SparqlValues&& values) -> SparqlValues {
  // Find all UNDEF values and remember tuples (rows) where all values are
  // UNDEF as well as for each variable when it had a value that was not UNDEF
  // at least once.
  std::vector<std::pair<std::size_t, std::size_t>> variableBound;
  for (size_t i = 0; i < values._variables.size(); ++i) {
    variableBound.emplace_back(i, 0);
  }
  std::vector<std::size_t> emptyValues;
  for (std::size_t i = 0; i < values._values.size(); ++i) {
    AD_CONTRACT_CHECK(values._values[i].size() == values._variables.size());
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

  // Remove all value tuples where every value is "UNDEF".
  for (auto it = emptyValues.rbegin(); it != emptyValues.rend(); ++it) {
    getWarnings().push_back("Removing VALUES tuple where all values are UNDEF");
    values._values.erase(values._values.begin() + *it);
  }

  // Remove all variables that are not bound in a single value tuple.
  for (auto it = variableBound.rbegin(); it != variableBound.rend(); ++it) {
    if (it->second) {
      continue;
    }
    getWarnings().push_back(
        "Removing VALUES variable for which all values were UNDEF");
    values._variables.erase(values._variables.begin() + it->first);
    for (auto& val : values._values) {
      val.erase(val.begin() + it->first);
    }
  }

  // If no variables or tuples remain, throw an exception.
  if (values._values.empty() || values._variables.empty()) {
    AD_THROW(
        "After removing all UNDEF values and variables, a VALUES clause was "
        "found to be empty. Such an operation result (the neutral element for "
        "JOIN) is currently not supported by QLever");
  }

  return std::move(values);
}

// ____________________________________________________________________________
void Values::computeResult(ResultTable* result) {
  // Set basic properties of the result table.
  result->_sortedBy = resultSortedOn();
  result->_idTable.setNumColumns(getResultWidth());
  result->_resultTypes.resize(parsedValues_._variables.size(),
                              ResultTable::ResultType::KB);

  // Fill the result table using the `writeValues` method below.
  size_t resWidth = getResultWidth();
  CALL_FIXED_SIZE(resWidth, &Values::writeValues, this, result);
}

// ____________________________________________________________________________
template <size_t I>
void Values::writeValues(ResultTable* result) {
  IdTableStatic<I> idTable = std::move(result->_idTable).toStatic<I>();
  idTable.resize(parsedValues_._values.size());
  size_t rowIdx = 0;
  std::vector<size_t> numLocalVocabPerColumn(idTable.numColumns());
  for (auto& row : parsedValues_._values) {
    for (size_t colIdx = 0; colIdx < idTable.numColumns(); colIdx++) {
      TripleComponent& tc = row[colIdx];
      Id id = std::move(tc).toValueId(getIndex().getVocab(),
                                      *result->getLocalVocab());
      idTable(rowIdx, colIdx) = id;
      if (id.getDatatype() == Datatype::LocalVocabIndex) {
        ++numLocalVocabPerColumn[colIdx];
      }
    }
    rowIdx++;
  }
  AD_CORRECTNESS_CHECK(rowIdx == parsedValues_._values.size());
  LOG(INFO) << "Number of tuples in VALUES clause: " << rowIdx << std::endl;
  LOG(INFO) << "Number of entries in local vocabulary per column: "
            << absl::StrJoin(numLocalVocabPerColumn, ", ") << std::endl;
  result->_idTable = std::move(idTable).toDynamic();
}
