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
    : Operation(qec), parsedValues_(std::move(parsedValues)) {
  AD_CONTRACT_CHECK(
      std::ranges::all_of(parsedValues_._values, [&](const auto& row) {
        return row.size() == parsedValues_._variables.size();
      }));
}

// ____________________________________________________________________________
string Values::getCacheKeyImpl() const {
  return absl::StrCat("VALUES (", parsedValues_.variablesToString(), ") { ",
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
vector<ColumnIndex> Values::resultSortedOn() const { return {}; }

// ____________________________________________________________________________
VariableToColumnMap Values::computeVariableToColumnMap() const {
  // we use `unsigned char` instead of `bool` because of the strange
  // specialization of `std::vector<bool>`
  std::vector<unsigned char> colContainsUndef(parsedValues_._variables.size(),
                                              char{0});
  for (const auto& row : parsedValues_._values) {
    for (size_t i = 0; i < row.size(); ++i) {
      unsigned char& isUndef = colContainsUndef.at(i);
      isUndef = static_cast<bool>(isUndef) || row[i].isUndef();
    }
  }
  VariableToColumnMap map;
  for (size_t i = 0; i < parsedValues_._variables.size(); i++) {
    using enum ColumnIndexAndTypeInfo::UndefStatus;
    auto undefStatus = static_cast<bool>(colContainsUndef.at(i))
                           ? PossiblyUndefined
                           : AlwaysDefined;
    map[parsedValues_._variables[i]] = ColumnIndexAndTypeInfo{i, undefStatus};
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
uint64_t Values::getSizeEstimateBeforeLimit() {
  return parsedValues_._values.size();
}

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
    multiplicities_[col] =
        static_cast<float>(numValues) / static_cast<float>(numDistinctValues);
  }
}

// ____________________________________________________________________________
ProtoResult Values::computeResult([[maybe_unused]] bool requestLaziness) {
  // Set basic properties of the result table.
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());

  LocalVocab localVocab{};

  // Fill the result table using the `writeValues` method below.
  size_t resWidth = getResultWidth();
  CALL_FIXED_SIZE(resWidth, &Values::writeValues, this, &idTable, &localVocab);
  return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
}

// ____________________________________________________________________________
template <size_t I>
void Values::writeValues(IdTable* idTablePtr, LocalVocab* localVocab) {
  IdTableStatic<I> idTable = std::move(*idTablePtr).toStatic<I>();
  idTable.resize(parsedValues_._values.size());
  size_t rowIdx = 0;
  std::vector<size_t> numLocalVocabPerColumn(idTable.numColumns());
  for (auto& row : parsedValues_._values) {
    for (size_t colIdx = 0; colIdx < idTable.numColumns(); colIdx++) {
      const TripleComponent& tc = row[colIdx];
      // TODO<joka921> We don't want to move, but also don't want to
      // unconditionally copy.
      Id id = TripleComponent{tc}.toValueId(getIndex().getVocab(), *localVocab);
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
  *idTablePtr = std::move(idTable).toDynamic();
}
