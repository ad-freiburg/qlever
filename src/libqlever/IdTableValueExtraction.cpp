// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "libqlever/IdTableValueExtraction.h"

#include <charconv>
#include <cmath>
#include <limits>

#include "index/ExportIds.h"

namespace qlever {

// Convert an Id to an integer if it stores a numeric type
std::optional<int64_t> getInt(Id id) {
  [[maybe_unused]] static constexpr auto Undefined = Datatype::Undefined;
  [[maybe_unused]] static constexpr auto Bool = Datatype::Bool;
  [[maybe_unused]] static constexpr auto Int = Datatype::Int;
  [[maybe_unused]] static constexpr auto Double = Datatype::Double;
  [[maybe_unused]] static constexpr auto VocabIndex = Datatype::VocabIndex;
  [[maybe_unused]] static constexpr auto LocalVocabIndex =
      Datatype::LocalVocabIndex;
  [[maybe_unused]] static constexpr auto TextRecordIndex =
      Datatype::TextRecordIndex;
  [[maybe_unused]] static constexpr auto Date = Datatype::Date;
  [[maybe_unused]] static constexpr auto GeoPoint = Datatype::GeoPoint;
  [[maybe_unused]] static constexpr auto WordVocabIndex =
      Datatype::WordVocabIndex;
  [[maybe_unused]] static constexpr auto BlankNodeIndex =
      Datatype::BlankNodeIndex;
  [[maybe_unused]] static constexpr auto EncodedVal = Datatype::EncodedVal;
  [[maybe_unused]] static constexpr auto MaxValue = Datatype::MaxValue;
  switch (id.getDatatype()) {
    case Int:
      return id.getInt();
    case Double: {
      double d = id.getDouble();
      if (std::isfinite(d) && d >= std::numeric_limits<int64_t>::min() &&
          d <= std::numeric_limits<int64_t>::max()) {
        return static_cast<int64_t>(d);
      }
      return std::nullopt;
    }
    default:
      return std::nullopt;
  }
}

// Convert an Id to a double if it stores a numeric type
std::optional<double> getDouble(Id id) {
  [[maybe_unused]] static constexpr auto Undefined = Datatype::Undefined;
  [[maybe_unused]] static constexpr auto Bool = Datatype::Bool;
  [[maybe_unused]] static constexpr auto Int = Datatype::Int;
  [[maybe_unused]] static constexpr auto Double = Datatype::Double;
  [[maybe_unused]] static constexpr auto VocabIndex = Datatype::VocabIndex;
  [[maybe_unused]] static constexpr auto LocalVocabIndex =
      Datatype::LocalVocabIndex;
  [[maybe_unused]] static constexpr auto TextRecordIndex =
      Datatype::TextRecordIndex;
  [[maybe_unused]] static constexpr auto Date = Datatype::Date;
  [[maybe_unused]] static constexpr auto GeoPoint = Datatype::GeoPoint;
  [[maybe_unused]] static constexpr auto WordVocabIndex =
      Datatype::WordVocabIndex;
  [[maybe_unused]] static constexpr auto BlankNodeIndex =
      Datatype::BlankNodeIndex;
  [[maybe_unused]] static constexpr auto EncodedVal = Datatype::EncodedVal;
  [[maybe_unused]] static constexpr auto MaxValue = Datatype::MaxValue;
  switch (id.getDatatype()) {
    case Double:
      return id.getDouble();
    case Int:
      return static_cast<double>(id.getInt());
    default:
      return std::nullopt;
  }
}

// Convert an Id to a boolean if it stores a boolean type
std::optional<bool> getBool(Id id) {
  [[maybe_unused]] static constexpr auto Undefined = Datatype::Undefined;
  [[maybe_unused]] static constexpr auto Bool = Datatype::Bool;
  [[maybe_unused]] static constexpr auto Int = Datatype::Int;
  [[maybe_unused]] static constexpr auto Double = Datatype::Double;
  [[maybe_unused]] static constexpr auto VocabIndex = Datatype::VocabIndex;
  [[maybe_unused]] static constexpr auto LocalVocabIndex =
      Datatype::LocalVocabIndex;
  [[maybe_unused]] static constexpr auto TextRecordIndex =
      Datatype::TextRecordIndex;
  [[maybe_unused]] static constexpr auto Date = Datatype::Date;
  [[maybe_unused]] static constexpr auto GeoPoint = Datatype::GeoPoint;
  [[maybe_unused]] static constexpr auto WordVocabIndex =
      Datatype::WordVocabIndex;
  [[maybe_unused]] static constexpr auto BlankNodeIndex =
      Datatype::BlankNodeIndex;
  [[maybe_unused]] static constexpr auto EncodedVal = Datatype::EncodedVal;
  [[maybe_unused]] static constexpr auto MaxValue = Datatype::MaxValue;
  if (id.getDatatype() == Bool) {
    return id.getBool();
  }
  return std::nullopt;
}

// Convert an Id to a string representation
std::optional<std::string> getString(Id id, const Index& index,
                                     const LocalVocab& localVocab) {
  auto optionalStringAndType =
      ql::exportIds::idToStringAndType(index, id, localVocab);
  if (!optionalStringAndType.has_value()) {
    return std::nullopt;
  }
  return std::move(optionalStringAndType->first);
}

std::optional<int64_t> getFeatureIdFromIri(Id id, const Index& index,
                                           const LocalVocab& localVocab) {
  auto type = id.getDatatype();
  // NOTE: The BMW demo used a dedicated `getNumberFromEncodedIri` for the
  // `EncodedVal` case, which no longer exists in the current qlever core.
  // Instead we decode all three cases to their IRI string via `getString`
  // (which itself handles `EncodedVal`) and parse the numeric suffix below.
  if (type != Datatype::EncodedVal && type != Datatype::VocabIndex &&
      type != Datatype::LocalVocabIndex) {
    return std::nullopt;
  }
  auto str = getString(id, index, localVocab);
  if (!str.has_value()) {
    return std::nullopt;
  }
  std::string_view s = str.value();
  auto pos = s.rfind('_');
  if (pos == s.npos) {
    return std::nullopt;
  }
  // NOTE: `pos + 1`, so that the `_` itself is dropped; otherwise `from_chars`
  // below fails on it and this function always returns `std::nullopt`.
  s.remove_prefix(pos + 1);
  int64_t res;
  auto err = std::from_chars(s.data(), s.data() + s.size(), res);
  if (err.ec == std::errc() && err.ptr == s.data() + s.size() - 1) {
    return res;
  } else {
    return std::nullopt;
  }
}

// Helper function to convert an Id to URI string
std::string idToUri(Id id, const Index& index, const LocalVocab& localVocab) {
  auto uriOpt = getString(id, index, localVocab);
  AD_CONTRACT_CHECK(uriOpt.has_value(), "Failed to convert Id to URI string");
  return std::move(uriOpt.value());
}

// Helper function to convert a URI string back to Id for a specific instance
Id uriToId(const std::string& uri, const Index& index) {
  // Try to get the ID from the vocabulary
  VocabIndex idx;
  bool found = index.getVocab().getId(uri, &idx);
  AD_CONTRACT_CHECK(found, "Failed to convert URI to Id: ", uri);
  return Id::makeFromVocabIndex(idx);
}

ColumnIndex getColumnIndex(const VariableToColumnMap& variableColumns,
                           const Variable& var) {
  auto it = variableColumns.find(var);
  AD_CONTRACT_CHECK(it != variableColumns.end(),
                    "Variable " + var.name() + " not found in result");
  return it->second.columnIndex_;
}

std::optional<ColumnIndex> getOptionalColumnIndex(
    const VariableToColumnMap& variableColumns, const Variable& var) {
  auto it = variableColumns.find(var);
  if (it != variableColumns.end()) {
    return it->second.columnIndex_;
  }
  return std::nullopt;
}

}  // namespace qlever
