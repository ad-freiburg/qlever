// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "engine/BinaryExport.h"

#include <array>
#include <cstring>
#include <utility>

#include "backports/algorithm.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/Result.h"
#include "engine/StringMapping.h"
#include "util/Algorithm.h"
#include "util/Generator.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FromCallableSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"

using CancellationHandle = ad_utility::SharedCancellationHandle;
namespace {
// Return a `std::string_view` wrapping the passed value.
std::string_view raw(const std::integral auto& value) {
  return std::string_view{reinterpret_cast<const char*>(&value), sizeof(value)};
}
}  // namespace

namespace qlever::binary_export {

// Convert `originalId`, which might contain references into this process'
// memory space to an id that completely inlines a value, or one that only has
// a reference to the passed `stringMapping`.
AD_ALWAYS_INLINE static Id toExportableId(Id originalId,
                                          StringMapping& stringMapping) {
  if (originalId.isTrivial() ||
      originalId.getDatatype() == Datatype::BlankNodeIndex ||
      originalId.getDatatype() == Datatype::EncodedVal) {
    return originalId;
  } else {
    return stringMapping.remapId(originalId);
  }
}

// The format transfers the bits of `Id`s directly, so a change of their
// representation requires increasing `ad_utility::binaryQleverExportVersion`.
// The following assertions only detect a change of the datatype tags.
static_assert(ad_utility::binaryQleverExportVersion == 1);
static_assert(Id::numDatatypeBits == 4);
static constexpr auto datatypesOfVersion1 = []() {
  using enum Datatype;
  return std::to_array({Undefined, Bool, Int, Double, VocabIndex,
                        LocalVocabIndex, SecondaryVocabIndex, TextRecordIndex,
                        Date, GeoPoint, WordVocabIndex, BlankNodeIndex,
                        EncodedVal});
}();
static_assert(isListOfAllDatatypesInOrder(datatypesOfVersion1));

static constexpr std::string_view magicBytes = "QLEVER.EXPORT";

// The header of the format. Its serialization starts with the `magicBytes` and
// the version, such that the reader can check the format and its version.
struct Header {
  // The encoding of the `GeoPoint`s of the exporting instance, see
  // `GeoPoint::encoding()`.
  GeoPointEncodingEnum geoPointEncoding_ = GeoPointEncodingEnum::ZOrder;
  // The patterns of the `EncodedIriManager` of the exporting instance, which
  // are transferred as JSON.
  std::vector<encodedIri::Pattern> patterns_;
  // The names of the variables, one per column.
  std::vector<std::string> variableNames_;
  // The names of the selected variables that are not bound in the result, and
  // therefore have no column. The reader could treat every missing variable
  // as unbound without them, but with them it can check that the result
  // contains exactly the variables it expects.
  std::vector<std::string> unboundVariableNames_;

  // Return the serialized header.
  std::string serialize() const {
    ad_utility::serialization::ByteBufferWriteSerializer serializer;
    nlohmann::json patterns;
    detail::patternsToJson(patterns, patterns_,
                           EncodedIriManager::NumBitsEncoding);
    serializer << std::string{magicBytes};
    serializer << ad_utility::binaryQleverExportVersion;
    serializer << geoPointEncoding_;
    serializer << patterns.dump();
    serializer << variableNames_;
    serializer << unboundVariableNames_;
    return std::string{serializer.data().data(), serializer.data().size()};
  }
};

// The marker that precedes each batch of strings (see `StringMapping`) in the
// stream of IDs. It is an undefined `Id` with additional bits set, which never
// occurs as an actual value (all undefined values are `Id::makeUndefined()`).
// The reader reads the first `Id` of each row and compares it to this marker
// to decide whether a row or a batch of strings follows. This is subtle: it
// only works because the marker is never the first `Id` of an actual row.
static constexpr Id::T vocabMarker = Id::makeUndefined().getBits() + 0b10101010;
static_assert(Id::fromBits(vocabMarker).getDatatype() == Datatype::Undefined);

// _____________________________________________________________________________
void ExportBatch::addRow(const IdTableView<0>& idTable, size_t row,
                         ql::span<const ColumnIndex> columns) {
  size_t offset = buffer_.size();
  buffer_.resize(offset + columns.size() * sizeof(Id::T));
  char* target = buffer_.data() + offset;
  for (ColumnIndex column : columns) {
    Id::T bits = toExportableId(idTable(row, column), stringMapping_).getBits();
    std::memcpy(target, &bits, sizeof(bits));
    target += sizeof(bits);
  }
  ++numRows_;
}

// _____________________________________________________________________________
bool ExportBatch::shouldFinish() const {
  return buffer_.size() >= sizes_.minNumIdBytes_ ||
         stringMapping_.numBytes() >= sizes_.minNumStringBytes_;
}

// _____________________________________________________________________________
std::string ExportBatch::finish() {
  auto strings = stringMapping_.takeStrings();
  // Grow the `buffer_` at most once for the marker and the strings.
  buffer_.reserve(buffer_.size() + sizeof(vocabMarker) +
                  BinaryExportHelpers::serializedSize(strings));
  // The marker tells the reader that the strings follow, see `vocabMarker`.
  buffer_.append(raw(vocabMarker));
  BinaryExportHelpers::appendVectorOfStrings(buffer_, strings);
  numRows_ = 0;
  return std::exchange(buffer_, std::string{});
}

// _____________________________________________________________________________
size_t BinaryExportHelpers::serializedSize(
    const std::vector<std::string>& strings) {
  // The serialization of a vector or a string consists of its size followed
  // by its elements.
  return sizeof(size_t) +
         ::ranges::accumulate(strings, size_t{0}, {}, [](const auto& string) {
           return sizeof(size_t) + string.size();
         });
}

// _____________________________________________________________________________
void BinaryExportHelpers::appendVectorOfStrings(
    std::string& target, const std::vector<std::string>& strings) {
  [[maybe_unused]] size_t sizeBefore = target.size();
  auto write = [&target](const char* src, size_t numBytes) {
    target.append(src, numBytes);
  };
  ad_utility::serialization::WriteViaCallableSerializer writer{write};
  writer << strings;
  AD_EXPENSIVE_CHECK(target.size() - sizeBefore == serializedSize(strings));
}

// The special case of a result without columns, for which only the number of
// rows is exported.
static std::string exportNumRows(LimitOffsetClause limitAndOffset,
                                 const Result& result,
                                 const CancellationHandle& cancellationHandle) {
  uint64_t numRows = 0;
  uint64_t resultSize = 0;
  for (const auto& [pair, range] : ExportQueryExecutionTrees::getRowIndices(
           limitAndOffset, result, resultSize)) {
    numRows += range.size();
    cancellationHandle->throwIfCancelled();
  }
  return std::string{raw(numRows)};
}

// Export the rows of the `result` (only the `columns`) in batches, see
// `ExportBatch`. Each batch is yielded at once.
static cppcoro::generator<std::string> exportRows(
    LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
    std::vector<ColumnIndex> columns, const Index& index,
    CancellationHandle cancellationHandle) {
  // NOTE: The `batch` resolves the strings of the IDs immediately, so the
  // local vocabs of the tables of the `result` don't have to be kept alive.
  ExportBatch batch{index};
  uint64_t resultSize = 0;
  for (const auto& [pair, range] : ExportQueryExecutionTrees::getRowIndices(
           limitAndOffset, *result, resultSize)) {
    for (uint64_t i : range) {
      batch.addRow(pair.idTable_, i, columns);
      if (batch.shouldFinish()) {
        co_yield batch.finish();
      }
      cancellationHandle->throwIfCancelled();
    }
  }
  // Always send the trailing vocab so the importer can finalize the last batch.
  co_yield batch.finish();
}

// _____________________________________________________________________________
ad_utility::streams::stream_generator exportAsQLeverBinary(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle) {
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();
  AD_LOG_DEBUG << "Starting binary export..." << std::endl;

  // Only the variables that are bound in the result get a column.
  QueryExecutionTree::ColumnIndicesAndTypes columns =
      qet.selectedVariablesToColumnIndices(selectClause, true);
  Header header{GeoPoint::encoding(),
                qet.getQec()->getIndex().encodedIriManager().patterns_,
                {},
                {}};
  for (const auto& [variable, column] : ::ranges::views::zip(
           selectClause.getSelectedVariablesAsStrings(), columns)) {
    (column.has_value() ? header.variableNames_ : header.unboundVariableNames_)
        .push_back(variable);
  }
  std::erase(columns, std::nullopt);

  co_yield header.serialize();

  if (columns.empty()) {
    co_yield exportNumRows(limitAndOffset, *result, cancellationHandle);
    co_return;
  }

  for (const std::string& batch :
       exportRows(limitAndOffset, std::move(result),
                  ad_utility::transform(columns,
                                        [](const auto& column) -> ColumnIndex {
                                          return column->columnIndex_;
                                        }),
                  qet.getQec()->getIndex(), std::move(cancellationHandle))) {
    co_yield batch;
  }
}

}  // namespace qlever::binary_export

#endif
