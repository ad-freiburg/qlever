// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "engine/BinaryExport.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <cstring>
#include <utility>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/Result.h"
#include "engine/StringMapping.h"
#include "index/TripleComponentConversions.h"
#include "util/Algorithm.h"
#include "util/Generator.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/FromCallableSerializer.h"
#include "util/Serializer/SerializeOptional.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
// TODO<joka921> Can we get further type erasure here to not include the
// `HttpClient`.
#include "util/http/HttpClient.h"

using CancellationHandle = ad_utility::SharedCancellationHandle;
namespace {
// Return a `std::string_view` wrapping the passed value.
std::string_view raw(const std::integral auto& value) {
  return std::string_view{reinterpret_cast<const char*>(&value), sizeof(value)};
}
}  // namespace

namespace qlever::binary_export {

// _____________________________________________________________________________
AD_ALWAYS_INLINE Id toExportableId(Id originalId,
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
static_assert([]() {
  using enum Datatype;
  constexpr std::array datatypesOfVersion1{Undefined,
                                           Bool,
                                           Int,
                                           Double,
                                           VocabIndex,
                                           LocalVocabIndex,
                                           SecondaryVocabIndex,
                                           TextRecordIndex,
                                           Date,
                                           GeoPoint,
                                           WordVocabIndex,
                                           BlankNodeIndex,
                                           EncodedVal};
  if (datatypesOfVersion1.size() != static_cast<size_t>(MaxValue) + 1) {
    return false;
  }
  for (size_t i = 0; i < datatypesOfVersion1.size(); ++i) {
    if (static_cast<size_t>(datatypesOfVersion1[i]) != i) {
      return false;
    }
  }
  return true;
}());

static constexpr std::string_view magicBytes = "QLEVER.EXPORT";

// _____________________________________________________________________________
void BinaryExportHelpers::throwIfTooLarge(size_t size, size_t maxSize) {
  if (size > maxSize) {
    throw std::runtime_error{absl::StrCat(
        "The result in QLever's binary export format contains a string or a "
        "batch of strings that is larger than the maximum size of ",
        ad_utility::MemorySize::bytes(maxSize).asString())};
  }
}

// The header of the format. Its serialization starts with the `magicBytes` and
// the version, which are checked when reading it.
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
  // therefore have no column.
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

  // Read a header that was written by `serialize` from the `reader` (see
  // `BinaryExportHelpers::readString` for the `maxNumBytes`).
  template <typename Reader>
  static Header read(Reader& reader, size_t maxNumBytes) {
    using H = BinaryExportHelpers;
    // The magic bytes are serialized as a string. Its size is checked first,
    // such that no memory is allocated for the size of an invalid response.
    std::string magic(magicBytes.size(), '\0');
    if (H::read<size_t>(reader) == magic.size()) {
      reader(magic.data(), magic.size());
    }
    if (magic != magicBytes) {
      throw std::runtime_error{
          "The result is not in QLever's binary export format"};
    }
    auto version = H::read<uint16_t>(reader);
    if (version != ad_utility::binaryQleverExportVersion) {
      throw std::runtime_error{
          absl::StrCat("The result is in version ", version,
                       " of QLever's binary export format, but only version ",
                       ad_utility::binaryQleverExportVersion, " is supported")};
    }
    Header header;
    header.geoPointEncoding_ = H::read<GeoPointEncodingEnum>(reader);
    if (header.geoPointEncoding_ != GeoPointEncodingEnum::ZOrder &&
        header.geoPointEncoding_ != GeoPointEncodingEnum::LatMajor) {
      throw std::runtime_error{
          "The result in QLever's binary export format has an invalid "
          "encoding of its geographic points"};
    }
    header.patterns_ = detail::patternsFromJson(
        nlohmann::json::parse(H::readString(reader, maxNumBytes)),
        EncodedIriManager::NumBitsEncoding, EncodedIriManager::maxNumPrefixes_);
    header.variableNames_ = H::readVectorOfStrings(reader, maxNumBytes);
    header.unboundVariableNames_ = H::readVectorOfStrings(reader, maxNumBytes);
    return header;
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
  // The marker tells the reader that the strings follow, see `vocabMarker`.
  absl::StrAppend(
      &buffer_, raw(vocabMarker),
      BinaryExportHelpers::writeVectorOfStrings(stringMapping_.takeStrings()));
  numRows_ = 0;
  return std::exchange(buffer_, std::string{});
}

// The special case of a result without columns, for which only the number of
// rows is exported. `exportNumRows` and `importNumRows` are the two
// counterparts of this format.
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

// _____________________________________________________________________________
template <typename Reader>
static IdTable importNumRows(Reader& reader, size_t numColumns,
                             const QueryExecutionContext& qec) {
  auto numRows = BinaryExportHelpers::read<uint64_t>(reader);
  IdTable result{numColumns, qec.getAllocator()};
  result.resize(numRows);
  for (auto column : result.getColumns()) {
    ql::ranges::fill(column, Id::makeUndefined());
  }
  return result;
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
                  ad_utility::transform(
                      columns,
                      [](const auto& column) { return column->columnIndex_; }),
                  qet.getQec()->getIndex(), std::move(cancellationHandle))) {
    co_yield batch;
  }
}

// Convert the string representation of a literal or IRI to a local `Id` in
// the same way as for any other input (see `toValueId`): an IRI that can be
// encoded with a local pattern becomes an `EncodedVal`, a word of the
// vocabularies of the local index the corresponding `VocabIndex` or
// `SecondaryVocabIndex`, and only the remaining words are added to `vocab`.
static Id stringToId(std::string representation,
                     const QueryExecutionContext& qec, LocalVocab& vocab) {
  auto literalOrIri =
      ad_utility::triple_component::LiteralOrIri::fromStringRepresentation(
          std::move(representation));
  TripleComponent tripleComponent =
      literalOrIri.isIri()
          ? TripleComponent{std::move(literalOrIri.getIri())}
          : TripleComponent{std::move(literalOrIri.getLiteral())};
  return toValueId(std::move(tripleComponent), qec.getIndex().getImpl(), vocab);
}

// _____________________________________________________________________________
IdRewriter::IdRewriter(const QueryExecutionContext& qec,
                       std::vector<encodedIri::Pattern> remotePatterns,
                       GeoPointEncodingEnum remoteGeoPointEncoding)
    : qec_{&qec},
      remotePatterns_{std::move(remotePatterns)},
      prefixMapping_{getPrefixMapping(qec, remotePatterns_)},
      remoteGeoPointEncoding_{remoteGeoPointEncoding} {}

// _____________________________________________________________________________
void IdRewriter::startBatch(std::vector<std::string> transmittedStrings) {
  transmittedIds_.clear();
  transmittedIds_.reserve(transmittedStrings.size());
  for (auto& string : transmittedStrings) {
    transmittedIds_.push_back(
        stringToId(std::move(string), *qec_, localVocab_));
  }
  decodedIris_.clear();
}

// _____________________________________________________________________________
void IdRewriter::rewriteRows(IdTable& table, size_t firstRow) {
  for (auto column : table.getColumns()) {
    for (Id& id : column.subspan(firstRow)) {
      id = (*this)(id);
    }
  }
}

// _____________________________________________________________________________
Id IdRewriter::operator()(Id id) {
  switch (id.getDatatype()) {
    case Datatype::EncodedVal:
      return rewriteEncodedVal(id);
    case Datatype::BlankNodeIndex:
      return rewriteBlankNode(id);
    case Datatype::LocalVocabIndex:
      return rewriteString(id);
    case Datatype::GeoPoint:
      return rewriteGeoPoint(id);
    default:
      // All other IDs that the exporter sends are self-contained, see
      // `toExportableId`.
      if (!id.isTrivial()) {
        throw std::runtime_error{absl::StrCat(
            "The result in QLever's binary export format contains an ID of "
            "the unexpected datatype ",
            toString(id.getDatatype()))};
      }
      return id;
  }
}

// _____________________________________________________________________________
Id IdRewriter::rewriteEncodedVal(Id id) {
  auto [prefixIdx, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(id);
  if (auto it = prefixMapping_.find(prefixIdx); it != prefixMapping_.end()) {
    return EncodedIriManager::makeIdFromPrefixIdxAndPayload(it->second,
                                                            payload);
  }
  if (prefixIdx >= remotePatterns_.size()) {
    throw std::runtime_error{
        "The result in QLever's binary export format contains an encoded IRI "
        "with an unknown pattern"};
  }
  // The decoded IRI might be encoded with a different local pattern.
  auto [it, inserted] = decodedIris_.try_emplace(id.getBits(), Id{});
  if (inserted) {
    it->second =
        stringToId(encodedIri::decodeToIri(remotePatterns_[prefixIdx], payload),
                   *qec_, localVocab_);
  }
  return it->second;
}

// _____________________________________________________________________________
Id IdRewriter::rewriteBlankNode(Id id) {
  auto [it, inserted] = blankNodeMapping_.try_emplace(id.getBits(), Id{});
  if (inserted) {
    it->second = Id::makeFromBlankNodeIndex(blankNodeVocab_.getBlankNodeIndex(
        qec_->getIndex().getBlankNodeManager()));
  }
  return it->second;
}

// _____________________________________________________________________________
Id IdRewriter::rewriteString(Id id) const {
  // Undo the shift done by `StringMapping::remapId`.
  auto index =
      reinterpret_cast<size_t>(id.getLocalVocabIndex()) >> Id::numDatatypeBits;
  if (index >= transmittedIds_.size()) {
    throw std::runtime_error{
        "The result in QLever's binary export format refers to a string that "
        "was not transmitted"};
  }
  return transmittedIds_[index];
}

// _____________________________________________________________________________
Id IdRewriter::rewriteGeoPoint(Id id) const {
  if (remoteGeoPointEncoding_ == GeoPoint::encoding()) {
    return id;
  }
  static constexpr auto mask = ad_utility::bitMaskForLowerBits(Id::numDataBits);
  return Id::fromBits((id.getBits() & ~mask) |
                      GeoPoint::convertEncoding(id.getBits() & mask,
                                                remoteGeoPointEncoding_,
                                                GeoPoint::encoding()));
}

// _____________________________________________________________________________
LocalVocab IdRewriter::takeLocalVocab() {
  if (!blankNodeMapping_.empty()) {
    // NOTE: `mergeWith` marks the `blankNodeVocab_` as copied, which only
    // forbids adding words, but not blank nodes, to it.
    localVocab_.mergeWith(blankNodeVocab_);
  }
  // The IDs of the current batch might point into the old local vocab.
  transmittedIds_.clear();
  decodedIris_.clear();
  return std::exchange(localVocab_, LocalVocab{});
}

// _____________________________________________________________________________
ad_utility::HashMap<uint8_t, uint8_t> IdRewriter::getPrefixMapping(
    const QueryExecutionContext& qec,
    const std::vector<encodedIri::Pattern>& patterns) {
  ad_utility::HashMap<uint8_t, uint8_t> prefixMapping;
  const auto& localPatterns = qec.getIndex().encodedIriManager().patterns_;
  for (const auto& [index, pattern] : ::ranges::views::enumerate(patterns)) {
    auto patternIt = ql::ranges::find(localPatterns, pattern);
    if (patternIt != localPatterns.end()) {
      prefixMapping[index] = static_cast<uint8_t>(
          ql::ranges::distance(localPatterns.begin(), patternIt));
    }
  }
  return prefixMapping;
}

// For each of the `expectedVariables`, the index of the column of the
// response that contains it (`std::nullopt` if it is unbound). Throw if the
// response doesn't contain exactly the `expectedVariables`.
static std::vector<std::optional<size_t>> getRemoteColumns(
    const std::vector<std::string>& expectedVariables, const Header& header) {
  const auto& remoteVariables = header.variableNames_;
  auto allRemoteVariables = remoteVariables;
  ql::ranges::copy(header.unboundVariableNames_,
                   std::back_inserter(allRemoteVariables));
  using Set = ad_utility::HashSet<std::string>;
  if (Set{allRemoteVariables.begin(), allRemoteVariables.end()} !=
      Set{expectedVariables.begin(), expectedVariables.end()}) {
    throw std::runtime_error{absl::StrCat(
        "The result in QLever's binary export format contains the variables \"",
        absl::StrJoin(allRemoteVariables, " "), "\", but the variables \"",
        absl::StrJoin(expectedVariables, " "), "\" were expected")};
  }
  return ad_utility::transform(
      expectedVariables,
      [&remoteVariables](const auto& variable) -> std::optional<size_t> {
        auto it = ql::ranges::find(remoteVariables, variable);
        if (it == remoteVariables.end()) {
          return std::nullopt;
        }
        return static_cast<size_t>(it - remoteVariables.begin());
      });
}

// Read the remaining IDs of a row of the response (the `firstId` has already
// been read) from the `reader` into the `rowBuffer` (which has one
// entry per column of the response), and append the row to the `table`
// (which has one column per entry of `remoteColumns`, see `getRemoteColumns`).
template <typename Reader>
static void readRow(Reader& reader, Id::T firstId,
                    const std::vector<std::optional<size_t>>& remoteColumns,
                    std::vector<Id::T>& rowBuffer, IdTable& table) {
  rowBuffer.at(0) = firstId;
  reader(reinterpret_cast<char*>(rowBuffer.data() + 1),
         (rowBuffer.size() - 1) * sizeof(Id::T));
  table.emplace_back();
  size_t rowIndex = table.size() - 1;
  for (size_t col = 0; col < remoteColumns.size(); ++col) {
    table(rowIndex, col) =
        remoteColumns[col].has_value()
            ? Id::fromBits(rowBuffer[remoteColumns[col].value()])
            : Id::makeUndefined();
  }
}

// Core coroutine that reads the binary response and yields one
// `IdTableVocabPair` per vocab batch (or a single one if `yieldOnce` is set).
// The columns of the result are the `expectedVariables` in this order, see
// `getRemoteColumns`.
static Result::Generator importBinaryGenerator(
    HttpOrHttpsResponse response, const QueryExecutionContext& qec,
    std::vector<std::string> expectedVariables, bool yieldOnce,
    size_t maxStringBatchSize, CancellationHandle cancellationHandle) {
  ByteSpanReader reader{response.body_};
  auto header = Header::read(reader, maxStringBatchSize);
  auto remoteColumns = getRemoteColumns(expectedVariables, header);
  size_t numColumns = expectedVariables.size();

  // Special case 0 columns: yield a single pair with the correct row count.
  if (header.variableNames_.empty()) {
    co_yield Result::IdTableVocabPair{importNumRows(reader, numColumns, qec),
                                      LocalVocab{}};
    co_return;
  }

  IdRewriter rewriter{qec, std::move(header.patterns_),
                      header.geoPointEncoding_};
  IdTable currentBatch{numColumns, qec.getAllocator()};
  // The rows before this index have already been rewritten.
  size_t firstRowToRewrite = 0;
  std::vector<Id::T> rowBuffer(header.variableNames_.size());

  while (!reader.atEnd()) {
    cancellationHandle->throwIfCancelled();
    auto firstId = BinaryExportHelpers::read<Id::T>(reader);
    if (firstId != vocabMarker) {
      readRow(reader, firstId, remoteColumns, rowBuffer, currentBatch);
      continue;
    }
    // The end of a batch, rewrite its rows using the strings that follow.
    rewriter.startBatch(
        BinaryExportHelpers::readVectorOfStrings(reader, maxStringBatchSize));
    rewriter.rewriteRows(currentBatch, firstRowToRewrite);
    firstRowToRewrite = currentBatch.size();
    if (!yieldOnce) {
      co_yield Result::IdTableVocabPair{std::move(currentBatch),
                                        rewriter.takeLocalVocab()};
      currentBatch = IdTable{numColumns, qec.getAllocator()};
      firstRowToRewrite = 0;
    }
  }
  // The exporter always ends with a vocab, so all rows have been converted.
  if (firstRowToRewrite != currentBatch.size()) {
    throw std::runtime_error{
        "The result in QLever's binary export format ended unexpectedly"};
  }
  if (yieldOnce) {
    co_yield Result::IdTableVocabPair{std::move(currentBatch),
                                      rewriter.takeLocalVocab()};
  }
}

// _____________________________________________________________________________
Result importBinaryHttpResponse(bool requestLaziness,
                                HttpOrHttpsResponse response,
                                const QueryExecutionContext& qec,
                                std::vector<std::string> expectedVariables,
                                std::vector<ColumnIndex> resultSortedOn,
                                ad_utility::MemorySize maxStringBatchSize,
                                CancellationHandle cancellationHandle) {
  auto generator = importBinaryGenerator(
      std::move(response), qec, std::move(expectedVariables), !requestLaziness,
      maxStringBatchSize.getBytes(), std::move(cancellationHandle));

  if (requestLaziness) {
    return Result{std::move(generator), std::move(resultSortedOn)};
  } else {
    auto [idTable, localVocab] =
        ad_utility::getSingleElement(std::move(generator));
    return Result{std::move(idTable), std::move(resultSortedOn),
                  std::move(localVocab)};
  }
}
}  // namespace qlever::binary_export

#endif
