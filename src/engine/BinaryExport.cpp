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

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/Result.h"
#include "engine/StringMapping.h"
#include "util/Algorithm.h"
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
AD_ALWAYS_INLINE Id
toExportableId(Id originalId, [[maybe_unused]] const LocalVocab& localVocab,
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

  AD_SERIALIZE_FRIEND_FUNCTION(Header) {
    static constexpr bool isReader =
        ad_utility::serialization::ReadSerializer<S>;
    std::string magic{magicBytes};
    serializer | magic;
    if constexpr (isReader) {
      if (magic != magicBytes) {
        throw std::runtime_error{
            "The result is not in QLever's binary export format"};
      }
    }
    uint16_t version = ad_utility::binaryQleverExportVersion;
    serializer | version;
    if constexpr (isReader) {
      if (version != ad_utility::binaryQleverExportVersion) {
        throw std::runtime_error{absl::StrCat(
            "The result is in version ", version,
            " of QLever's binary export format, but only version ",
            ad_utility::binaryQleverExportVersion, " is supported")};
      }
    }
    serializer | arg.geoPointEncoding_;
    std::string patterns;
    if constexpr (isReader) {
      serializer >> patterns;
      arg.patterns_ = detail::patternsFromJson(
          nlohmann::json::parse(patterns), EncodedIriManager::NumBitsEncoding,
          EncodedIriManager::maxNumPrefixes_);
    } else {
      nlohmann::json json;
      detail::patternsToJson(json, arg.patterns_,
                             EncodedIriManager::NumBitsEncoding);
      serializer << json.dump();
    }
    serializer | arg.variableNames_;
  }
};

// Use special undefined value that's not actually used as a real value.
static constexpr Id::T vocabMarker = Id::makeUndefined().getBits() + 0b10101010;
static_assert(Id::fromBits(vocabMarker).getDatatype() == Datatype::Undefined);

// _____________________________________________________________________________
ad_utility::streams::stream_generator exportAsQLeverBinary(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle) {
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();
  AD_LOG_DEBUG << "Starting binary export..." << std::endl;

  using namespace std::string_view_literals;
  // Get all columns with defined variables.
  QueryExecutionTree::ColumnIndicesAndTypes columns =
      qet.selectedVariablesToColumnIndices(selectClause, true);
  std::erase(columns, std::nullopt);

  {
    ad_utility::serialization::ByteBufferWriteSerializer serializer{};
    serializer << Header{GeoPoint::encoding(),
                         qet.getQec()->getIndex().encodedIriManager().patterns_,
                         ad_utility::transform(columns, [](const auto& column) {
                           return column.value().variable_;
                         })};
    co_yield std::string_view{serializer.data().data(),
                              serializer.data().size()};
  }

  // Iterate over the result and yield the bindings.
  uint64_t resultSize = 0;

  // For zero columns, only emit the total row count.
  if (columns.empty()) {
    for (const auto& [pair, range] : ExportQueryExecutionTrees::getRowIndices(
             limitAndOffset, *result, resultSize)) {
      for ([[maybe_unused]] uint64_t i : range) {
        cancellationHandle->throwIfCancelled();
      }
    }
    co_yield raw(resultSize);
    co_return;
  }

  // Non-zero columns: export IDs with periodic vocab flushes.
  StringMapping stringMapping;
  // The `stringMapping` might refer to entries of the local vocabs of the
  // tables of the result, which (for a lazy result) are destroyed when the next
  // table is requested. They are kept alive until the next flush.
  LocalVocab localVocabsOfBatch;
  uint64_t numRowsInBatch = 0;
  auto flush = [&numRowsInBatch, &stringMapping, &qet, &localVocabsOfBatch]() {
    numRowsInBatch = 0;
    auto strings = BinaryExportHelpers::writeVectorOfStrings(
        stringMapping.flush(qet.getQec()->getIndex()));
    localVocabsOfBatch = LocalVocab{};
    return absl::StrCat(raw(vocabMarker), strings);
  };
  for (const auto& [pair, range] : ExportQueryExecutionTrees::getRowIndices(
           limitAndOffset, *result, resultSize)) {
    for (uint64_t i : range) {
      for (const auto& column : columns) {
        Id id = pair.idTable_(i, column->columnIndex_);
        co_yield raw(
            toExportableId(id, pair.localVocab_, stringMapping).getBits());
      }
      ++numRowsInBatch;
      // TODO<joka921> arbitrary constants.
      if (numRowsInBatch >= 100'000 || stringMapping.size() >= 10'000) {
        co_yield flush();
      }
      cancellationHandle->throwIfCancelled();
    }
    if (stringMapping.size() > 0) {
      localVocabsOfBatch.mergeWith(pair.localVocab());
    }
  }

  // Always send the trailing vocab so the importer can finalize the last batch.
  co_yield flush();
}

// Convert the string representation of a literal or IRI to a local `Id`, see
// `LocalVocab::getIdAndAddIfNotContained`.
static Id stringToId(std::string representation,
                     const QueryExecutionContext& qec, LocalVocab& vocab) {
  return vocab.getIdAndAddIfNotContained(
      LocalVocabEntry::fromStringRepresentation(std::move(representation),
                                                qec.getLocalVocabContext()));
}

// Replace a blank node of the exporting instance by a new local blank node.
// The same remote blank node is always replaced by the same local one, see
// `blankNodeMapping`.
static Id remapBlankNode(Id id, const QueryExecutionContext& qec,
                         LocalVocab& vocab,
                         ad_utility::HashMap<Id::T, Id>& blankNodeMapping) {
  auto [it, inserted] = blankNodeMapping.try_emplace(id.getBits(), Id{});
  if (inserted) {
    it->second = Id::makeFromBlankNodeIndex(
        vocab.getBlankNodeIndex(qec.getIndex().getBlankNodeManager()));
  }
  return it->second;
}

// Remap an `Id` of type `EncodedVal` that was encoded using the remote
// `patterns`. If the pattern also exists locally, only the tag is changed,
// otherwise the IRI is decoded and converted to a local `Id`.
static Id remapEncodedVal(
    Id id, const QueryExecutionContext& qec, LocalVocab& vocab,
    const ad_utility::HashMap<uint8_t, uint8_t>& prefixMapping,
    const std::vector<encodedIri::Pattern>& patterns) {
  auto [prefixIdx, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(id);
  if (prefixMapping.contains(prefixIdx)) {
    return EncodedIriManager::makeIdFromPrefixIdxAndPayload(
        prefixMapping.at(prefixIdx), payload);
  }
  return stringToId(encodedIri::decodeToIri(patterns.at(prefixIdx), payload),
                    qec, vocab);
}

// _____________________________________________________________________________
void BinaryExportHelpers::rewriteVocabIds(
    IdTable& result, const size_t dirtyIndex, const QueryExecutionContext& qec,
    LocalVocab& vocab, const std::vector<std::string>& transmittedStrings,
    const ad_utility::HashMap<uint8_t, uint8_t>& prefixMapping,
    const std::vector<encodedIri::Pattern>& prefixes,
    ad_utility::HashMap<Id::T, Id>& blankNodeMapping,
    GeoPointEncodingEnum remoteGeoPointEncoding) {
  for (auto col : result.getColumns()) {
    ql::ranges::for_each(
        col.subspan(dirtyIndex),
        [&qec, &vocab, &transmittedStrings, &prefixMapping, &prefixes,
         &blankNodeMapping, remoteGeoPointEncoding](Id& id) {
          switch (id.getDatatype()) {
            case Datatype::EncodedVal:
              id = remapEncodedVal(id, qec, vocab, prefixMapping, prefixes);
              break;
            case Datatype::BlankNodeIndex:
              id = remapBlankNode(id, qec, vocab, blankNodeMapping);
              break;
            case Datatype::LocalVocabIndex:
              // Undo the shift done during encoding.
              id = stringToId(
                  transmittedStrings.at(
                      reinterpret_cast<size_t>(id.getLocalVocabIndex()) >>
                      Id::numDatatypeBits),
                  qec, vocab);
              break;
            case Datatype::GeoPoint:
              // The two instances might use different encodings of `GeoPoint`s.
              if (remoteGeoPointEncoding != GeoPoint::encoding()) {
                static constexpr auto mask =
                    ad_utility::bitMaskForLowerBits(Id::numDataBits);
                id = Id::fromBits(
                    (id.getBits() & ~mask) |
                    GeoPoint::convertEncoding(id.getBits() & mask,
                                              remoteGeoPointEncoding,
                                              GeoPoint::encoding()));
              }
              break;
            default:
              // All other IDs that the exporter sends are self-contained, see
              // `toExportableId`.
              AD_CORRECTNESS_CHECK(id.isTrivial());
          }
        });
  }
}

// _____________________________________________________________________________
ad_utility::HashMap<uint8_t, uint8_t> BinaryExportHelpers::getPrefixMapping(
    const QueryExecutionContext& qec,
    const std::vector<encodedIri::Pattern>& prefixes) {
  ad_utility::HashMap<uint8_t, uint8_t> prefixMapping;
  const auto& localPrefixes = qec.getIndex().encodedIriManager().patterns_;
  for (const auto& [index, prefix] : ::ranges::views::enumerate(prefixes)) {
    auto prefixIt = ql::ranges::find(localPrefixes, prefix);
    if (prefixIt != localPrefixes.end()) {
      prefixMapping[index] = static_cast<uint8_t>(
          ql::ranges::distance(localPrefixes.begin(), prefixIt));
    }
  }
  return prefixMapping;
}

// Core coroutine that reads the binary response and yields one
// `IdTableVocabPair` per vocab batch (or a single one if `yieldOnce` is set).
// The columns of the result are the `expectedVariables` in this order. A
// variable that is not part of the response is undefined in all rows, and
// variables of the response that are not expected are ignored.
Result::Generator importBinaryGenerator(
    HttpOrHttpsResponse response, const QueryExecutionContext& qec,
    std::vector<std::string> expectedVariables, bool yieldOnce) {
  auto bytes = response.body_ | ql::views::join;
  auto it = ql::ranges::begin(bytes);
  auto end = ql::ranges::end(bytes);

  BinaryExportHelpers::IteratorReader<decltype(it), decltype(end)> itReader{
      it, end};
  ad_utility::serialization::ReadViaCallableSerializer serializer{
      std::ref(itReader)};
  Header header;
  serializer >> header;
  // Done with the serializer for now, reextracting the iterator.
  it = itReader.it;

  auto prefixMapping =
      BinaryExportHelpers::getPrefixMapping(qec, header.patterns_);
  size_t numRemoteColumns = header.variableNames_.size();
  size_t numColumns = expectedVariables.size();

  // For each column of the result, the corresponding column of the response.
  std::vector<std::optional<size_t>> remoteColumns;
  for (const auto& variable : expectedVariables) {
    auto remoteIt = ql::ranges::find(header.variableNames_, variable);
    remoteColumns.push_back(
        remoteIt == header.variableNames_.end()
            ? std::nullopt
            : std::optional{static_cast<size_t>(
                  remoteIt - header.variableNames_.begin())});
  }

  // Special case 0 columns: yield a single pair with the correct row count.
  if (numRemoteColumns == 0) {
    auto numRows = BinaryExportHelpers::read<uint64_t>(it, end);
    IdTable result{numColumns, qec.getAllocator()};
    result.resize(numRows);
    for (auto column : result.getColumns()) {
      ql::ranges::fill(column, Id::makeUndefined());
    }
    co_yield Result::IdTableVocabPair{std::move(result), LocalVocab{}};
    co_return;
  }

  LocalVocab vocab;
  ad_utility::HashMap<Id::T, Id> blankNodeMapping;

  IdTable currentBatch{numColumns, qec.getAllocator()};
  // At which index we need to start converting values.
  size_t dirtyIndex = 0;
  std::vector<Id::T> row(numRemoteColumns);

  while (it != end) {
    row.at(0) = BinaryExportHelpers::read<Id::T>(it, end);
    if (row.at(0) == vocabMarker) {
      auto transmittedStrings =
          BinaryExportHelpers::readVectorOfStrings(it, end);
      BinaryExportHelpers::rewriteVocabIds(currentBatch, dirtyIndex, qec, vocab,
                                           transmittedStrings, prefixMapping,
                                           header.patterns_, blankNodeMapping,
                                           header.geoPointEncoding_);
      dirtyIndex = currentBatch.size();
      if (!yieldOnce) {
        // The blank nodes of later batches might be the same as the ones of
        // this batch, so the next batch has to keep them alive.
        LocalVocab nextVocab;
        if (!blankNodeMapping.empty()) {
          nextVocab.mergeWith(vocab);
        }
        co_yield Result::IdTableVocabPair{std::move(currentBatch),
                                          std::move(vocab)};
        currentBatch = IdTable{numColumns, qec.getAllocator()};
        vocab = std::move(nextVocab);
        dirtyIndex = 0;
      }
    } else {
      for (size_t i = 1; i < numRemoteColumns; ++i) {
        row[i] = BinaryExportHelpers::read<Id::T>(it, end);
      }
      currentBatch.emplace_back();
      size_t rowIndex = currentBatch.size() - 1;
      for (size_t col = 0; col < numColumns; ++col) {
        currentBatch(rowIndex, col) =
            remoteColumns[col].has_value()
                ? Id::fromBits(row[remoteColumns[col].value()])
                : Id::makeUndefined();
      }
    }
  }
  // The exporter always ends with a vocab, so all rows have been converted.
  if (dirtyIndex != currentBatch.size()) {
    throw std::runtime_error{
        "The result in QLever's binary export format ended unexpectedly"};
  }
  if (yieldOnce) {
    co_yield Result::IdTableVocabPair{std::move(currentBatch),
                                      std::move(vocab)};
  }
}

// _____________________________________________________________________________
Result importBinaryHttpResponse(bool requestLaziness,
                                HttpOrHttpsResponse response,
                                const QueryExecutionContext& qec,
                                std::vector<std::string> expectedVariables,
                                std::vector<ColumnIndex> resultSortedOn) {
  auto generator = importBinaryGenerator(
      std::move(response), qec, std::move(expectedVariables), !requestLaziness);

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
