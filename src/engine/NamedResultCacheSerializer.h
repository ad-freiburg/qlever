// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_NAMEDRESULTCACHESERIALIZER_H
#define QLEVER_SRC_ENGINE_NAMEDRESULTCACHESERIALIZER_H

#include <boost/math/tools/roots.hpp>
#include <cstdint>

#include "backports/algorithm.h"
#include "engine/NamedResultCache.h"
#include "engine/NamedResultCacheFormatVersion.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/Serializer/LayoutReadSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/Serializer/TripleSerializer.h"

namespace namedResultCacheSerializer::detail {
// An arbitrary magic byte that is written at the very beginning of a
// serialized `NamedResultCache`. Used by `readFromSerializer` to give a clear
// error message when the input is not a serialized `NamedResultCache`.
constexpr uint8_t magicByte = 0xC3;
}  // namespace namedResultCacheSerializer::detail

namespace namedResultCacheSerializer {
using Entries = NamedResultCache::Entries;
using Value = NamedResultCache::Value;

// Write the `entries` (as returned by `getAllEntriesSortedByKey`) to the
// `serializer`, in exactly the format that `readFromSerializer` reads. Each
// value is written via `writeEntry(serializer, value)`, which has to write a
// `NamedResultCache::Value`, but may write a modified version of it (see
// `writeValue` below). The entries are written in the format version
// `entriesVersion` (one of `legacyFormatVersion` and
// `formatVersion`). Note that `writeEntry` has to write the values in
// the same `entriesVersion`, and that the legacy version can only represent
// geo indices with a single segment. The header that is written first (the
// magic byte, the version, and the number of entries) is read by
// `readEntriesHeader` below.
CPP_template(typename Serializer, typename WriteEntry)(
    requires ad_utility::serialization::WriteSerializer<Serializer> CPP_and
        ql::concepts::invocable<
            const WriteEntry&, Serializer&,
            const Value&>) void writeEntries(Serializer& serializer,
                                             const Entries& entries,
                                             const WriteEntry& writeEntry,
                                             uint16_t entriesVersion) {
  AD_CONTRACT_CHECK(entriesVersion == formatVersion ||
                    entriesVersion == legacyFormatVersion);
  // Write the magic byte and format version first, s.t. `readFromSerializer`
  // can detect and reject incompatible or unrelated input.
  serializer << detail::magicByte;
  serializer << entriesVersion;

  // Serialize the number of entries.
  serializer << entries.size();

  // Serialize each entry.
  for (const auto& [key, value] : entries) {
    serializer << key;
    writeEntry(serializer, *value);
  }
}

// The header that `writeEntries` writes before the entries (see
// `readEntriesHeader` and `parseEntriesHeader`).
struct EntriesHeader {
  // The byte range of the header (the magic byte, the version, and the number
  // of entries) in the buffer that it was written to. Only set by
  // `parseEntriesHeader`.
  ad_utility::serialization::ByteRange whole_;
  // The entries format version (one of `legacyFormatVersion` and
  // `formatVersion`), in which all the entries that follow are written.
  uint16_t version_ = 0;
  size_t numEntries_ = 0;
};

// Read and check the header that `writeEntries` writes before the entries
// from the `serializer`, which is advanced past the header (the entries
// themselves then follow). Throw if the magic byte does not match, or if the
// version is none of `legacyFormatVersion` and `formatVersion`. The `whole_`
// of the result is not set (see `parseEntriesHeader`).
CPP_template(typename Serializer)(
    requires ad_utility::serialization::ReadSerializer<Serializer>)
    EntriesHeader readEntriesHeader(Serializer& serializer) {
  EntriesHeader header;
  uint8_t readMagicByte;
  serializer >> readMagicByte;
  if (readMagicByte != detail::magicByte) {
    AD_THROW(
        "The given input is not a serialized `NamedResultCache` (the magic "
        "byte does not match)");
  }
  serializer >> header.version_;
  if (header.version_ != formatVersion &&
      header.version_ != legacyFormatVersion) {
    AD_THROW(absl::StrCat(
        "The serialized `NamedResultCache` has format version ",
        header.version_,
        ", but this version of QLever only supports the format versions ",
        legacyFormatVersion, " and ", formatVersion,
        ". The named result cache was probably written by an incompatible "
        "version of QLever"));
  }
  serializer >> header.numEntries_;
  return header;
}

// Parse the header that `writeEntries` writes before the entries, starting at
// the current position of the `reader`, which is advanced past the header
// (the entries themselves then follow, see `parseEntryLayout`). This is
// `readEntriesHeader` (including its checks), plus the `whole_` range. The
// input has to be written with the aligned serialization (see
// `AlignedByteBufferWriteSerializer`).
EntriesHeader parseEntriesHeader(
    ad_utility::serialization::LayoutReadSerializer& reader);

// The byte ranges that the parts of one serialized entry of a
// `NamedResultCache` (its key followed by its value, as written by
// `writeEntries` and `writeValue`) occupy in the buffer that it was written to
// (see `parseEntryLayout`). All ranges are offsets into that buffer.
struct EntryLayout {
  using ByteRange = ad_utility::serialization::ByteRange;
  // The complete entry, starting with its key.
  ByteRange whole_;
  std::string key_;
  size_t numRows_ = 0;
  size_t numColumns_ = 0;
  // For each column the payload bytes of its `Id`s (`numRows_ * sizeof(Id)`
  // bytes, aligned to `alignof(Id)`), without the preceding size.
  std::vector<ByteRange> columnPayloads_;
  // The complete serialized `VariableToColumnMap`.
  ByteRange varToColMap_;
  // The payload bytes of the `resultSortedOn_` array, and its contents.
  ByteRange resultSortedOn_;
  std::vector<uint64_t> sortedOnColumns_;
  // The payload bytes (the characters) of the cache key.
  ByteRange cacheKey_;
  // Whether the entry has a geo index, and its layout if so.
  bool hasGeoIndex_ = false;
  SpatialJoinCachedIndex::Layout geo_;
};
}  // namespace namedResultCacheSerializer

namespace namedResultCacheSerializer {
// Read a `NamedResultCache::Value` that was written in the format of the given
// `entriesVersion` (one of `legacyFormatVersion` and
// `formatVersion`) from the `serializer` into `arg`. The
// `allocatorForSerialization_` and `contextForSerialization_` of `arg` have to
// be set. This is the read counterpart of `writeValue` below, and also what the
// generic serialization of a `NamedResultCache::Value` below does (for the
// current version).
//
// If the `serializer` only determines the layout (see `LayoutSerializer`),
// then `arg` has to be an `EntryLayout` instead, into which the byte ranges of
// the parts of the value are recorded (everything but its `whole_` and `key_`,
// see `parseEntryLayout`, which sets them). Nothing is constructed then, the
// `Id`s of the columns are not inspected, and only the checks of the framing
// and of the sizes (e.g. of the number of rows of each column) are performed
// (see also `SpatialJoinCachedIndex::readFromSerializer`).
CPP_template(typename Serializer, typename Arg)(
    requires ad_utility::serialization::ReadSerializer<
        Serializer>) void readValue(Serializer& serializer, Arg& arg,
                                    uint16_t entriesVersion) {
  using namespace ad_utility::serialization;
  constexpr bool onlyLayout = LayoutSerializer<Serializer>;
  static_assert(onlyLayout
                    ? ad_utility::isSimilar<Arg, EntryLayout>
                    : ad_utility::isSimilar<Arg, NamedResultCache::Value>,
                "`readValue` reads into a `NamedResultCache::Value`, or into "
                "an `EntryLayout` if the serializer only determines the "
                "layout");

  // Deserialize the `LocalVocab` and get the `Id` mapping (for the layout, only
  // skip the local vocab).
  std::tuple<LocalVocab, absl::flat_hash_map<Id::T, Id>> localVocabAndMapping;
  if constexpr (onlyLayout) {
    ad_utility::detail::skipLocalVocab(serializer);
  } else {
    AD_CORRECTNESS_CHECK(arg.contextForSerialization_ != nullptr);
    localVocabAndMapping = ad_utility::detail::deserializeLocalVocab(
        serializer, *arg.contextForSerialization_);
  }
  [[maybe_unused]] auto& [localVocab, mapping] = localVocabAndMapping;

  // Deserialize the `IdTable` with the `Id` mapping applied (for the layout,
  // only record where the columns are).
  size_t numRows, numColumns;
  serializer >> numRows;
  serializer >> numColumns;

  [[maybe_unused]] ExplicitIdTableOperation::IdTableOrView resultTable;
  if constexpr (!onlyLayout) {
    AD_CORRECTNESS_CHECK(arg.allocatorForSerialization_.has_value());
  }
  if constexpr (onlyLayout) {
    arg.numRows_ = numRows;
    arg.numColumns_ = numColumns;
    arg.columnPayloads_.reserve(numColumns);
    for (size_t i = 0; i < numColumns; ++i) {
      auto column = serializer.template readArray<Id>();
      AD_CONTRACT_CHECK(column.size() == numRows * sizeof(Id),
                        "A column of a named cache entry has the wrong size");
      arg.columnPayloads_.push_back(column);
    }
  } else if constexpr (ZeroCopyReadSerializer<Serializer>) {
    // Zero-copy path: build a non-owning `IdTableView<0>` directly from
    // spans into the serializer's buffer, without copying the column data.
    // Since the writing side (see above) rejects any entry that contains a
    // `LocalVocabIndex` id, `mapping` can never actually apply to any id in
    // the columns, so skipping `deserializeIds`'s remapping step here is
    // safe. We still defensively re-check the invariant.
    IdTableView<0>::ViewSpans columns;
    columns.reserve(numColumns);
    for (size_t i = 0; i < numColumns; ++i) {
      auto column = zeroCopyDeserializeToSpan<Id>(serializer);
      AD_CORRECTNESS_CHECK(column.size() == numRows);
      AD_CORRECTNESS_CHECK(
          ql::ranges::find(column, Datatype::LocalVocabIndex,
                           Id::getDatatypeL) == column.end(),
          "Named result cache entries that contain local vocab entries "
          "currently cannot be deserialized.");
      columns.push_back(column);
    }
    resultTable =
        IdTableView<0>::fromColumns(std::move(columns), numColumns, numRows,
                                    arg.allocatorForSerialization_.value());
  } else {
    IdTable idTable{numColumns, arg.allocatorForSerialization_.value()};
    idTable.resize(numRows);
    for (auto&& col : idTable.getColumns()) {
      ad_utility::detail::deserializeIds(serializer, mapping, col);
    }
    resultTable = std::make_shared<const IdTable>(std::move(idTable));
  }

  // Deserialize the `VariableToColumnMap`, see `serializeDeterministically`
  // in `VariableToColumnMap.h` (for the layout, only record its range).
  VariableToColumnMap varToColMap;
  auto readVarToColMap = [&serializer, &varToColMap] {
    serializeDeterministically(serializer, varToColMap);
  };
  if constexpr (onlyLayout) {
    arg.varToColMap_ = serializer.recordRange(readVarToColMap);
  } else {
    readVarToColMap();
  }

  // Deserialize `resultSortedOn` (for the layout, its range and contents).
  std::vector<ColumnIndex> resultSortedOn;
  if constexpr (onlyLayout) {
    auto sortedOn = zeroCopyDeserializeToSpan<ColumnIndex>(serializer);
    arg.resultSortedOn_ = serializer.rangeOf(sortedOn);
    arg.sortedOnColumns_.assign(sortedOn.begin(), sortedOn.end());
  } else {
    serializer >> resultSortedOn;
  }

  // Deserialize `cacheKey` (for the layout, only its range).
  std::string cacheKey;
  if constexpr (onlyLayout) {
    arg.cacheKey_ = serializer.template readArray<char>();
  } else {
    serializer >> cacheKey;
  }

  // Deserialize `cachedGeoIndex` (for the layout, its layout).
  bool hasGeoIndex;
  serializer >> hasGeoIndex;
  if constexpr (onlyLayout) {
    arg.hasGeoIndex_ = hasGeoIndex;
    if (hasGeoIndex) {
      arg.geo_ = SpatialJoinCachedIndex::parseLayout(serializer, numRows,
                                                     entriesVersion);
    }
  } else {
    std::optional<SpatialJoinCachedIndex> cachedGeoIndex;
    if (hasGeoIndex) {
      cachedGeoIndex.emplace(SpatialJoinCachedIndex::readFromSerializer(
          serializer, numRows, entriesVersion));
    }

    // Construct the `Value`.
    arg = NamedResultCache::Value{
        std::move(resultTable),    std::move(varToColMap),
        std::move(resultSortedOn), std::move(localVocab),
        std::move(cacheKey),       std::move(cachedGeoIndex)};
  }
}
}  // namespace namedResultCacheSerializer

// _____________________________________________________________________________
CPP_template_def(typename Serializer)(
    requires ad_utility::serialization::WriteSerializer<Serializer>)
void NamedResultCache::writeToSerializer(Serializer& serializer) const {
  namedResultCacheSerializer::writeEntries(
      serializer, getAllEntriesSortedByKey(),
      [](Serializer& s, const Value& value) { s << value; },
      namedResultCacheSerializer::formatVersion);
}

// _____________________________________________________________________________
CPP_template_def(typename Serializer)(
    requires ad_utility::serialization::ReadSerializer<Serializer>)
void NamedResultCache::readFromSerializer(Serializer& serializer,
                                          Value::Allocator allocator,
                                          const LocalVocabContext& context) {
  // Clear the cache first.
  clear();

  // Read and check the header (the magic byte, the format version, and the
  // number of entries) written by `writeToSerializer`.
  auto header = namedResultCacheSerializer::readEntriesHeader(serializer);

  // Deserialize each entry and add to the cache.
  for (size_t i = 0; i < header.numEntries_; ++i) {
    // Deserialize the key
    Key key;
    serializer >> key;

    // Deserialize the value.
    Value value;
    value.allocatorForSerialization_ = allocator;
    value.contextForSerialization_ = &context;
    namedResultCacheSerializer::readValue(serializer, value, header.version_);

    // Use the store method to maintain consistency.
    store(key, std::move(value));
  }
}

namespace namedResultCacheSerializer {
// Write `value` to the `serializer`, in exactly the format that the read
// branch of the serialization of a `NamedResultCache::Value` below reads.
//
// The `columns` (a range of ranges of `Id`, one per column of the result) and
// the `resultSortedOn` are passed separately, so that a caller can write a
// *rewritten* version of the `value`: a caller may for example replace the
// `Id`s that refer to local vocab entries by `Id`s of the main or of a
// secondary vocabulary, which may also change the sort order. The
// `columns` therefore only have to agree with `value.result_` in their number
// and in the number of rows, which is checked. If `writeLocalVocabWords` is
// `false`, the words of the local vocab of the `value` are not written (only
// its blank node blocks, see `serializeOnlyBlankNodeBlocksFromLocalVocab`),
// because such a caller has stored them elsewhere. The geo index (if any) is
// written in the format of the given `entriesVersion`, which has to be the one
// given to `writeEntries`. In the legacy version, the geo index must have a
// single segment. The read counterpart `readValue` above also determines the
// layout of the written bytes (see `parseEntryLayout` below).
CPP_template(typename Serializer, typename Columns)(
    requires ad_utility::serialization::WriteSerializer<
        Serializer>) void writeValue(Serializer& serializer,
                                     const NamedResultCache::Value& value,
                                     const Columns& columns,
                                     const std::vector<ColumnIndex>&
                                         resultSortedOn,
                                     bool writeLocalVocabWords,
                                     uint16_t entriesVersion) {
  // Serialize the `LocalVocab` first (required for ID remapping).
  if (writeLocalVocabWords) {
    ad_utility::detail::serializeLocalVocab(serializer, value.localVocab_);
  } else {
    ad_utility::detail::serializeOnlyBlankNodeBlocksFromLocalVocab(
        serializer, value.localVocab_);
  }

  // Serialize the `IdTable` (uses the `serializeIds` helper which handles
  // `LocalVocab` IDs).
  const auto& resultView = ExplicitIdTableOperation::viewOf(value.result_);
  serializer << resultView.numRows();
  serializer << resultView.numColumns();
  AD_CORRECTNESS_CHECK(ql::ranges::size(columns) == resultView.numColumns());
  for (const auto& col : columns) {
    // NOTE 1: Although the code for serialization of a local vocab above is
    // already incorporated, we currently still let local vocab entries throw
    // an exception, because there are some caveats in the serialization that
    // don't work yet, and will only be mitigated in the future. The check only
    // applies to the `Id`s that are actually written (the `columns`, not
    // `value.result_`). The blob writer (see
    // `NamedCachedQueryBlobManager::serialize`) therefore never triggers it,
    // because it passes `columns` in which canonicalization has already
    // replaced all such `Id`s (see `canonicalizeForSerialization`).
    //
    // NOTE 2: Even though we disallow the local vocab, it is crucial to
    // serialize the local vocab because of possible added blank node indices,
    // which we do handle correctly, and which also rely on the local vocab.
    //
    // NOTE 3: The blobs of `NamedCachedQueryBlobManager` support local vocab
    // entries by canonicalizing every entry first, which rewrites those `Id`s
    // (see `NamedCacheSecondaryVocabRewriter.h`). Only the blank node blocks of
    // the local vocab are written for them, not its words.
    //
    // TODO<joka921> Mitigate the inconsistencies in the serializer, and then
    // allow local vocab entries here.
    AD_CORRECTNESS_CHECK(
        ql::ranges::find(col, Datatype::LocalVocabIndex, Id::getDatatypeL) ==
            ql::ranges::end(col),
        "Named result cache entries that contain local vocab entries "
        "currently cannot be serialized. Note that local vocab entries can "
        "also occur if SPARQL UPDATE operations have been performed on the "
        "index before creating the named cached result.");
    AD_CORRECTNESS_CHECK(ql::ranges::size(col) == resultView.numRows());
    ad_utility::detail::serializeIds(serializer, col);
  }

  // Serialize the `VariableToColumnMap` deterministically, see
  // `serializeDeterministically` in `VariableToColumnMap.h`.
  serializeDeterministically(serializer, value.varToColMap_);

  // Serialize `resultSortedOn` (vector of `ColumnIndex`).
  serializer << resultSortedOn;

  // Serialize `cacheKey` (string).
  serializer << value.cacheKey_;

  // Serialize the `cachedGeoIndex_`.
  //
  // NOTE: The `cachedGeoIndex_` is not default-constructible, so it cannot be
  // read back via the generic serialization of a `std::optional`, and for
  // consistency it is written manually as well (the same reasoning as for the
  // `VariableToColumnMap`, see `serializeDeterministically`).
  bool hasGeoIndex = value.cachedGeoIndex_.has_value();
  serializer << hasGeoIndex;
  if (hasGeoIndex) {
    value.cachedGeoIndex_.value().writeToSerializer(serializer, entriesVersion);
  }
}

// Determine the layout (see `EntryLayout`) of one entry (its key followed by
// its value) that was written by `writeEntries` and `writeValue` in the given
// `entriesVersion` with the aligned serialization (see
// `AlignedByteBufferWriteSerializer`), starting at the current position of the
// `reader`, which is advanced past the entry. This is `readValue` with a
// layout serializer (so the format is only spelled out there), plus the key
// and the `whole_` range. Throw if the input is corrupt (for example, a column
// with the wrong number of rows) or truncated.
EntryLayout parseEntryLayout(
    ad_utility::serialization::LayoutReadSerializer& reader,
    uint16_t entriesVersion);
}  // namespace namedResultCacheSerializer

namespace ad_utility::serialization {

// Serialization for `NamedResultCache::Value`
// This serializes the complete Value including the `LocalVocab` with proper ID
// remapping.
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::SimilarTo<T, NamedResultCache::Value>)) {
  if constexpr (WriteSerializer<S>) {
    // Write the value as it is: with the original columns, the original sort
    // order, and the words of its local vocab (see `writeValue` above for the
    // cases in which those are replaced).
    const auto& resultView = ExplicitIdTableOperation::viewOf(arg.result_);
    namedResultCacheSerializer::writeValue(
        serializer, arg, resultView.getColumns(), arg.resultSortedOn_,
        /*writeLocalVocabWords=*/true,
        namedResultCacheSerializer::formatVersion);
  } else {
    namedResultCacheSerializer::readValue(
        serializer, arg, namedResultCacheSerializer::formatVersion);
  }
}

}  // namespace ad_utility::serialization
#endif  // QLEVER_SRC_ENGINE_NAMEDRESULTCACHESERIALIZER_H
