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
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/Serializer/TripleSerializer.h"

namespace namedResultCacheSerializer::detail {
// An arbitrary magic byte that is written at the very beginning of a
// serialized `NamedResultCache`. Used by `readFromSerializer` to give a clear
// error message when the input is not a serialized `NamedResultCache`.
constexpr uint8_t magicByte = 0xC3;

// The version of the (de)serialization format implemented below. Increment
// this whenever the format changes in a way that is incompatible with
// previously serialized data, s.t. `readFromSerializer` can detect and reject
// data that was written by an incompatible version of QLever.
//
// Version 1 is the legacy format, whose geo index consists of the geometry
// column, one encoded S2 index, and a hash map from shape ids to rows. It is
// still read by `readFromSerializer`, but is only written if no entry has a geo
// index (see `writeEntries`). Version 2 is the current format, whose geo index
// is the segmented one that is written by `SpatialJoinCachedIndex::
// writeToSerializer`. All other parts of the two formats are identical.
constexpr uint16_t legacyFormatVersion = 1;
constexpr uint16_t formatVersion = 2;
}  // namespace namedResultCacheSerializer::detail

namespace namedResultCacheSerializer {
using Entries = NamedResultCache::Entries;
using Value = NamedResultCache::Value;

// Write the `entries` (as returned by `getAllEntriesSortedByKey`) to the
// `serializer`, in exactly the format that `readFromSerializer` reads. Each
// value is written via `writeEntry(serializer, key, value)`, which has to write
// a `NamedResultCache::Value`, but may write a modified version of it (see
// `writeValue` below). The format version `entriesVersion` of the entries is
// written, it is `detail::formatVersion` by default. Writing the legacy
// `detail::legacyFormatVersion` is rejected if any of the entries has a geo
// index, because it cannot be represented in that format. Note that
// `writeEntry` has to write the values in the same `entriesVersion`.
CPP_template(typename Serializer, typename WriteEntry)(
    requires ad_utility::serialization::WriteSerializer<Serializer> CPP_and
        ql::concepts::invocable<
            const WriteEntry&, Serializer&, const std::string&,
            const Value&>) void writeEntries(Serializer& serializer,
                                             const Entries& entries,
                                             const WriteEntry& writeEntry,
                                             uint16_t entriesVersion =
                                                 detail::formatVersion) {
  AD_CONTRACT_CHECK(entriesVersion == detail::formatVersion ||
                    entriesVersion == detail::legacyFormatVersion);
  AD_CONTRACT_CHECK(
      entriesVersion != detail::legacyFormatVersion ||
          ql::ranges::none_of(
              entries,
              [](const auto& entry) {
                return entry.second->cachedGeoIndex_.has_value();
              }),
      "Entries with a geo index cannot be written in the legacy format");
  // Write the magic byte and format version first, s.t. `readFromSerializer`
  // can detect and reject incompatible or unrelated input.
  serializer << detail::magicByte;
  serializer << entriesVersion;

  // Serialize the number of entries.
  serializer << entries.size();

  // Serialize each entry.
  for (const auto& [key, value] : entries) {
    serializer << key;
    writeEntry(serializer, key, *value);
  }
}
}  // namespace namedResultCacheSerializer

namespace namedResultCacheSerializer {
// Read a `NamedResultCache::Value` that was written in the format of the given
// `entriesVersion` (one of `detail::legacyFormatVersion` and
// `detail::formatVersion`) from the `serializer` into `arg`. The
// `allocatorForSerialization_` and `contextForSerialization_` of `arg` have to
// be set. This is the read counterpart of `writeValue` below, and also what the
// generic serialization of a `NamedResultCache::Value` below does (for the
// current version).
CPP_template(typename Serializer)(
    requires ad_utility::serialization::ReadSerializer<
        Serializer>) void readValue(Serializer& serializer,
                                    NamedResultCache::Value& arg,
                                    uint16_t entriesVersion) {
  using namespace ad_utility::serialization;
  // Deserialize the LocalVocab and get the ID mapping.
  AD_CORRECTNESS_CHECK(arg.contextForSerialization_ != nullptr);
  auto [localVocab, mapping] = ad_utility::detail::deserializeLocalVocab(
      serializer, *arg.contextForSerialization_);

  // Deserialize the IdTable with ID mapping applied.
  size_t numRows, numColumns;
  serializer >> numRows;
  serializer >> numColumns;

  AD_CORRECTNESS_CHECK(arg.allocatorForSerialization_.has_value());
  ExplicitIdTableOperation::IdTableOrView resultTable;
  if constexpr (ZeroCopyReadSerializer<Serializer>) {
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
                           &Id::getDatatype) == column.end(),
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
  // in `VariableToColumnMap.h`.
  VariableToColumnMap varToColMap;
  serializeDeterministically(serializer, varToColMap);

  // Deserialize `resultSortedOn`.
  std::vector<ColumnIndex> resultSortedOn;
  serializer >> resultSortedOn;

  // Deserialize `cacheKey`.
  std::string cacheKey;
  serializer >> cacheKey;

  // Deserialize `cachedGeoIndex`.
  bool hasGeoIndex;
  serializer >> hasGeoIndex;
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
}  // namespace namedResultCacheSerializer

// _____________________________________________________________________________
CPP_template_def(typename Serializer)(
    requires ad_utility::serialization::WriteSerializer<
        Serializer>) void NamedResultCache::writeToSerializer(Serializer&
                                                                  serializer)
    const {
  namedResultCacheSerializer::writeEntries(
      serializer, getAllEntriesSortedByKey(),
      [](Serializer& s, [[maybe_unused]] const std::string& key,
         const Value& value) { s << value; });
}

// _____________________________________________________________________________
CPP_template_def(typename Serializer)(
    requires ad_utility::serialization::ReadSerializer<
        Serializer>) void NamedResultCache::
    readFromSerializer(Serializer& serializer, Value::Allocator allocator,
                       const LocalVocabContext& context) {
  // Clear the cache first.
  clear();

  // Read and check the magic byte and format version written by
  // `writeToSerializer`.
  uint8_t readMagicByte;
  serializer >> readMagicByte;
  if (readMagicByte != namedResultCacheSerializer::detail::magicByte) {
    AD_THROW(
        "The given input is not a serialized `NamedResultCache` (the magic "
        "byte does not match)");
  }
  uint16_t readFormatVersion;
  serializer >> readFormatVersion;
  if (readFormatVersion != namedResultCacheSerializer::detail::formatVersion &&
      readFormatVersion !=
          namedResultCacheSerializer::detail::legacyFormatVersion) {
    AD_THROW(absl::StrCat(
        "The serialized `NamedResultCache` has format version ",
        readFormatVersion,
        ", but this version of QLever only supports the format versions ",
        namedResultCacheSerializer::detail::legacyFormatVersion, " and ",
        namedResultCacheSerializer::detail::formatVersion,
        ". The named result cache was probably written by an incompatible "
        "version of QLever"));
  }

  // Deserialize the number of entries.
  size_t numEntries;
  serializer >> numEntries;

  // Deserialize each entry and add to the cache.
  for (size_t i = 0; i < numEntries; ++i) {
    // Deserialize the key
    Key key;
    serializer >> key;

    // Deserialize the value.
    Value value;
    value.allocatorForSerialization_ = allocator;
    value.contextForSerialization_ = &context;
    namedResultCacheSerializer::readValue(serializer, value, readFormatVersion);

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
// given to `writeEntries`, and must not be the legacy version if the `value`
// has a geo index.
CPP_template(typename Serializer, typename Columns)(
    requires ad_utility::serialization::WriteSerializer<
        Serializer>) void writeValue(Serializer& serializer,
                                     const NamedResultCache::Value& value,
                                     const Columns& columns,
                                     const std::vector<ColumnIndex>&
                                         resultSortedOn,
                                     bool writeLocalVocabWords,
                                     uint16_t entriesVersion =
                                         detail::formatVersion) {
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
    // don't work yet, and will only be mitigated in the future. The blob
    // writer (see `NamedCachedQueryBlobManager::serialize`) never triggers this
    // check: it always passes `writeLocalVocabWords=false` and `columns` in
    // which all such `Id`s have already been replaced by canonicalization (see
    // `canonicalizeWithPermutation`), so this check only applies to the `Id`s
    // that are actually written.
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
        ql::ranges::find(col, Datatype::LocalVocabIndex, &Id::getDatatype) ==
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
    AD_CONTRACT_CHECK(entriesVersion == detail::formatVersion,
                      "A geo index can only be written in the current format");
    value.cachedGeoIndex_.value().writeToSerializer(serializer);
  }
}
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
        /*writeLocalVocabWords=*/true);
  } else {
    namedResultCacheSerializer::readValue(
        serializer, arg, namedResultCacheSerializer::detail::formatVersion);
  }
}

}  // namespace ad_utility::serialization
#endif  // QLEVER_SRC_ENGINE_NAMEDRESULTCACHESERIALIZER_H
