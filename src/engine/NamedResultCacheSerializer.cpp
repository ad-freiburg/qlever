// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/NamedResultCacheSerializer.h"

#include <absl/strings/str_cat.h>

#include <vector>

#include "engine/VariableToColumnMap.h"
#include "global/Id.h"
#include "util/BlankNodeManager.h"
#include "util/Exception.h"

namespace namedResultCacheSerializer {

// _____________________________________________________________________________
EntriesHeader parseEntriesHeader(
    ad_utility::serialization::LayoutReader& reader) {
  EntriesHeader header;
  header.whole_.begin_ = reader.position();
  auto readMagicByte = reader.readScalar<uint8_t>();
  if (readMagicByte != detail::magicByte) {
    AD_THROW(
        "The given input is not a serialized `NamedResultCache` (the magic "
        "byte does not match)");
  }
  header.version_ = reader.readScalar<uint16_t>();
  if (header.version_ != formatVersion &&
      header.version_ != legacyFormatVersion) {
    AD_THROW(absl::StrCat(
        "The serialized `NamedResultCache` has format version ",
        header.version_,
        ", but this version of QLever only supports the format versions ",
        legacyFormatVersion, " and ", formatVersion));
  }
  header.numEntries_ = reader.readScalar<size_t>();
  header.whole_.end_ = reader.position();
  return header;
}

// _____________________________________________________________________________
EntryLayout parseEntryLayout(ad_utility::serialization::LayoutReader& reader,
                             uint16_t entriesVersion) {
  AD_CONTRACT_CHECK(entriesVersion == formatVersion ||
                    entriesVersion == legacyFormatVersion);
  EntryLayout entry;
  entry.whole_.begin_ = reader.position();
  reader.reader() >> entry.key_;

  // The local vocab, as written by `ad_utility::detail::serializeLocalVocab`
  // (or `serializeOnlyBlankNodeBlocksFromLocalVocab`): the owned blank node
  // blocks, followed by the number of words and the words, each of which
  // consists of an `Id` and its string representation.
  reader.readScalar<std::vector<
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>>();
  auto numWords = reader.readScalar<uint64_t>();
  for (uint64_t i = 0; i < numWords; ++i) {
    reader.readScalar<Id::T>();
    reader.readArray<char>();
  }

  // The `IdTable`: its number of rows and columns, followed by the columns.
  entry.numRows_ = reader.readScalar<size_t>();
  entry.numColumns_ = reader.readScalar<size_t>();
  entry.columnPayloads_.reserve(entry.numColumns_);
  for (size_t i = 0; i < entry.numColumns_; ++i) {
    auto column = reader.readArray<Id>();
    AD_CONTRACT_CHECK(column.size() == entry.numRows_ * sizeof(Id),
                      "A column of a named cache entry has the wrong size");
    entry.columnPayloads_.push_back(column);
  }

  // The `VariableToColumnMap`, see `serializeDeterministically` in
  // `VariableToColumnMap.h`.
  entry.varToColMap_.begin_ = reader.position();
  {
    VariableToColumnMap scratch;
    serializeDeterministically(reader.reader(), scratch);
  }
  entry.varToColMap_.end_ = reader.position();

  // The `resultSortedOn_` (a vector of `ColumnIndex`), the cache key, and the
  // optional geo index.
  auto [sortedOnRange, sortedOn] = reader.readArrayWithContents<uint64_t>();
  entry.resultSortedOn_ = sortedOnRange;
  entry.sortedOnColumns_ = std::move(sortedOn);
  entry.cacheKey_ = reader.readArray<char>();
  entry.hasGeoIndex_ = reader.readScalar<bool>();
  if (entry.hasGeoIndex_) {
    entry.geo_ = SpatialJoinCachedIndex::parseLayout(reader, entry.numRows_,
                                                     entriesVersion);
  }
  entry.whole_.end_ = reader.position();
  return entry;
}

}  // namespace namedResultCacheSerializer
