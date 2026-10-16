// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/NamedResultCacheSerializer.h"

#include "util/Exception.h"

namespace namedResultCacheSerializer {

// _____________________________________________________________________________
EntriesHeader parseEntriesHeader(
    ad_utility::serialization::LayoutReadSerializer& reader) {
  EntriesHeader header;
  header.whole_ =
      reader.recordRange([&] { header = readEntriesHeader(reader); });
  return header;
}

// _____________________________________________________________________________
EntryLayout parseEntryLayout(
    ad_utility::serialization::LayoutReadSerializer& reader,
    uint16_t entriesVersion) {
  AD_CONTRACT_CHECK(entriesVersion == formatVersion ||
                    entriesVersion == legacyFormatVersion);
  EntryLayout entry;
  entry.whole_ = reader.recordRange([&] {
    reader >> entry.key_;
    readValue(reader, entry, entriesVersion);
  });
  return entry;
}

}  // namespace namedResultCacheSerializer
