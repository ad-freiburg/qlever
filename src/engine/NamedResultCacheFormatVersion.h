// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_NAMEDRESULTCACHEFORMATVERSION_H
#define QLEVER_SRC_ENGINE_NAMEDRESULTCACHEFORMATVERSION_H

#include <cstdint>

// NOTE: These constants live in a header of their own, because they are needed
// by both `NamedResultCacheSerializer.h` and `SpatialJoinCachedIndex.h`, and
// the former (indirectly) includes the latter.
namespace namedResultCacheSerializer {
// The version of the (de)serialization format implemented in
// `NamedResultCacheSerializer.h`. Increment this whenever the format changes in
// a way that is incompatible with previously serialized data, s.t.
// `readFromSerializer` can detect and reject data that was written by an
// incompatible version of QLever.
//
// Version 1 is the legacy format, whose geo index consists of the geometry
// column, one encoded S2 index, and a hash map from shape ids to rows. It can
// only represent a geo index with a single segment. Version 2 is the current
// format, whose geo index is the segmented one. Both versions are read by
// `readFromSerializer` and written by `writeEntries` (see
// `SpatialJoinCachedIndex::writeToSerializer` for the two formats of the geo
// index). All other parts of the two formats are identical.
constexpr uint16_t legacyFormatVersion = 1;
constexpr uint16_t formatVersion = 2;
}  // namespace namedResultCacheSerializer

#endif  // QLEVER_SRC_ENGINE_NAMEDRESULTCACHEFORMATVERSION_H
