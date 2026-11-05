// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H

#include <array>
#include <cstddef>

#include "backports/span.h"
#include "engine/idTable/splitLayout/IdColumn.h"
#include "global/Id.h"
#include "util/Forward.h"

// The bytes of a column of `Id`s on disk, in the permutations of the index and
// in the temporary files of the external sorter and of the parallel merge. The
// callers compress, write and read the bytes. There is one `encode` and one
// `decode` per type of column, so the bytes follow from the columns of
// `IdTable`. In both cases a column has `BYTES_PER_ID_COLUMN_ENTRY` bytes per
// `Id` on disk.
//
// The bytes of a column consist of one chunk per contiguous array in memory,
// so they are never copied. `encode` calls `useBytes` with a `std::array` of
// `ql::span<const std::byte>`, one per chunk, and returns its result. `decode`
// calls `fillBytes` with a `std::array` of `ql::span<std::byte>` of the same
// sizes, which it has to fill completely. The callers compress each chunk as a
// ZSTD frame of its own, see `ZstdWrapper::compress` and
// `ZstdWrapper::decompressToBuffers`.
namespace ad_utility::idColumnByteIO {

// A column of whole `Id`s: one chunk, the `Id`s in memory.
template <typename UseBytes>
auto encode(ql::span<const Id> column, UseBytes&& useBytes) {
  return AD_FWD(useBytes)(std::array{ql::as_bytes(column)});
}

template <typename FillBytes>
void decode(ql::span<Id> column, FillBytes&& fillBytes) {
  AD_FWD(fillBytes)(std::array{ql::as_writable_bytes(column)});
}

// A column of the split layout: two chunks, the payloads of all the `Id`s and
// then their datatypes.
template <typename UseBytes>
auto encode(columnBasedIdTable::splitLayout::ConstIdColumnRef column,
            UseBytes&& useBytes) {
  return AD_FWD(useBytes)(std::array{ql::as_bytes(column.rawPayloads()),
                                     ql::as_bytes(column.rawDatatypes())});
}

template <typename FillBytes>
void decode(columnBasedIdTable::splitLayout::IdColumnRef column,
            FillBytes&& fillBytes) {
  AD_FWD(fillBytes)
  (std::array{ql::as_writable_bytes(column.rawPayloads()),
              ql::as_writable_bytes(column.rawDatatypes())});
}
}  // namespace ad_utility::idColumnByteIO

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
