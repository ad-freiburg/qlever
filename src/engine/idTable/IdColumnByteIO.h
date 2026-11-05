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

#include "backports/concepts.h"
#include "backports/span.h"
#include "engine/idTable/splitLayout/IdColumn.h"
#include "global/Id.h"

// The bytes of a column of `Id`s on disk, in the permutations of the index and
// in the temporary files of the external sorter and of the parallel merge. The
// callers compress, write and read the bytes. There is one `encode` and one
// `decode` per type of column, so the bytes follow from the columns of
// `IdTable`. In both cases a column has `BYTES_PER_ID_COLUMN_ENTRY` bytes per
// `Id` on disk.
//
// The bytes of a column consist of one chunk per contiguous array in memory,
// so they are never copied. `encode` calls `useBytes` with the chunks (see
// `ConstChunks`) and returns its result. `decode` calls `fillBytes` with
// buffers of the same sizes (see `Chunks`), which it has to fill completely.
// The callers compress each chunk as a ZSTD frame of its own, see
// `ZstdWrapper::compress` and `ZstdWrapper::decompressToBuffers`.
namespace ad_utility::idColumnByteIO {

// The chunks of a column, one per contiguous array in memory.
using ConstChunks = ql::span<const ql::span<const std::byte>>;
using Chunks = ql::span<const ql::span<std::byte>>;

// A column of whole `Id`s: one chunk, the `Id`s in memory.
CPP_template(typename UseBytes)(
    requires ql::concepts::invocable<UseBytes, ConstChunks>)
auto encode(ql::span<const Id> column, UseBytes useBytes) {
  std::array chunks{ql::as_bytes(column)};
  return useBytes(ConstChunks{chunks});
}

CPP_template(typename FillBytes)(
    requires ql::concepts::invocable<FillBytes, Chunks>)
void decode(ql::span<Id> column, FillBytes fillBytes) {
  std::array chunks{ql::as_writable_bytes(column)};
  fillBytes(Chunks{chunks});
}

// A column of the split layout: two chunks, the payloads of all the `Id`s and
// then their datatypes.
CPP_template(typename UseBytes)(
    requires ql::concepts::invocable<UseBytes, ConstChunks>)
auto encode(columnBasedIdTable::splitLayout::ConstIdColumnRef column,
            UseBytes useBytes) {
  std::array chunks{ql::as_bytes(column.rawPayloads()),
                    ql::as_bytes(column.rawDatatypes())};
  return useBytes(ConstChunks{chunks});
}

CPP_template(typename FillBytes)(
    requires ql::concepts::invocable<FillBytes, Chunks>)
void decode(columnBasedIdTable::splitLayout::IdColumnRef column,
            FillBytes fillBytes) {
  std::array chunks{ql::as_writable_bytes(column.rawPayloads()),
                    ql::as_writable_bytes(column.rawDatatypes())};
  fillBytes(Chunks{chunks});
}
}  // namespace ad_utility::idColumnByteIO

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
