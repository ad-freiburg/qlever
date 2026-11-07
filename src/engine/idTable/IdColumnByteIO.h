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

// Helper functions to encode and decode a column of an `IdTable`, for both
// layouts of an `Id` (see `global/FoldedValueId.h` and
// `engine/idTable/splitLayout/SplitLayoutId.h`). They are mostly used to
// compress the columns for storing them on disk, see `CompressedRelation*.cpp`,
// `CompressedExternalIdTable.h` and `CompressedIdTableBlocks.h`.
namespace ad_utility::idColumnByteIO {

// The chunks of a column, one per contiguous array in memory.
using ReadOnlyChunks = ql::span<const ql::span<const std::byte>>;
using WritableChunks = ql::span<const ql::span<std::byte>>;

// A column of whole `Id`s: one chunk, the `Id`s in memory.
CPP_template(typename UseBytes)(
    requires ql::concepts::invocable<UseBytes, ReadOnlyChunks>)
auto encode(ql::span<const Id> source, UseBytes useBytes) {
  std::array chunks{ql::as_bytes(source)};
  return useBytes(ReadOnlyChunks{chunks});
}

CPP_template(typename FillBytes)(
    requires ql::concepts::invocable<FillBytes, WritableChunks>)
void decode(ql::span<Id> target, FillBytes fillBytes) {
  std::array chunks{ql::as_writable_bytes(target)};
  fillBytes(WritableChunks{chunks});
}

// A column of the split layout: two chunks, the payloads of all the `Id`s and
// then their datatypes.
CPP_template(typename UseBytes)(
    requires ql::concepts::invocable<UseBytes, ReadOnlyChunks>)
auto encode(columnBasedIdTable::splitLayout::ConstIdColumnRef source,
            UseBytes useBytes) {
  std::array chunks{ql::as_bytes(source.rawPayloads()),
                    ql::as_bytes(source.rawDatatypes())};
  return useBytes(ReadOnlyChunks{chunks});
}

CPP_template(typename FillBytes)(
    requires ql::concepts::invocable<FillBytes, WritableChunks>)
void decode(columnBasedIdTable::splitLayout::IdColumnRef target,
            FillBytes fillBytes) {
  std::array chunks{ql::as_writable_bytes(target.rawPayloads()),
                    ql::as_writable_bytes(target.rawDatatypes())};
  fillBytes(WritableChunks{chunks});
}
}  // namespace ad_utility::idColumnByteIO

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
