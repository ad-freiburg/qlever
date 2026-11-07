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

// The bytes of a column as views into its memory, one span per contiguous
// array (see `chunksOf`), so that they can be compressed without a copy.
using ReadOnlyChunks = ql::span<const ql::span<const std::byte>>;
using WritableChunks = ql::span<const ql::span<std::byte>>;

// A column of whole `Id`s is a single array, so its bytes are the `Id`s exactly
// as they are in memory.
inline auto chunksOf(ql::span<const Id> column) {
  return std::array{ql::as_bytes(column)};
}
inline auto chunksOf(ql::span<Id> column) {
  return std::array{ql::as_writable_bytes(column)};
}

// A column of the split layout keeps the payloads and the datatypes in two
// arrays, so its bytes are all the payloads followed by all the datatypes.
inline auto chunksOf(columnBasedIdTable::splitLayout::ConstIdColumnRef column) {
  return std::array{ql::as_bytes(column.rawPayloads()),
                    ql::as_bytes(column.rawDatatypes())};
}
inline auto chunksOf(columnBasedIdTable::splitLayout::IdColumnRef column) {
  return std::array{ql::as_writable_bytes(column.rawPayloads()),
                    ql::as_writable_bytes(column.rawDatatypes())};
}

CPP_template(typename Column, typename UseBytes)(
    requires ql::concepts::invocable<UseBytes, ReadOnlyChunks>)
auto encode(const Column& source, UseBytes useBytes) {
  return useBytes(ReadOnlyChunks{chunksOf(source)});
}

CPP_template(typename Column, typename FillBytes)(
    requires ql::concepts::invocable<FillBytes, WritableChunks>)
void decode(const Column& target, FillBytes fillBytes) {
  fillBytes(WritableChunks{chunksOf(target)});
}
}  // namespace ad_utility::idColumnByteIO

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
