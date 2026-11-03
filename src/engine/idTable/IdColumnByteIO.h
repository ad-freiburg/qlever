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

#include <cstring>

#include "backports/memory.h"
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
// `encode` calls `useBytes` with the bytes of the column (a
// `ql::span<const char>`) and returns its result. The bytes are only valid
// during the call. `decode` calls `fillBytes` with a `ql::span<char>` of that
// size, which it has to fill completely, and stores the bytes in the column.
namespace ad_utility::idColumnByteIO {

// A column of whole `Id`s: its bytes are the `Id`s in memory, without a copy.
template <typename UseBytes>
auto encode(ql::span<const Id> column, UseBytes&& useBytes) {
  return AD_FWD(useBytes)(
      ql::span<const char>{reinterpret_cast<const char*>(column.data()),
                           column.size() * sizeof(Id)});
}

template <typename FillBytes>
void decode(ql::span<Id> column, FillBytes&& fillBytes) {
  AD_FWD(fillBytes)
  (ql::span<char>{reinterpret_cast<char*>(column.data()),
                  column.size() * sizeof(Id)});
}

// A column of the split layout: the payloads of all the `Id`s, followed by
// their datatypes. The two arrays are copied into one buffer and back.
template <typename UseBytes>
auto encode(columnBasedIdTable::splitLayout::ConstIdColumnRef column,
            UseBytes&& useBytes) {
  const auto payloads = column.rawPayloads();
  const auto datatypes = column.rawDatatypes();
  const size_t numPayloadBytes = payloads.size() * sizeof(Id::PayloadType);
  const size_t numDatatypeBytes = datatypes.size() * sizeof(Id::DatatypeType);
  const auto bytes =
      ql::make_unique_for_overwrite<char[]>(numPayloadBytes + numDatatypeBytes);
  std::memcpy(bytes.get(), payloads.data(), numPayloadBytes);
  std::memcpy(bytes.get() + numPayloadBytes, datatypes.data(),
              numDatatypeBytes);
  return AD_FWD(useBytes)(
      ql::span<const char>{bytes.get(), numPayloadBytes + numDatatypeBytes});
}

template <typename FillBytes>
void decode(columnBasedIdTable::splitLayout::IdColumnRef column,
            FillBytes&& fillBytes) {
  const auto payloads = column.rawPayloads();
  const auto datatypes = column.rawDatatypes();
  const size_t numPayloadBytes = payloads.size() * sizeof(Id::PayloadType);
  const size_t numDatatypeBytes = datatypes.size() * sizeof(Id::DatatypeType);
  const auto bytes =
      ql::make_unique_for_overwrite<char[]>(numPayloadBytes + numDatatypeBytes);
  AD_FWD(fillBytes)
  (ql::span<char>{bytes.get(), numPayloadBytes + numDatatypeBytes});
  std::memcpy(payloads.data(), bytes.get(), numPayloadBytes);
  std::memcpy(datatypes.data(), bytes.get() + numPayloadBytes,
              numDatatypeBytes);
}

}  // namespace ad_utility::idColumnByteIO

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
