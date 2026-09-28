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
#include <vector>

#include "engine/idTable/IdColumn.h"
#include "engine/idTable/IdRef.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace columnBasedIdTable {

// Bytes per packed `Id` (1 datatype byte + 8 payload bytes, no padding,
// unlike `sizeof(Id) == 16`). Code computing rows-per-block from a packed
// column's byte size must use this, not `sizeof(Id)`.
inline constexpr size_t BYTES_PER_ID_COLUMN_ENTRY =
    sizeof(uint64_t) + sizeof(uint8_t);

// Pack `column` into a buffer of `column.size() * BYTES_PER_ID_COLUMN_ENTRY`
// bytes: datatype byte then payload word per entry (native byte order),
// matching `Id::BitRepresentation`'s field order. Replaces what used to be a
// plain `Id*`/`sizeof(Id)` byte range, e.g. as compression input.
inline std::vector<char> packIdColumnToBytes(const ConstIdColumnRef& column) {
  std::vector<char> result(column.size() * BYTES_PER_ID_COLUMN_ENTRY);
  char* out = result.data();
  for (ConstIdRef ref : column) {
    auto [datatype_, payload_] = ref.getBits();
    std::memcpy(out, &datatype_, sizeof(datatype_));
    out += sizeof(datatype_);
    std::memcpy(out, &payload_, sizeof(payload_));
    out += sizeof(payload_);
  }
  return result;
}

// The inverse of `packIdColumnToBytes` above: unpack `bytes` into `column`.
// `bytes.size()` must be exactly `column.size() * BYTES_PER_ID_COLUMN_ENTRY`.
inline void unpackBytesToIdColumn(ql::span<const char> bytes,
                                  const IdColumnRef& column) {
  AD_CONTRACT_CHECK(bytes.size() == column.size() * BYTES_PER_ID_COLUMN_ENTRY);
  const char* in = bytes.data();
  for (auto&& i : column) {
    uint8_t datatype;
    uint64_t payload;
    std::memcpy(&datatype, in, sizeof(datatype));
    in += sizeof(datatype);
    std::memcpy(&payload, in, sizeof(payload));
    in += sizeof(payload);
    i = idFromBitsCompat({datatype, payload});
  }
}

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
