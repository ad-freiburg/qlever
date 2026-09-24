// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H

#include <cstring>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdColumn.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace columnBasedIdTable {

// Bytes per packed `Id` (1 datatype byte + 8 payload bytes, no padding,
// unlike `sizeof(Id) == 16`). Code computing rows-per-block from a packed
// column's byte size must use this, not `sizeof(Id)`.
inline constexpr size_t BYTES_PER_ID_COLUMN_ENTRY =
    sizeof(uint64_t) + sizeof(uint8_t);

// Pack `column` (any range of `Id`, `IdRef`, or `ConstIdRef` -- an
// `IdColumn`/`ConstIdColumn`, but also a plain `std::vector<Id>`/
// `ql::span<Id>`) into a buffer of `column.size() * BYTES_PER_ID_COLUMN_ENTRY`
// bytes: datatype byte then payload word per entry (native byte order),
// matching `ValueIdBitRepresentation`'s field order and every
// `Id::fromBits({datatype, payload})` call site. Replaces what used to be a
// plain `Id*`/`sizeof(Id)` byte range, e.g. as compression input.
template <typename Range>
inline std::vector<char> packIdColumnToBytes(const Range& column) {
  std::vector<char> result(ql::ranges::size(column) *
                           BYTES_PER_ID_COLUMN_ENTRY);
  char* out = result.data();
  for (const auto& element : column) {
    auto bits = element.getBits();
    std::memcpy(out, &bits.datatype_, sizeof(bits.datatype_));
    out += sizeof(bits.datatype_);
    std::memcpy(out, &bits.payload_, sizeof(bits.payload_));
    out += sizeof(bits.payload_);
  }
  return result;
}

// The inverse of `packIdColumnToBytes` above: unpack `bytes` into `column`.
// `bytes.size()` must be exactly `column.size() * BYTES_PER_ID_COLUMN_ENTRY`.
inline void unpackBytesToIdColumn(ql::span<const char> bytes,
                                  const IdColumn& column) {
  AD_CONTRACT_CHECK(bytes.size() ==
                    column.size() * BYTES_PER_ID_COLUMN_ENTRY);
  const char* in = bytes.data();
  for (auto && i : column) {
    uint8_t datatype;
    uint64_t payload;
    std::memcpy(&datatype, in, sizeof(datatype));
    in += sizeof(datatype);
    std::memcpy(&payload, in, sizeof(payload));
    in += sizeof(payload);
    i = Id::fromBits({datatype, payload});
  }
}

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
