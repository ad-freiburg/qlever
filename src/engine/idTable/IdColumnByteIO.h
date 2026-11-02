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

#include <cstddef>
#include <cstring>

#include "backports/memory.h"
#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Forward.h"

// The format of the bytes of a column of `Id`s on disk: in the permutations of
// the index (`CompressedRelationWriter` and `CompressedRelationReader`) and in
// the temporary files of the external sorter (`CompressedExternalIdTable.h`)
// and of the parallel merge (`CompressedIdTableBlocks.h`). It separates the
// bytes on disk from the layout of the `Id`s in memory. The compression is done
// by the callers.
//
// All formats have the same interface, so that `Format` at the bottom of this
// file can choose between them:
//
//   // The number of bytes of one `Id` on disk.
//   static constexpr size_t numBytesPerId;
//
//   // Call `useBytes` with the bytes of the `ids` as a `ql::span<const char>`
//   // and return its result. The bytes are only valid during the call.
//   static auto encode(ConstIdColumnRef ids, UseBytes&& useBytes);
//
//   // Call `fillBytes` with a `ql::span<char>`, which it has to fill
//   // completely (for example by decompressing into it), and store the `Id`s
//   // of these bytes in `ids`.
//   static void decode(IdColumnRef ids, FillBytes&& fillBytes);
//
// Both spans have `ids.size() * numBytesPerId` bytes. The callbacks let a
// format use the memory of the column itself, without a copy. When
// `IdColumnRef` becomes a view of separate payload and datatype arrays, the
// formats have to be adapted.
namespace ad_utility::idColumnByteIO {
// The bytes of the `Id`s as they are in memory, `sizeof(Id)` per `Id`. Both
// directions use the memory of the column directly.
struct LegacyFormat {
  static constexpr size_t numBytesPerId = sizeof(Id);

  template <typename UseBytes>
  static auto encode(ConstIdColumnRef ids, UseBytes&& useBytes) {
    return AD_FWD(useBytes)(ql::span{reinterpret_cast<const char*>(ids.data()),
                                     ids.size() * numBytesPerId});
  }

  template <typename FillBytes>
  static void decode(IdColumnRef ids, FillBytes&& fillBytes) {
    AD_FWD(fillBytes)
    (ql::span{reinterpret_cast<char*>(ids.data()), ids.size() * numBytesPerId});
  }
};

// The payloads of all the `Id`s, followed by their datatypes, both in native
// byte order. For the `SplitLayoutId` these are 9 instead of 16 bytes per `Id`.
// Both directions go through a buffer.
struct SplitFormat {
  static constexpr size_t numBytesPerId =
      sizeof(Id::PayloadType) + sizeof(Id::DatatypeType);

  template <typename UseBytes>
  static auto encode(const ConstIdColumnRef ids, UseBytes&& useBytes) {
    const size_t numIds = ids.size();
    const auto bytes =
        ql::make_unique_for_overwrite<char[]>(numIds * numBytesPerId);

    char* payloads = bytes.get();
    char* datatypes = payloads + numIds * sizeof(Id::PayloadType);
    for (size_t i = 0; i < numIds; ++i) {
      Id::PayloadType payload = ids[i].getPayloadBits();
      Id::DatatypeType datatype = ids[i].getDatatypeBits();
      std::memcpy(payloads + i * sizeof(Id::PayloadType), &payload,
                  sizeof(Id::PayloadType));
      std::memcpy(datatypes + i * sizeof(Id::DatatypeType), &datatype,
                  sizeof(Id::DatatypeType));
    }
    return AD_FWD(useBytes)(
        ql::span<const char>{bytes.get(), numIds * numBytesPerId});
  }

  template <typename FillBytes>
  static void decode(IdColumnRef ids, FillBytes&& fillBytes) {
    const size_t numIds = ids.size();
    const auto bytes =
        ql::make_unique_for_overwrite<char[]>(numIds * numBytesPerId);
    AD_FWD(fillBytes)(ql::span{bytes.get(), numIds * numBytesPerId});

    const char* payloads = bytes.get();
    const char* datatypes = payloads + numIds * sizeof(Id::PayloadType);
    for (size_t i = 0; i < numIds; ++i) {
      Id::PayloadType payload;
      Id::DatatypeType datatype;
      std::memcpy(&payload, payloads + i * sizeof(Id::PayloadType),
                  sizeof(Id::PayloadType));
      std::memcpy(&datatype, datatypes + i * sizeof(Id::DatatypeType),
                  sizeof(Id::DatatypeType));
      ids[i] = Id::fromBits(datatype, payload);
    }
  }
};

// The format of all the columns of `Id`s on disk. For the split layout, the
// `LegacyFormat` would also store the padding of each `Id`. A change of the
// format changes the index format, which then needs a new
// `qlever::indexFormatVersion`.
#ifdef QLEVER_USE_ID_SPLIT_LAYOUT
using CompressFormat = SplitFormat;
#else
using CompressFormat = LegacyFormat;
#endif
}  // namespace ad_utility::idColumnByteIO

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNBYTEIO_H
