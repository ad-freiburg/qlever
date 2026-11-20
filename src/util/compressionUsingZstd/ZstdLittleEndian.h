// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDLITTLEENDIAN_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDLITTLEENDIAN_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "util/serializer/ByteBufferSerializer.h"
#include "util/serializer/FromCallableSerializer.h"

// Low-level helpers to read and write the integers of the ZSTD frame format
// (see `ZstdFrameHeader.h`), which are shared by `ZstdFrameHeader.cpp`,
// `ZstdFrameBlocks.cpp`, and `ZstdSkippableFrame.cpp`. All integers of the
// frame format are little endian, which is the native byte order on all
// platforms that QLever currently supports, so they are simply (de)serialized
// via their in-memory representation.
namespace ad_utility::zstd::detail {

// Return a serializer that appends the bytes of everything that is written to
// it to the end of `out`.
inline auto appendingSerializer(std::vector<char>& out) {
  return serialization::WriteViaCallableSerializer{
      [&out](const char* bytes, size_t numBytes) {
        out.insert(out.end(), bytes, bytes + numBytes);
      }};
}

// Read the `T` that is stored in the first `sizeof(T)` bytes of `bytes`, which
// has to be at least that large.
template <typename T>
T readValue(ql::span<const char> bytes) {
  serialization::ByteBufferReadSerializerT reader{bytes.first(sizeof(T))};
  T value;
  reader >> value;
  return value;
}

// Return the byte at `bytes[i]` as an unsigned integer (`char` may be signed,
// so a plain conversion would sign-extend bytes >= 128).
inline uint32_t byteAt(ql::span<const char> bytes, size_t i) {
  return static_cast<uint8_t>(bytes[i]);
}

}  // namespace ad_utility::zstd::detail

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDLITTLEENDIAN_H
