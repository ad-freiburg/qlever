// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_ALLOCATIONLIMITEDREADSERIALIZER_H
#define QLEVER_SRC_UTIL_SERIALIZER_ALLOCATIONLIMITEDREADSERIALIZER_H

#include <absl/strings/str_cat.h>

#include <cstddef>

#include "util/NoCopyNoMove.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility::serialization {

// A `ReadSerializer` that reads from the `UnderlyingSerializer`, but throws
// as soon as the values that it reads would allocate more than a given
// maximal number of bytes in total (see `checkAllocation` in `Serializer.h`).
// This protects against allocating huge amounts of memory for invalid input,
// for example a huge size of a `std::string` in a corrupted or malicious
// network response. The check happens before the memory is allocated.
//
// NOTE: Only the allocations of the `serialize` functions that call
// `checkAllocation` (currently those of `std::vector` and `std::string`) are
// counted.
CPP_template(typename UnderlyingSerializer)(
    requires ReadSerializer<UnderlyingSerializer>)
class AllocationLimitedReadSerializer : public NoCopyNoMove {
 public:
  using SerializerType = ReadSerializerTag;
  static_assert(!usesAlignedSerialization<UnderlyingSerializer>);

 private:
  UnderlyingSerializer& underlyingSerializer_;
  size_t maxNumBytes_;
  size_t remainingNumBytes_;

 public:
  AllocationLimitedReadSerializer(UnderlyingSerializer& underlyingSerializer,
                                  size_t maxNumBytes)
      : underlyingSerializer_{underlyingSerializer},
        maxNumBytes_{maxNumBytes},
        remainingNumBytes_{maxNumBytes} {}

  // Read from the underlying serializer.
  void serializeBytes(char* bytePointer, size_t numBytes) {
    underlyingSerializer_.serializeBytes(bytePointer, numBytes);
  }

  // Throw if the allocation of `numElements` elements of `elementSize` bytes
  // each exceeds the remaining number of bytes, otherwise subtract it.
  void checkAllocation(size_t numElements, size_t elementSize) {
    // NOTE: Dividing instead of multiplying avoids an overflow.
    if (elementSize != 0 && numElements > remainingNumBytes_ / elementSize) {
      throw SerializationException{absl::StrCat(
          "Reading the serialized data would allocate more than the maximum "
          "of ",
          maxNumBytes_, " bytes")};
    }
    remainingNumBytes_ -= numElements * elementSize;
  }
};

// Read a value of type `T` from the `serializer` (see `read` in
// `Serializer.h`). Throw if reading it would allocate more than `maxNumBytes`,
// see `AllocationLimitedReadSerializer` for details.
CPP_template(typename T, typename S)(requires ReadSerializer<S>)
T readWithAllocationLimit(S& serializer, size_t maxNumBytes) {
  AllocationLimitedReadSerializer<S> limitedSerializer{serializer, maxNumBytes};
  return read<T>(limitedSerializer);
}

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_ALLOCATIONLIMITEDREADSERIALIZER_H
