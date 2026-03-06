// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONALGORITHM_H
#define QLEVER_SRC_UTIL_COMPRESSIONALGORITHM_H

#include "util/EnumWithStrings.h"
#include "util/compression/CompressionAlgorithmEnum.h"
#include "util/compression/Lz4Wrapper.h"
#include "util/compression/ZstdWrapper.h"

namespace ad_utility {

// Wrapper class for the supported decompression algorithms (Currently
// `Zstandard` and `LZ4`) On the one hand, this class behaves like an enum with
// conversions to and from strings and JSON via the `EnumWithStrings` mixin. On
// the other hand it also has the functionality to directly compress and
// decompress data by using the `CompressionWrapper` that pertains to the
// current enum value.
class CompressionAlgorithm
    : public ad_utility::EnumWithStrings<CompressionAlgorithm,
                                         CompressionAlgorithmEnum> {
 public:
  using Enum = CompressionAlgorithmEnum;

  static const CompressionAlgorithm Zstd;
  static const CompressionAlgorithm Lz4;

  static constexpr std::array<std::pair<Enum, std::string_view>, 2>
      descriptions_{{{Enum::Zstd, "zstd"}, {Enum::Lz4, "lz4"}}};

  static constexpr std::string_view typeName() {
    return "compression algorithm";
  }

  using EnumWithStrings::EnumWithStrings;

 private:
  // Helper function that is used to implement the (de)-compression methods
  // below.
  template <typename F, typename... Args>
  decltype(auto) apply(F f, Args&&... args) const {
    auto doApply = [&]<typename T>() {
      return f.template operator()<T>(AD_FWD(args)...);
    };
    switch (value()) {
      case Enum::Zstd:
        return doApply.template operator()<ZstdWrapper>();
      case Enum::Lz4:
#ifdef QLEVER_HAS_LZ4
        return doApply.template operator()<Lz4Wrapper>();
#else
        throw std::runtime_error(
            "LZ4 compression was requested, but QLever was compiled without "
            "LZ4 support");
#endif
    }
  }

 public:
  // Forward the methods of the `CompressionWrappers`.

  // Compress `numBytes` starting at `src` using the current algorithm.
  std::vector<char> compress(const void* src, size_t numBytes) const {
    return apply([&]<typename T>() { return T::compress(src, numBytes); });
  }

  // Decompress to a `std::vector<T>`.
  template <typename T>
  std::vector<T> decompress(void* src, size_t numBytes,
                            size_t knownOriginalSize) const {
    return apply([&]<typename W>() {
      return W::template decompress<T>(src, numBytes, knownOriginalSize);
    });
  }

  // Decompress to a given buffer.
  template <typename T>
  size_t decompressToBuffer(const char* src, size_t numBytes, T* buffer,
                            size_t bufferCapacity) const {
    return apply([&]<typename W>() {
      return W::decompressToBuffer(src, numBytes, buffer, bufferCapacity);
    });
  }
};

inline const CompressionAlgorithm CompressionAlgorithm::Zstd{
    detail::CompressionAlgorithmEnum::Zstd};
inline const CompressionAlgorithm CompressionAlgorithm::Lz4{
    detail::CompressionAlgorithmEnum::Lz4};
}  // namespace ad_utility

// TODO<joka921> leave this in the ad_utility namespace....
using ad_utility::CompressionAlgorithm;

#endif  // QLEVER_SRC_UTIL_COMPRESSIONALGORITHM_H
