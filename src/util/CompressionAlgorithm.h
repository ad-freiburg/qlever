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

namespace detail {
enum struct CompressionAlgorithmEnum : uint8_t { Zstd = 0, Lz4 = 1 };
}
// Wrapper class for the compression algorithm used for permutation data.
// Inherits string conversion, JSON serialization, etc. from `EnumWithStrings`.
class CompressionAlgorithm
    : public ad_utility::EnumWithStrings<CompressionAlgorithm,
                                         detail::CompressionAlgorithmEnum> {
 public:
  using Enum = detail::CompressionAlgorithmEnum;

  static const CompressionAlgorithm Zstd;
  static const CompressionAlgorithm Lz4;

  static constexpr std::array<std::pair<Enum, std::string_view>, 2>
      descriptions_{{{Enum::Zstd, "zstd"}, {Enum::Lz4, "lz4"}}};

  static constexpr std::string_view typeName() {
    return "compression algorithm";
  }

  using EnumWithStrings::EnumWithStrings;
};

inline const CompressionAlgorithm CompressionAlgorithm::Zstd{
    detail::CompressionAlgorithmEnum::Zstd};
inline const CompressionAlgorithm CompressionAlgorithm::Lz4{
    detail::CompressionAlgorithmEnum::Lz4};

#endif  // QLEVER_SRC_UTIL_COMPRESSIONALGORITHM_H
