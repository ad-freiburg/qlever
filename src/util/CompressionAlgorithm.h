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

// Wrapper class for the compression algorithm used for permutation data.
// Inherits string conversion, JSON serialization, etc. from `EnumWithStrings`.
class CompressionAlgorithm
    : public ad_utility::EnumWithStrings<CompressionAlgorithm> {
 public:
  enum struct Enum : uint8_t { Zstd = 0, Lz4 = 1 };

  static constexpr size_t numValues_ = 2;
  static constexpr std::array<Enum, numValues_> all_{Enum::Zstd, Enum::Lz4};
  static constexpr std::array<std::string_view, numValues_> descriptions_{
      "zstd", "lz4"};

  static constexpr std::string_view typeName() {
    return "compression algorithm";
  }

  using EnumWithStrings::EnumWithStrings;
};

#endif  // QLEVER_SRC_UTIL_COMPRESSIONALGORITHM_H
