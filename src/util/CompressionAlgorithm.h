// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_COMPRESSIONALGORITHM_H
#define QLEVER_SRC_UTIL_COMPRESSIONALGORITHM_H

#include <cstdint>
#include <stdexcept>
#include <string_view>

#include "util/json.h"

// Enum for the compression algorithm used for permutation data.
enum class CompressionAlgorithm : uint8_t { Zstd = 0, Lz4 = 1 };

// Convert a `CompressionAlgorithm` to its string representation.
inline std::string_view toString(CompressionAlgorithm algo) {
  switch (algo) {
    case CompressionAlgorithm::Zstd:
      return "zstd";
    case CompressionAlgorithm::Lz4:
      return "lz4";
  }
  throw std::runtime_error("Unknown CompressionAlgorithm value.");
}

// Parse a `CompressionAlgorithm` from a string. Throws on unknown input.
inline CompressionAlgorithm compressionAlgorithmFromString(
    std::string_view str) {
  if (str == "zstd") {
    return CompressionAlgorithm::Zstd;
  } else if (str == "lz4") {
    return CompressionAlgorithm::Lz4;
  }
  throw std::runtime_error("Unknown compression algorithm: \"" +
                           std::string(str) +
                           "\". Supported values are \"zstd\" and \"lz4\".");
}

// JSON serialization for `CompressionAlgorithm`.
inline void to_json(nlohmann::json& j, CompressionAlgorithm algo) {
  j = toString(algo);
}

inline void from_json(const nlohmann::json& j, CompressionAlgorithm& algo) {
  algo = compressionAlgorithmFromString(j.get<std::string>());
}

#endif  // QLEVER_SRC_UTIL_COMPRESSIONALGORITHM_H
