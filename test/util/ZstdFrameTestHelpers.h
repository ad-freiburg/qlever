// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_ZSTDFRAMETESTHELPERS_H
#define QLEVER_TEST_UTIL_ZSTDFRAMETESTHELPERS_H

#include <random>
#include <string>
#include <vector>

#include "util/compressionUsingZstd/ZstdWrapper.h"

// Helpers for the tests of `ZstdFrameHeader.h`, `ZstdFrameBlocks.h`, and
// `ZstdSkippableFrame.h`.
namespace zstdFrameTestHelpers {

using Bytes = std::vector<char>;

// Return compressible bytes that are deterministic for a given `seed` and large
// enough to be split into several blocks.
inline Bytes someData(size_t numBytes, unsigned seed) {
  std::mt19937 generator{seed};
  Bytes result;
  while (result.size() < numBytes) {
    auto word = std::to_string(generator() % 10000);
    result.insert(result.end(), word.begin(), word.end());
    result.push_back(static_cast<char>(generator() % 3));
  }
  result.resize(numBytes);
  return result;
}

// Compress `data` with ZSTD at level 1 (without a checksum).
inline Bytes compress(const Bytes& data) {
  return ZstdWrapper::compress(data.data(), data.size(), 1);
}

}  // namespace zstdFrameTestHelpers

#endif  // QLEVER_TEST_UTIL_ZSTDFRAMETESTHELPERS_H
