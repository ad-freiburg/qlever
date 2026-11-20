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

#include <gtest/gtest.h>
#include <zstd.h>

#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "util/compressionUsingZstd/ZstdExpected.h"
#include "util/compressionUsingZstd/ZstdWrapper.h"

// Helpers for the tests of `ZstdFrameHeader.h`, `ZstdFrameBlocks.h`,
// `ZstdSkippableFrame.h`, and the modules that build on them.
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

// Compress `data` with ZSTD at level 1 into a frame with a checksum.
inline Bytes compressWithChecksum(const Bytes& data) {
  ZSTD_CCtx* context = ZSTD_createCCtx();
  ZSTD_CCtx_setParameter(context, ZSTD_c_compressionLevel, 1);
  ZSTD_CCtx_setParameter(context, ZSTD_c_checksumFlag, 1);
  Bytes result(ZSTD_compressBound(data.size()));
  size_t size = ZSTD_compress2(context, result.data(), result.size(),
                               data.data(), data.size());
  ZSTD_freeCCtx(context);
  EXPECT_FALSE(ZSTD_isError(size));
  result.resize(size);
  return result;
}

// Return success iff the `expected` holds a value. On failure, report the
// message of the error.
template <typename T>
::testing::AssertionResult hasValue(
    const ad_utility::zstd::Expected<T>& expected) {
  if (expected.hasValue()) {
    return ::testing::AssertionSuccess();
  }
  return ::testing::AssertionFailure()
         << "unexpected error: " << expected.error().message_;
}

// Return success iff the `expected` holds an error whose message contains the
// `substring`.
template <typename T>
::testing::AssertionResult isError(
    const ad_utility::zstd::Expected<T>& expected,
    std::string_view substring = "") {
  if (expected.hasValue()) {
    return ::testing::AssertionFailure()
           << "expected an error, but got a value";
  }
  if (expected.error().message_.find(substring) == std::string::npos) {
    return ::testing::AssertionFailure()
           << "the error message \"" << expected.error().message_
           << "\" does not contain \"" << substring << "\"";
  }
  return ::testing::AssertionSuccess();
}

}  // namespace zstdFrameTestHelpers

#endif  // QLEVER_TEST_UTIL_ZSTDFRAMETESTHELPERS_H
