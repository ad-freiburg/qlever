// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONALGORITHMENUM_H
#define QLEVER_SRC_UTIL_COMPRESSIONALGORITHMENUM_H

#include <cstdint>

// An enum for the different compression algorithms used inside QLever.
// For details see `CompressionAlgorithm.h`. This header can be included in
// places, where we only care about the enum, and not about the other
// functionality of `CompressionAlgorithm.h`.
namespace ad_utility {
enum struct CompressionAlgorithmEnum : uint8_t { Zstd = 0, Lz4 = 1 };
}

#endif
