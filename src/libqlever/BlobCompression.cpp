// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/BlobCompression.h"

#include "util/compressionUsingZstd/SegmentedZstdFrame.h"
#include "util/compressionUsingZstd/ZstdWrapper.h"

namespace qlever::blobCompression {

// _____________________________________________________________________________
std::vector<char> compressBlob(ql::span<const char> uncompressedBlob,
                               const BlobEncoding& encoding,
                               ql::span<const char> reusableBase) {
  if (!encoding.segmentSizes_.has_value()) {
    auto frame = ZstdWrapper::compressToUninitializedBuffer(
        uncompressedBlob, encoding.compressionLevel_);
    return {frame.begin(), frame.end()};
  }
  auto base = ad_utility::zstd::SegmentedFrame::read(reusableBase);
  return ad_utility::zstd::compressSegmented(
      uncompressedBlob, encoding.segmentSizes_.value(),
      encoding.compressionLevel_, base.has_value() ? &base.value() : nullptr);
}

}  // namespace qlever::blobCompression
