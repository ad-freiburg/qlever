// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <zstd.h>

#include <string>
#include <vector>

#include "util/ZstdFrameTestHelpers.h"
#include "util/compressionUsingZstd/ZstdSkippableFrame.h"

using namespace ad_utility::zstd;
using namespace zstdFrameTestHelpers;

// A trailing skippable frame is ignored by ZSTD and can be found from the end.
TEST(ZstdSkippableFrame, trailingSkippableFrame) {
  auto data = someData(1000, 3);
  auto blob = compress(data);
  const size_t frameSize = blob.size();
  std::string content = "some content";
  appendTrailingSkippableFrame(blob, 5, content);
  EXPECT_EQ(blob.size(), frameSize + skippableFrameHeaderSize + content.size() +
                             sizeof(uint32_t));
  // Every decompressor ignores the skippable frame.
  Bytes decompressed(data.size());
  EXPECT_EQ(ZSTD_decompress(decompressed.data(), decompressed.size(),
                            blob.data(), blob.size()),
            data.size());
  EXPECT_EQ(decompressed, data);
  EXPECT_EQ(ZSTD_findFrameCompressedSize(blob.data(), blob.size()), frameSize);
  // ZSTD also sees the trailing size as part of the skippable frame.
  auto skippable = ql::span<const char>{blob}.subspan(frameSize);
  EXPECT_EQ(ZSTD_findFrameCompressedSize(skippable.data(), skippable.size()),
            skippable.size());

  auto read = readTrailingSkippableFrame(blob, 5);
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(std::string(read->begin(), read->end()), content);
  // An empty content.
  Bytes empty;
  appendTrailingSkippableFrame(empty, 0, {});
  EXPECT_EQ(readTrailingSkippableFrame(empty, 0)->size(), 0u);
  // The largest magic variant.
  Bytes last;
  appendTrailingSkippableFrame(last, 15, content);
  EXPECT_EQ(readTrailingSkippableFrame(last, 15)->size(), content.size());
}

// Data that does not end in a matching trailing skippable frame is rejected.
TEST(ZstdSkippableFrame, invalidTrailingSkippableFrame) {
  auto data = someData(1000, 3);
  auto blob = compress(data);
  const size_t frameSize = blob.size();
  std::string content = "some content";
  appendTrailingSkippableFrame(blob, 5, content);
  // Wrong magic variant, no frame, empty, or truncated.
  EXPECT_FALSE(readTrailingSkippableFrame(blob, 4));
  EXPECT_FALSE(readTrailingSkippableFrame(compress(data), 5));
  EXPECT_FALSE(readTrailingSkippableFrame(Bytes{}, 5));
  EXPECT_FALSE(readTrailingSkippableFrame(
      ql::span<const char>{blob}.first(blob.size() - 1), 5));
  // A corrupted content size in the header.
  auto corrupted = blob;
  corrupted[frameSize + 4] ^= 1;
  EXPECT_FALSE(readTrailingSkippableFrame(corrupted, 5));
  // A corrupted trailing total size (too large, and too small).
  corrupted = blob;
  corrupted.back() = 0x7F;
  EXPECT_FALSE(readTrailingSkippableFrame(corrupted, 5));
  corrupted = blob;
  corrupted[corrupted.size() - 4] = 3;
  EXPECT_FALSE(readTrailingSkippableFrame(corrupted, 5));
  // Only 16 magic variants exist.
  EXPECT_ANY_THROW(appendTrailingSkippableFrame(blob, 16, content));
}
