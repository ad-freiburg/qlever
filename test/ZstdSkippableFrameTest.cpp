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

namespace {
// The magic variants that are used in the tests.
constexpr auto variant0 = SkippableMagicVariant::make<0>();
constexpr auto variant4 = SkippableMagicVariant::make<4>();
constexpr auto variant5 = SkippableMagicVariant::make<5>();
constexpr auto variant15 = SkippableMagicVariant::make<15>();
static_assert(variant15.value() == 15);
}  // namespace

// A trailing skippable frame is ignored by ZSTD and can be found from the end.
TEST(ZstdSkippableFrame, trailingSkippableFrame) {
  auto data = someData(1000, 3);
  auto blob = compress(data);
  const size_t frameSize = blob.size();
  std::string content = "some content";
  ASSERT_TRUE(hasValue(appendTrailingSkippableFrame(blob, variant5, content)));
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

  auto read = readTrailingSkippableFrame(blob, variant5);
  ASSERT_TRUE(hasValue(read));
  EXPECT_EQ(std::string(read->begin(), read->end()), content);
  // An empty content.
  Bytes empty;
  ASSERT_TRUE(hasValue(appendTrailingSkippableFrame(empty, variant0, {})));
  EXPECT_EQ(readTrailingSkippableFrame(empty, variant0)->size(), 0u);
  // The largest magic variant.
  Bytes last;
  ASSERT_TRUE(hasValue(appendTrailingSkippableFrame(last, variant15, content)));
  EXPECT_EQ(readTrailingSkippableFrame(last, variant15)->size(),
            content.size());
}

// Data that does not end in a matching trailing skippable frame is rejected.
TEST(ZstdSkippableFrame, invalidTrailingSkippableFrame) {
  auto data = someData(1000, 3);
  auto blob = compress(data);
  const size_t frameSize = blob.size();
  std::string content = "some content";
  ASSERT_TRUE(hasValue(appendTrailingSkippableFrame(blob, variant5, content)));
  // Wrong magic variant, no frame, empty, or truncated.
  EXPECT_TRUE(
      isError(readTrailingSkippableFrame(blob, variant4), "magic variant 4"));
  EXPECT_TRUE(isError(readTrailingSkippableFrame(compress(data), variant5)));
  EXPECT_TRUE(
      isError(readTrailingSkippableFrame(Bytes{}, variant5), "too short"));
  EXPECT_TRUE(
      isError(readTrailingSkippableFrame(
                  ql::span<const char>{blob}.first(blob.size() - 1), variant5),
              "invalid trailing frame size"));
  // A corrupted content size in the header.
  auto corrupted = blob;
  corrupted[frameSize + 4] ^= 1;
  EXPECT_TRUE(isError(readTrailingSkippableFrame(corrupted, variant5),
                      "does not match its total size"));
  // A corrupted trailing total size (too large, and too small).
  corrupted = blob;
  corrupted.back() = 0x7F;
  EXPECT_TRUE(isError(readTrailingSkippableFrame(corrupted, variant5),
                      "invalid trailing frame size"));
  corrupted = blob;
  corrupted[corrupted.size() - 4] = 3;
  EXPECT_TRUE(isError(readTrailingSkippableFrame(corrupted, variant5),
                      "invalid trailing frame size"));
}
