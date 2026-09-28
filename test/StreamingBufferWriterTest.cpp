// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "util/StreamingBufferWriter.h"

using ad_utility::StreamingBufferWriter;

// The invariant-framework concept was removed from the tree, so verify the
// stateful-writer properties directly: the writer is nothrow-movable but
// never copied, so buffer ownership cannot alias, and the fence never throws.
static_assert(std::is_nothrow_move_constructible_v<StreamingBufferWriter>);
static_assert(std::is_nothrow_move_assignable_v<StreamingBufferWriter>);
static_assert(!std::is_copy_constructible_v<StreamingBufferWriter>);
static_assert(!std::is_copy_assignable_v<StreamingBufferWriter>);
static_assert(noexcept(StreamingBufferWriter::sfence()));

// _____________________________________________________________________________
TEST(StreamingBufferWriterTest, BasicStreamingWriteAndFlush) {
  constexpr size_t bufferSize = 1024;
  std::vector<char> rawBuffer(bufferSize, 0);

  StreamingBufferWriter writer(
      std::span<char>{rawBuffer.data(), rawBuffer.size()});
  EXPECT_EQ(writer.capacity(), bufferSize);
  EXPECT_EQ(writer.bytesWritten(), 0);
  EXPECT_TRUE(writer.empty());
  EXPECT_FALSE(writer.full());

  std::string chunk1 = "Hello, World! ";
  writer.write(chunk1);
  EXPECT_EQ(writer.bytesWritten(), chunk1.size());
  EXPECT_EQ(writer.remainingCapacity(), bufferSize - chunk1.size());

  std::string chunk2 = "This is a non-temporal streaming store test.";
  writer.write(chunk2);
  EXPECT_EQ(writer.bytesWritten(), chunk1.size() + chunk2.size());

  writer.flush();

  std::string fullResult(rawBuffer.data(), writer.bytesWritten());
  EXPECT_EQ(fullResult, chunk1 + chunk2);
}

// _____________________________________________________________________________
TEST(StreamingBufferWriterTest, OwningBufferConstructor) {
  constexpr size_t capacity = 512;
  StreamingBufferWriter writer(capacity);

  EXPECT_TRUE(writer.isOwner());
  EXPECT_EQ(writer.capacity(), capacity);
  EXPECT_EQ(writer.bytesWritten(), 0);

  std::string testPayload(256, 'X');
  writer.write(testPayload);
  writer.flush();

  EXPECT_EQ(writer.bytesWritten(), 256);
  EXPECT_EQ(std::string_view(writer.data(), writer.bytesWritten()),
            testPayload);
}

// _____________________________________________________________________________
TEST(StreamingBufferWriterTest, VariousSizesAndUnalignedOffsets) {
  // Test combinations of unaligned destination offsets and varied chunk sizes
  // across small (<16), medium (<64), and large (>64) boundaries.
  constexpr size_t totalBuffer = 4096;
  std::vector<char> destMemory(totalBuffer, 0);
  std::vector<char> srcMemory(totalBuffer);
  std::iota(srcMemory.begin(), srcMemory.end(), 1);

  const std::vector<size_t> startOffsets = {0,  1,  3,  7,  15, 16, 17,
                                            31, 32, 33, 63, 64, 65};
  const std::vector<size_t> testLengths = {
      0, 1, 2, 7, 15, 16, 17, 32, 63, 64, 65, 127, 128, 255, 512, 1024, 2048};

  for (size_t offset : startOffsets) {
    for (size_t len : testLengths) {
      if (offset + len > totalBuffer) {
        continue;
      }
      std::fill(destMemory.begin(), destMemory.end(), 0);

      StreamingBufferWriter writer(
          std::span<char>{destMemory.data() + offset, len});
      writer.write(srcMemory.data(), len);
      writer.flush();

      EXPECT_EQ(writer.bytesWritten(), len);
      EXPECT_TRUE(std::equal(srcMemory.begin(), srcMemory.begin() + len,
                             destMemory.begin() + offset));
    }
  }
}

// _____________________________________________________________________________
// Unaligned destinations with lengths that reach the 64-byte streaming loop
// must be copied correctly: the head copy aligns the write position to a
// cache-line boundary before any non-temporal store runs.
TEST(StreamingBufferWriterTest, UnalignedLargeCopyReachesStreamingLoop) {
  constexpr size_t totalBuffer = 4096;
  std::vector<char> destMemory(totalBuffer, 0);
  std::vector<char> srcMemory(totalBuffer);
  std::iota(srcMemory.begin(), srcMemory.end(), 1);

  // Offsets that are not even 16-byte aligned, with lengths that exceed one
  // 64-byte streaming block after the head alignment.
  for (size_t offset : {1u, 3u, 7u, 17u, 33u}) {
    for (size_t len : {64u, 65u, 127u, 512u, 2048u}) {
      ASSERT_LT(offset + len, totalBuffer);
      std::fill(destMemory.begin(), destMemory.end(), 0);

      StreamingBufferWriter::streamCopy(destMemory.data() + offset,
                                        srcMemory.data(), len);

      EXPECT_TRUE(std::equal(srcMemory.begin(), srcMemory.begin() + len,
                             destMemory.begin() + offset))
          << "offset " << offset << ", len " << len;
    }
  }
}

// _____________________________________________________________________________
TEST(StreamingBufferWriterTest, WriteAndFlush) {
  constexpr size_t bufferSize = 256;
  std::vector<char> rawBuffer(bufferSize, 0);
  StreamingBufferWriter writer(
      std::span<char>{rawBuffer.data(), rawBuffer.size()});

  std::string payload(128, 'Y');
  writer.writeAndFlush(payload.data(), payload.size());
  EXPECT_EQ(writer.bytesWritten(), payload.size());
  EXPECT_EQ(std::string_view(rawBuffer.data(), writer.bytesWritten()), payload);

  // Boundary inputs: empty writes are accepted and change nothing.
  writer.writeAndFlush("", 0);
  EXPECT_EQ(writer.bytesWritten(), payload.size());
  writer.writeAndFlush(std::string_view{});
  EXPECT_EQ(writer.bytesWritten(), payload.size());

  std::string tail(64, 'Z');
  writer.writeAndFlush(tail);
  EXPECT_EQ(writer.bytesWritten(), payload.size() + tail.size());
  EXPECT_EQ(std::string_view(rawBuffer.data(), writer.bytesWritten()),
            payload + tail);
}

// _____________________________________________________________________________
TEST(StreamingBufferWriterTest, StaticStreamCopy) {
  constexpr size_t testSize = 2048;
  std::vector<char> src(testSize);
  std::vector<char> dest(testSize, 0);

  for (size_t i = 0; i < testSize; ++i) {
    src[i] = static_cast<char>(i % 251);
  }

  StreamingBufferWriter::streamCopy(dest.data(), src.data(), testSize);
  EXPECT_EQ(src, dest);
}

// _____________________________________________________________________________
TEST(StreamingBufferWriterTest, BoundsAndContractChecks) {
  std::vector<char> buffer(64, 0);
  StreamingBufferWriter writer(std::span<char>{buffer.data(), buffer.size()});

  std::string valid32(32, 'A');
  writer.write(valid32);
  EXPECT_EQ(writer.bytesWritten(), 32);

  std::string tooBig(33, 'B');
  EXPECT_THROW(writer.write(tooBig), ad_utility::Exception);

  // Reset and verify reuse
  writer.reset();
  EXPECT_EQ(writer.bytesWritten(), 0);
  EXPECT_NO_THROW(writer.write(valid32));
}

// _____________________________________________________________________________
TEST(StreamingBufferWriterTest, MoveSemantics) {
  StreamingBufferWriter writer1(256);
  writer1.write("Hello Move");
  EXPECT_EQ(writer1.bytesWritten(), 10);

  StreamingBufferWriter writer2(std::move(writer1));
  EXPECT_EQ(writer2.bytesWritten(), 10);
  EXPECT_EQ(writer2.capacity(), 256);
  EXPECT_EQ(std::string_view(writer2.data(), 10), "Hello Move");

  // writer1 was moved from
  EXPECT_EQ(writer1.capacity(), 0);
  EXPECT_EQ(writer1.bytesWritten(), 0);
}
