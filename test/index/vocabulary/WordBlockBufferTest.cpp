// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary/WordBlockBuffer.h"

namespace {
using ad_utility::vocabulary::WordBlockBuffer;
using ad_utility::vocabulary::WordBlockBufferPool;

// Push all the `words` (alternating between `isExternal` being true and false)
// and check that they are stored correctly.
void testPushAndRead(
    WordBlockBuffer& buffer, const std::vector<std::string>& words,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  size_t totalSize = 0;
  for (size_t i = 0; i < words.size(); ++i) {
    buffer.push(words[i], i % 2 == 0);
    totalSize += words[i].size();
  }
  ASSERT_EQ(buffer.size(), words.size());
  EXPECT_EQ(buffer.totalWordBytes(), ad_utility::MemorySize::bytes(totalSize));
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(buffer.words()[i], words[i]);
    EXPECT_EQ(buffer.isExternal(i), i % 2 == 0);
  }
}
}  // namespace

// _____________________________________________________________________________
TEST(WordBlockBuffer, EmptyBuffer) {
  WordBlockBuffer buffer;
  EXPECT_TRUE(buffer.empty());
  EXPECT_EQ(buffer.size(), 0u);
  EXPECT_TRUE(buffer.words().empty());
  EXPECT_EQ(buffer.totalWordBytes(), ad_utility::MemorySize::bytes(0));
}

// _____________________________________________________________________________
TEST(WordBlockBuffer, PushAndRead) {
  WordBlockBuffer buffer;
  // Note: The empty word is a corner case, as the pointer that is stored for it
  // is never dereferenced.
  testPushAndRead(buffer, {"alpha", "", "beta", "gamma", ""});
  EXPECT_FALSE(buffer.empty());
}

// _____________________________________________________________________________
TEST(WordBlockBuffer, WordsSpanningSeveralChunks) {
  WordBlockBuffer buffer;
  // Enough words to fill more than two chunks.
  std::vector<std::string> words;
  size_t numWords = 3 * WordBlockBuffer::chunkSize() / 1000;
  for (size_t i = 0; i < numWords; ++i) {
    words.push_back(std::string(1000, static_cast<char>('a' + i % 26)));
  }
  testPushAndRead(buffer, words);
}

// _____________________________________________________________________________
TEST(WordBlockBuffer, WordLargerThanChunk) {
  WordBlockBuffer buffer;
  std::string large(2 * WordBlockBuffer::chunkSize() + 42, 'x');
  testPushAndRead(buffer, {"small", large, "alsoSmall", large});
}

// _____________________________________________________________________________
TEST(WordBlockBuffer, ClearKeepsTheMemory) {
  WordBlockBuffer buffer;
  // Fill more than a single chunk, such that several chunks are reused below.
  std::vector<std::string> words;
  for (size_t i = 0; i < 2 * WordBlockBuffer::chunkSize() / 1000; ++i) {
    words.push_back(std::string(1000, 'a'));
  }
  testPushAndRead(buffer, words);
  std::vector<const char*> pointers;
  for (const auto& word : buffer.words()) {
    pointers.push_back(word.data());
  }

  buffer.clear();
  EXPECT_TRUE(buffer.empty());
  EXPECT_EQ(buffer.totalWordBytes(), ad_utility::MemorySize::bytes(0));

  // The same words are stored at exactly the same addresses again, which means
  // that the chunks were reused and not allocated anew.
  testPushAndRead(buffer, words);
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(buffer.words()[i].data(), pointers[i]);
  }
}

// _____________________________________________________________________________
TEST(WordBlockBufferPool, ReuseOfBuffers) {
  WordBlockBufferPool pool{2};
  auto buffer = pool.acquire();
  ASSERT_NE(buffer, nullptr);
  EXPECT_TRUE(buffer->empty());
  buffer->push("alpha", false);
  const auto* rawPointer = buffer.get();

  pool.release(std::move(buffer));
  // The buffer is recycled, and it is empty again.
  auto buffer2 = pool.acquire();
  EXPECT_EQ(buffer2.get(), rawPointer);
  EXPECT_TRUE(buffer2->empty());

  // A buffer to which another reference is still held is not recycled.
  auto copy = buffer2;
  pool.release(std::move(buffer2));
  auto buffer3 = pool.acquire();
  EXPECT_NE(buffer3.get(), rawPointer);
  // The copy is unaffected by the failed release.
  EXPECT_TRUE(copy->empty());
}

// _____________________________________________________________________________
TEST(WordBlockBufferPool, MaximalNumberOfBuffers) {
  WordBlockBufferPool pool{1};
  auto buffer1 = pool.acquire();
  auto buffer2 = pool.acquire();
  EXPECT_NE(buffer1.get(), buffer2.get());
  const auto* rawPointer1 = buffer1.get();
  pool.release(std::move(buffer1));
  // The pool is already full, so this buffer is simply destroyed.
  pool.release(std::move(buffer2));

  auto recycled = pool.acquire();
  EXPECT_EQ(recycled.get(), rawPointer1);
  // The pool is empty again, so a fresh buffer is created. Note: We have to
  // keep the `recycled` buffer alive, as a new buffer might otherwise be
  // allocated at exactly the same address.
  auto fresh = pool.acquire();
  EXPECT_NE(fresh.get(), recycled.get());
}

// _____________________________________________________________________________
TEST(WordBlockBufferPool, ReleaseOfNullptr) {
  WordBlockBufferPool pool{1};
  // Releasing a `nullptr` is a no-op, in particular it doesn't crash and
  // doesn't add anything to the pool.
  pool.release(nullptr);
  EXPECT_NE(pool.acquire(), nullptr);
}
