// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "VocabularyMergerTestHelpers.h"
#include "index/vocabulary_merger/AsyncBlockWriter.h"

using namespace ad_utility::vocabulary_merger;
using namespace ad_utility::vocabulary_merger::detail;
using namespace vocabularyMergerTestHelpers;
using ::testing::Pair;

namespace {
// The block that consists of all the words of the sub-vocabulary `0` of the
// `segment`, at the position `firstPosition`.
BlockPieces blockOf(std::shared_ptr<const Segment> segment,
                    uint64_t firstPosition) {
  size_t numWords = segment->wordBlocks_[0].numWords();
  return BlockPieces{{SegmentPiece{std::move(segment), 0, 0, numWords}},
                     numWords,
                     firstPosition};
}

// A `BlockWriterBase` whose `prepare` throws for the block at position `0`,
// and whose `AppendBlock` only counts the appended blocks in `numAppended`.
class ThrowingPrepareBlockWriter : public BlockWriterBase {
 private:
  std::atomic<size_t>& numAppended_;

 public:
  explicit ThrowingPrepareBlockWriter(std::atomic<size_t>& numAppended)
      : numAppended_{numAppended} {}

  size_t blockSize() const override {
    return DEFAULT_WORDS_PER_VOCABULARY_BLOCK;
  }

  AppendBlock prepare(WordBlock block) override {
    if (block.firstPosition_ == 0) {
      throw std::runtime_error{"The block could not be prepared"};
    }
    return [this]() { ++numAppended_; };
  }

 private:
  void finishImpl() override {}
};
}  // namespace

// Test that the blocks are appended in order (the `BlockWriterFromCallback`
// checks that the positions are consecutive), although they are prepared
// concurrently.
TEST(AsyncBlockWriter, blocksAreAppendedInOrder) {
  std::vector<std::pair<std::string, bool>> words;
  auto writer = makeParallelWriter(makeCollectingWordCallback(words));
  std::vector<std::pair<std::string, bool>> expected;
  {
    AsyncBlockWriter blockWriter{writer};
    for (size_t i = 0; i < 50; ++i) {
      std::string word = absl::StrFormat("\"w%02d\"", i);
      expected.emplace_back(word, true);
      blockWriter.push(0, blockOf(makeSegment({word}, 0, 1, writer), i));
    }
    EXPECT_EQ(blockWriter.numBlocks(), 50u);
    EXPECT_FALSE(blockWriter.hasFailed());
    blockWriter.finish();
  }
  writer.finish();
  EXPECT_EQ(words, expected);
}

// Test that an exception from the block writer (thrown on the appending task)
// is rethrown by `finish()`, and that the remaining blocks are skipped.
TEST(AsyncBlockWriter, exceptionIsPropagated) {
  size_t numCalls = 0;
  auto writer =
      makeParallelWriter([&numCalls](std::string_view, bool) -> uint64_t {
        ++numCalls;
        throw std::runtime_error{"The vocabulary could not be written"};
      });
  AsyncBlockWriter blockWriter{writer};
  blockWriter.push(0, blockOf(makeSegment({"\"a\""}, 0, 1, writer), 0));
  blockWriter.push(0, blockOf(makeSegment({"\"b\""}, 0, 1, writer), 1));
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      blockWriter.finish(), ::testing::HasSubstr("could not be written"),
      std::runtime_error);
  // The first block threw at its first word, and the second block was skipped
  // (the blocks of a sub-vocabulary are appended in order).
  EXPECT_EQ(numCalls, 1u);
}

// Test that an exception from the preparation of a block (thrown on the
// prepare task, not on the appending one) is rethrown by `finish()`, and that
// the blocks behind the failed one are not appended, although they were
// prepared successfully.
TEST(AsyncBlockWriter, exceptionOfPrepareIsPropagated) {
  std::atomic<size_t> numAppended = 0;
  SingleVocabularyParallelWriter writer{
      std::make_unique<ThrowingPrepareBlockWriter>(numAppended)};
  AsyncBlockWriter blockWriter{writer};
  for (size_t i = 0; i < 5; ++i) {
    blockWriter.push(
        0,
        blockOf(makeSegment({absl::StrFormat("\"w%d\"", i)}, 0, 1, writer), i));
  }
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      blockWriter.finish(), ::testing::HasSubstr("could not be prepared"),
      std::runtime_error);
  EXPECT_EQ(numAppended, 0u);
}

// Test that once a task has failed, `push` ignores further blocks: they are
// neither counted nor written, and `finish()` rethrows the original exception.
TEST(AsyncBlockWriter, pushAfterFailureIsIgnored) {
  size_t numCalls = 0;
  auto writer =
      makeParallelWriter([&numCalls](std::string_view, bool) -> uint64_t {
        ++numCalls;
        throw std::runtime_error{"The vocabulary could not be written"};
      });
  AsyncBlockWriter blockWriter{writer};
  blockWriter.push(0, blockOf(makeSegment({"\"a\""}, 0, 1, writer), 0));
  ASSERT_TRUE(waitUntil([&blockWriter]() { return blockWriter.hasFailed(); }));
  blockWriter.push(0, blockOf(makeSegment({"\"b\""}, 0, 1, writer), 1));
  EXPECT_EQ(blockWriter.numBlocks(), 1u);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      blockWriter.finish(), ::testing::HasSubstr("could not be written"),
      std::runtime_error);
  EXPECT_EQ(numCalls, 1u);
}
