// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Claude Code Assistant

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/stream_generator.h"

using namespace ad_utility::streams;
using ::testing::_;
using ::testing::InSequence;
using ::testing::StrictMock;

namespace {
constexpr size_t TEST_BATCH_SIZE = 10;
}  // namespace

// Mock callback to verify batch processing behavior
class MockBatchCallback {
 public:
  MOCK_METHOD(void, call, (std::string_view), (const));

  void operator()(std::string_view batch) const { call(batch); }
};

// _____________________________________________________________________________
TEST(StringBatcher, EmptyBatcherCallsCallbackOnFinish) {
  StrictMock<MockBatchCallback> mockCallback;

  // Empty batcher should not call callback on destruction if no data was added
  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};
}

// _____________________________________________________________________________
TEST(StringBatcher, SingleStringFittingInBatchCallsCallbackOnFinish) {
  StrictMock<MockBatchCallback> mockCallback;

  EXPECT_CALL(mockCallback, call(std::string_view("hello")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher("hello");
  batcher.finish();
}

// _____________________________________________________________________________
TEST(StringBatcher, SingleStringFittingInBatchCallsCallbackOnDestruction) {
  StrictMock<MockBatchCallback> mockCallback;

  EXPECT_CALL(mockCallback, call(std::string_view("hello")));

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    batcher("hello");
    // Destructor should call finish() automatically
  }
}

// _____________________________________________________________________________
TEST(StringBatcher, StringExactlyFillingBatchCallsCallbackImmediately) {
  StrictMock<MockBatchCallback> mockCallback;

  EXPECT_CALL(mockCallback, call(std::string_view("1234567890")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher("1234567890");  // Exactly TEST_BATCH_SIZE characters
  // No need to call finish() as batch is already complete
}

// _____________________________________________________________________________
TEST(StringBatcher, StringLargerThanBatchSplitsAcrossMultipleBatches) {
  StrictMock<MockBatchCallback> mockCallback;

  InSequence seq;
  EXPECT_CALL(mockCallback, call(std::string_view("1234567890")));
  EXPECT_CALL(mockCallback, call(std::string_view("abcdef")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher("1234567890abcdef");  // 16 characters, should split
  batcher.finish();
}

// _____________________________________________________________________________
TEST(StringBatcher, MultipleSmallStringsBatchedTogether) {
  StrictMock<MockBatchCallback> mockCallback;

  EXPECT_CALL(mockCallback, call(std::string_view("helloworld")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher("hello");  // 5 chars
  batcher("world");  // 5 chars, total = 10 = TEST_BATCH_SIZE
}

// _____________________________________________________________________________
TEST(StringBatcher, MultipleStringsExceedingBatchSize) {
  StrictMock<MockBatchCallback> mockCallback;

  InSequence seq;
  EXPECT_CALL(mockCallback, call(std::string_view("helloworld")));
  EXPECT_CALL(mockCallback, call(std::string_view("test")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher("hello");  // 5 chars
  batcher("world");  // 5 chars, batch full (10 chars), triggers callback
  batcher("test");   // 4 chars, starts new batch
  batcher.finish();  // Commits remaining batch
}

// _____________________________________________________________________________
TEST(StringBatcher, SingleCharacterHandling) {
  StrictMock<MockBatchCallback> mockCallback;

  EXPECT_CALL(mockCallback, call(std::string_view("abcdefghij")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  // Add single characters to fill exactly one batch
  for (char c = 'a'; c <= 'j'; ++c) {
    batcher(c);
  }
}

// _____________________________________________________________________________
TEST(StringBatcher, MixedStringViewsAndChars) {
  StrictMock<MockBatchCallback> mockCallback;

  InSequence seq;
  EXPECT_CALL(mockCallback, call(std::string_view("hello12345")));
  EXPECT_CALL(mockCallback, call(std::string_view("world!")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher("hello");  // 5 chars
  batcher('1');      // 1 char
  batcher("2345");   // 4 chars, total = 10, triggers callback
  batcher("world");  // 5 chars
  batcher('!');      // 1 char
  batcher.finish();
}

// _____________________________________________________________________________
TEST(StringBatcher, EmptyStringViewHandling) {
  StrictMock<MockBatchCallback> mockCallback;

  EXPECT_CALL(mockCallback, call(std::string_view("hello")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher("");       // Empty string should not affect batching
  batcher("hello");  // 5 chars
  batcher("");       // Another empty string
  batcher.finish();
}

// _____________________________________________________________________________
TEST(StringBatcher, VeryLargeStringSpanningMultipleBatches) {
  StrictMock<MockBatchCallback> mockCallback;

  // Create a string that spans exactly 3 batches
  std::string largeString(TEST_BATCH_SIZE * 3, 'X');

  InSequence seq;
  EXPECT_CALL(mockCallback,
              call(std::string_view(std::string(TEST_BATCH_SIZE, 'X'))));
  EXPECT_CALL(mockCallback,
              call(std::string_view(std::string(TEST_BATCH_SIZE, 'X'))));
  EXPECT_CALL(mockCallback,
              call(std::string_view(std::string(TEST_BATCH_SIZE, 'X'))));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher(largeString);
  // All batches should have been committed during the call
}

// _____________________________________________________________________________
TEST(StringBatcher, VeryLargeStringWithRemainder) {
  StrictMock<MockBatchCallback> mockCallback;

  // Create a string that spans 2 full batches + 3 remainder chars
  std::string largeString(TEST_BATCH_SIZE * 2 + 3, 'Y');

  InSequence seq;
  EXPECT_CALL(mockCallback,
              call(std::string_view(std::string(TEST_BATCH_SIZE, 'Y'))));
  EXPECT_CALL(mockCallback,
              call(std::string_view(std::string(TEST_BATCH_SIZE, 'Y'))));
  EXPECT_CALL(mockCallback, call(std::string_view("YYY")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher(largeString);
  batcher.finish();  // Should commit the 3-character remainder
}

// _____________________________________________________________________________
TEST(StringBatcher, SequentialBatchProcessing) {
  StrictMock<MockBatchCallback> mockCallback;

  InSequence seq;
  EXPECT_CALL(mockCallback, call(std::string_view("0123456789")));
  EXPECT_CALL(mockCallback, call(std::string_view("abcdefghij")));
  EXPECT_CALL(mockCallback, call(std::string_view("ABCDEFGHIJ")));
  EXPECT_CALL(mockCallback, call(std::string_view("end")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  // First batch
  batcher("0123456789");

  // Second batch
  batcher("abcdefghij");

  // Third batch
  batcher("ABCDEFGHIJ");

  // Partial fourth batch
  batcher("end");
  batcher.finish();
}

// _____________________________________________________________________________
TEST(StringBatcher, CallbackReceivesCorrectBatchContent) {
  std::vector<std::string> receivedBatches;

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&receivedBatches](std::string_view batch) {
        receivedBatches.emplace_back(batch);
      }};

  batcher("ABC");    // 3 chars
  batcher("123");    // 3 chars
  batcher("xyz!");   // 4 chars, total = 10, triggers callback
  batcher("final");  // 5 chars
  batcher.finish();

  ASSERT_EQ(receivedBatches.size(), 2);
  EXPECT_EQ(receivedBatches[0], "ABC123xyz!");
  EXPECT_EQ(receivedBatches[1], "final");
}

// _____________________________________________________________________________
TEST(StringBatcher, MultipleFinishCallsAreSafe) {
  StrictMock<MockBatchCallback> mockCallback;

  EXPECT_CALL(mockCallback, call(std::string_view("test")));

  StringBatcher<TEST_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher("test");
  batcher.finish();  // First finish call
  batcher.finish();  // Second finish call should be safe (no-op)
  batcher.finish();  // Third finish call should also be safe
}

// _____________________________________________________________________________
TEST(StringBatcher, BatchSizeOfOne) {
  StrictMock<MockBatchCallback> mockCallback;

  InSequence seq;
  EXPECT_CALL(mockCallback, call(std::string_view("a")));
  EXPECT_CALL(mockCallback, call(std::string_view("b")));
  EXPECT_CALL(mockCallback, call(std::string_view("c")));

  StringBatcher<1> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  batcher("abc");  // Each character should trigger a separate callback
}

// _____________________________________________________________________________
TEST(StringBatcher, LargeBatchSize) {
  constexpr size_t LARGE_BATCH_SIZE = 1000;
  StrictMock<MockBatchCallback> mockCallback;

  std::string largeContent(LARGE_BATCH_SIZE - 1, 'X');
  largeContent += "Y";  // Exactly LARGE_BATCH_SIZE characters

  EXPECT_CALL(mockCallback, call(std::string_view(largeContent)));

  StringBatcher<LARGE_BATCH_SIZE> batcher{
      [&mockCallback](std::string_view batch) { mockCallback(batch); }};

  // Add content in smaller chunks
  for (size_t i = 0; i < 10; ++i) {
    batcher(std::string(99, 'X'));
  }
  batcher(std::string(9, 'X'));
  batcher("Y");  // This should complete the batch and trigger callback
}
