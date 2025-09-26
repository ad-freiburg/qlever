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

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    batcher("hello");

    // Set expectation right before finish() to ensure it's not triggered by
    // destructor
    EXPECT_CALL(mockCallback, call(std::string_view("hello")));
    batcher.finish();

    // Verify expectation was satisfied before destructor
    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
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

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    // First batch will be called immediately when buffer is full
    EXPECT_CALL(mockCallback, call(std::string_view("1234567890")));
    batcher("1234567890abcdef");  // 16 characters, should split

    // Second batch only called on explicit finish()
    EXPECT_CALL(mockCallback, call(std::string_view("abcdef")));
    batcher.finish();

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
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

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    batcher("hello");  // 5 chars

    // First batch callback triggered when buffer becomes full
    EXPECT_CALL(mockCallback, call(std::string_view("helloworld")));
    batcher("world");  // 5 chars, batch full (10 chars), triggers callback

    batcher("test");  // 4 chars, starts new batch

    // Second batch only called on explicit finish()
    EXPECT_CALL(mockCallback, call(std::string_view("test")));
    batcher.finish();  // Commits remaining batch

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
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

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    batcher("hello");  // 5 chars
    batcher('1');      // 1 char

    // First batch callback triggered when buffer becomes full
    EXPECT_CALL(mockCallback, call(std::string_view("hello12345")));
    batcher("2345");  // 4 chars, total = 10, triggers callback

    batcher("world");  // 5 chars
    batcher('!');      // 1 char

    // Second batch only called on explicit finish()
    EXPECT_CALL(mockCallback, call(std::string_view("world!")));
    batcher.finish();

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
}

// _____________________________________________________________________________
TEST(StringBatcher, EmptyStringViewHandling) {
  StrictMock<MockBatchCallback> mockCallback;

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    batcher("");       // Empty string should not affect batching
    batcher("hello");  // 5 chars
    batcher("");       // Another empty string

    // Batch only called on explicit finish()
    EXPECT_CALL(mockCallback, call(std::string_view("hello")));
    batcher.finish();

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
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

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    // First two batches called immediately when buffer becomes full
    InSequence seq;
    EXPECT_CALL(mockCallback,
                call(std::string_view(std::string(TEST_BATCH_SIZE, 'Y'))));
    EXPECT_CALL(mockCallback,
                call(std::string_view(std::string(TEST_BATCH_SIZE, 'Y'))));

    batcher(largeString);

    // Remainder only called on explicit finish()
    EXPECT_CALL(mockCallback, call(std::string_view("YYY")));
    batcher.finish();  // Should commit the 3-character remainder

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
}

// _____________________________________________________________________________
TEST(StringBatcher, SequentialBatchProcessing) {
  StrictMock<MockBatchCallback> mockCallback;

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    // First batch - called immediately when full
    EXPECT_CALL(mockCallback, call(std::string_view("0123456789")));
    batcher("0123456789");

    // Second batch - called immediately when full
    EXPECT_CALL(mockCallback, call(std::string_view("abcdefghij")));
    batcher("abcdefghij");

    // Third batch - called immediately when full
    EXPECT_CALL(mockCallback, call(std::string_view("ABCDEFGHIJ")));
    batcher("ABCDEFGHIJ");

    // Partial fourth batch - only called on explicit finish()
    batcher("end");
    EXPECT_CALL(mockCallback, call(std::string_view("end")));
    batcher.finish();

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
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

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    batcher("test");

    // Only the first finish() call should trigger the callback
    EXPECT_CALL(mockCallback, call(std::string_view("test")));
    batcher.finish();  // First finish call

    // Verify that subsequent finish() calls are no-ops
    batcher.finish();  // Second finish call should be safe (no-op)
    batcher.finish();  // Third finish call should also be safe

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
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

// _____________________________________________________________________________
TEST(StringBatcher, FinishIsRequiredForPartialBatch) {
  StrictMock<MockBatchCallback> mockCallback;

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    batcher("test");  // Partial batch

    // This test ensures that without explicit finish(), the callback would not
    // be called before the EXPECT_CALL. The old version would pass even without
    // finish() because the destructor would call it.
    EXPECT_CALL(mockCallback, call(std::string_view("test")));
    batcher.finish();  // This explicit call is required

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
    // Now the destructor should be a no-op since finish() already committed the
    // batch
  }
}
