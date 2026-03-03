// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/stream_generator.h"

using namespace ad_utility::streams;
using ::testing::_;
using ::testing::InSequence;
using ::testing::StrictMock;

// First some example tests that demonstrate how to use the `STREAMABLE_...`
// macros:

namespace {
// A simple generator, that yields "hello" `i` times and concatenates the result
// into batches.
STREAMABLE_GENERATOR_TYPE yieldSomething(size_t i,
                                         STREAMABLE_YIELDER_ARG_DECL) {
  for (size_t j = 0; j < i; ++j) {
    STREAMABLE_YIELD("hello");
  }
  STREAMABLE_RETURN;  // could also be omitted.
}
}  // namespace

#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// Using `yieldSomething` from above in C++17 model
TEST(StringBatcher, StreamMacros) {
  std::string result;
  // Create a `StringBatcher` that does something with the yielded batches.
  StringBatcher batcher{[&result](const auto& batch) { result.append(batch); }};
  // Call the "generator" with a `reference_wrapper` to the `batcher` as the
  // last argument.
  yieldSomething(3, std::ref(batcher));

  // Finish to also make the last batch visible.
  batcher.finish();
  EXPECT_EQ(result, "hellohellohello");
}
#else
TEST(StringBatcher, StreamMacros) {
  std::vector<std::string> result;
  // In C++20, `yieldSomething` is an `input_range` that just yields the
  // results.
  for (const auto& batch : yieldSomething(3)) {
    result.emplace_back(batch);
  };
  EXPECT_THAT(result, ::testing::ElementsAre("hellohellohello"));
}

#endif

// In the following there are tests for the `StringBatcher` class.

namespace {
constexpr size_t TEST_BATCH_SIZE = 10;

// Mock callback to verify batch processing behavior
class MockBatchCallback {
 public:
  MOCK_METHOD(void, call, (std::string_view), (const));

  void operator()(std::string_view batch) const { call(batch); }
};
}  // namespace

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

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    // Set expectation right before the operation that should trigger it
    // immediately
    EXPECT_CALL(mockCallback, call(std::string_view("1234567890")));
    batcher("1234567890");  // Exactly TEST_BATCH_SIZE characters, should
                            // trigger callback immediately

    // Verify the expectation was satisfied immediately, not by destructor
    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
    // No need to call finish() as batch is already complete
  }
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

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    batcher("hello");  // 5 chars

    // Callback should be triggered immediately when buffer becomes full
    EXPECT_CALL(mockCallback, call(std::string_view("helloworld")));
    batcher("world");  // 5 chars, total = 10 = TEST_BATCH_SIZE

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
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

  {
    StringBatcher<TEST_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    // Add single characters to fill exactly one batch
    for (char c = 'a'; c <= 'i'; ++c) {  // 'a' to 'i' = 9 chars
      batcher(c);
    }

    // Callback should be triggered immediately when the 10th character fills
    // the batch
    EXPECT_CALL(mockCallback, call(std::string_view("abcdefghij")));
    batcher('j');  // 10th character, should trigger callback immediately

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
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

  {
    StringBatcher<1> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    // Each character should trigger a callback immediately as the batch size is
    // 1
    InSequence seq;
    EXPECT_CALL(mockCallback, call(std::string_view("a")));
    EXPECT_CALL(mockCallback, call(std::string_view("b")));
    EXPECT_CALL(mockCallback, call(std::string_view("c")));

    batcher("abc");  // Each character should trigger a separate callback
                     // immediately

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
}

// _____________________________________________________________________________
TEST(StringBatcher, LargeBatchSize) {
  constexpr size_t LARGE_BATCH_SIZE = 1000;
  StrictMock<MockBatchCallback> mockCallback;

  std::string largeContent(LARGE_BATCH_SIZE - 1, 'X');
  largeContent += "Y";  // Exactly LARGE_BATCH_SIZE characters

  {
    StringBatcher<LARGE_BATCH_SIZE> batcher{
        [&mockCallback](std::string_view batch) { mockCallback(batch); }};

    // Add content in smaller chunks
    for (size_t i = 0; i < 10; ++i) {
      batcher(std::string(99, 'X'));
    }
    batcher(std::string(9, 'X'));

    // Callback should be triggered immediately when buffer becomes exactly full
    EXPECT_CALL(mockCallback, call(std::string_view(largeContent)));
    batcher("Y");  // This should complete the batch and trigger callback
                   // immediately

    ::testing::Mock::VerifyAndClearExpectations(&mockCallback);
  }
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
