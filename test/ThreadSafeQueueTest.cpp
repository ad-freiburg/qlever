// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <atomic>
#include <ranges>

#include "./util/GTestHelpers.h"
#include "util/ThreadSafeQueue.h"
#include "util/TypeTraits.h"
#include "util/jthread.h"

using namespace ad_utility::data_structures;

namespace {

// Create a lambda, that pushes a given `size_t i` to the given `queue`. If the
// `Queue` is a `OrderedThreadSafeQueue`, then `i` is also used as the index for
// the call to `push`. This imposes requirements on the values that are pushed
// to avoid deadlocks, see `ThreadSafeQueue.h` for details.
template <typename Queue>
auto makePush(Queue& queue) {
  return [&queue](size_t i) {
    if constexpr (ad_utility::similarToInstantiation<Queue, ThreadSafeQueue>) {
      return queue.push(i);
    } else {
      return queue.push(i, i);
    }
  };
}

// Similar to `makePush` above, but the returned lambda doesn't push directly to
// the queue, but simply returns a value that can then be pushed to the queue.
// This is useful when testing the `queueManager` template.
template <typename Queue>
auto makeQueueValue() {
  return [](size_t i) {
    if constexpr (ad_utility::similarToInstantiation<Queue, ThreadSafeQueue>) {
      return i;
    } else {
      return std::pair{i, i};
    }
  };
}

// Some constants that are used in almost every test case.
constexpr size_t queueSize = 5;
constexpr size_t numThreads = 20;
constexpr size_t numValues = 200;

// Run the `test` function with a `ThreadSafeQueue` and an
// `OrderedThreadSafeQueue`. Both queues have a size of `queueSize` and `size_t`
// as their value type.
template <typename F>
void runWithBothQueueTypes(const F& testFunction) {
  testFunction(ThreadSafeQueue<size_t>{queueSize});
  testFunction(OrderedThreadSafeQueue<size_t>{queueSize});
}
}  // namespace

// ________________________________________________________________
TEST(ThreadSafeQueue, BufferSizeIsRespected) {
  auto runTest = [](auto queue) {
    std::atomic<size_t> numPushed = 0;
    auto push = makePush(queue);

    // Asynchronous worker thread that pushes incremental values to the queue.
    ad_utility::JThread t([&numPushed, &push, &queue] {
      while (numPushed < numValues) {
        push(numPushed++);
      }
      queue.finish();
    });

    size_t numPopped = 0;
    while (auto opt = queue.pop()) {
      // We have only one thread pushing, so the elements in the queue are
      // ordered.
      EXPECT_EQ(opt.value(), numPopped);
      ++numPopped;
      // Check that the size of the queue is respected. The pushing thread must
      // only continue to push once enough elements have been `pop`ped. The `+1`
      // is necessary because the calls to `pop` and `push` are not synchronized
      // with the atomic value `numPushed`.
      EXPECT_LE(numPushed, numPopped + queueSize + 1);
    }
  };
  runWithBothQueueTypes(runTest);
}

// _______________________________________________________________
TEST(ThreadSafeQueue, ReturnValueOfPush) {
  auto runTest = [](auto queue) {
    auto push = makePush(queue);
    // Test that `push` always returns true until `disablePush()` has been
    // called.
    EXPECT_TRUE(push(0));
    EXPECT_EQ(queue.pop(), 0);
    queue.finish();
    EXPECT_FALSE(push(1));
  };
  runWithBothQueueTypes(runTest);
}

// Test the case that multiple workers are pushing concurrently.
TEST(ThreadSafeQueue, Concurrency) {
  auto runTest = [](auto queue) {
    using Queue = decltype(queue);

    std::atomic<size_t> numPushed = 0;
    std::atomic<size_t> numThreadsDone = 0;
    auto push = makePush(queue);

    // Set up the worker threads.
    auto threadFunction = [&numPushed, &queue, &push, &numThreadsDone] {
      for (size_t i = 0; i < numValues; ++i) {
        // push the next available value that hasn't been pushed yet by another
        // thread.
        push(numPushed++);
      }
      numThreadsDone++;
      if (numThreadsDone == numThreads) {
        queue.finish();
      }
    };

    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
      threads.emplace_back(threadFunction);
    }

    // Pop the values from the queue and store them.
    size_t numPopped = 0;
    std::vector<size_t> result;
    while (auto opt = queue.pop()) {
      ++numPopped;
      result.push_back(opt.value());
      // The ` + numThreads` is because the atomic increment of `numPushed` is
      // done before the actual call to `push`. The `+ 1` is because another
      // element might have been pushed since our last call to `pop()`.
      EXPECT_LE(numPushed, numPopped + queueSize + 1 + numThreads);
    }

    // For the `OrderedThreadSafeQueue` we expect the result to already be in
    // order, for the `ThreadSafeQueue` the order is unspecified and we only
    // check the content.
    if (ad_utility::isInstantiation<Queue, ThreadSafeQueue>) {
      ql::ranges::sort(result);
    }
    EXPECT_THAT(result, ::testing::ElementsAreArray(
                            std::views::iota(0UL, numValues * numThreads)));
  };
  runWithBothQueueTypes(runTest);
}

// ________________________________________________________________
TEST(ThreadSafeQueue, PushException) {
  auto runTest = [](auto queue) {
    std::atomic<size_t> numPushed = 0;
    std::atomic<size_t> threadIndex = 0;

    auto push = makePush(queue);

    struct IntegerException : public std::exception {
      size_t value_;
      explicit IntegerException(size_t value) : value_{value} {}
    };

    auto threadFunction = [&numPushed, &queue, &threadIndex, &push] {
      bool hasThrown = false;
      for (size_t i = 0; i < numValues; ++i) {
        if (numPushed > 300 && !hasThrown) {
          hasThrown = true;
          // At some point, each thread pushes an Exception. After pushing the
          // exception, all calls to `push` return false.
          queue.pushException(
              std::make_exception_ptr(IntegerException{threadIndex++}));
          EXPECT_FALSE(push(numPushed++));
        } else if (hasThrown) {
          // In the case that we have previously thrown an exception, we know
          // that the queue is disabled. This means that we can safely push an
          // out-of-order value even to the ordered queue. Note that we
          // deliberately do not push `numPushed++` as usual, because otherwise
          // we cannot say much about the value of `numPushed` after throwing
          // the first exception. Note that this pattern is only for testing,
          // and that a thread that has pushed an exception to a queue should
          // stop pushing to the same queue in real life.
          EXPECT_FALSE(push(0));
        } else {
          // We cannot know whether this returns true or false, because another
          // thread already might have thrown an exception.
          push(numPushed++);
        }
      }
    };

    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
      threads.emplace_back(threadFunction);
    }

    size_t numPopped = 0;

    try {
      // The usual check as always, but at some point `pop` will throw, because
      // exceptions were pushed to the queue.
      while (queue.pop()) {
        ++numPopped;
        EXPECT_LE(numPushed, numPopped + queueSize + 1 + 2 * numThreads);
      }
      FAIL() << "Should have thrown" << std::endl;
    } catch (const IntegerException& i) {
      EXPECT_LT(i.value_, numThreads);
    }
  };
  runWithBothQueueTypes(runTest);
}

// ________________________________________________________________
TEST(ThreadSafeQueue, DisablePush) {
  auto runTest = [](auto queue) {
    using Queue = decltype(queue);

    std::atomic<size_t> numPushed = 0;
    auto push = makePush(queue);

    auto threadFunction = [&numPushed, &push] {
      while (true) {
        // Push until the consumer calls `disablePush`.
        if (!push(numPushed++)) {
          return;
        }
      }
    };

    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
      threads.emplace_back(threadFunction);
    }

    size_t numPopped = 0;
    std::vector<size_t> result;
    while (auto opt = queue.pop()) {
      ++numPopped;
      result.push_back(opt.value());
      EXPECT_LE(numPushed, numPopped + queueSize + 1 + numThreads);

      // Disable the push, make the consumers finish.
      if (numPopped == 400) {
        queue.finish();
        break;
      }
    }
    if (ad_utility::similarToInstantiation<Queue, ThreadSafeQueue>) {
      // When terminating early, we cannot actually say much about the result,
      // other than that it contains no duplicate values
      ql::ranges::sort(result);
      EXPECT_TRUE(std::unique(result.begin(), result.end()) == result.end());
    } else {
      // For the ordered queue we have the guarantee that all the pushed values
      // were in order.
      EXPECT_THAT(result,
                  ::testing::ElementsAreArray(ql::views::iota(0U, 400U)));
    }
  };
  runWithBothQueueTypes(runTest);
}

// Demonstrate the safe way to handle exceptions and early destruction in the
// worker threads as well as in the consumer threads. By `safe` we mean that the
// program is neither terminated nor does it run into a deadlock.
TEST(ThreadSafeQueue, SafeExceptionHandling) {
  auto runTest = [](bool workerThrows, auto&& queue) {
    auto throwingProcedure = [&]() {
      auto threadFunction = [&queue, workerThrows] {
        try {
          auto push = makePush(queue);
          size_t numPushed = 0;
          // We have to finish the threadas soon as `push` returns false.
          while (push(numPushed++)) {
            // Manually throw an exception if `workerThrows` was specified.
            if (numPushed >= numValues / 2 && workerThrows) {
              throw std::runtime_error{"Producer died"};
            }
          }
        } catch (...) {
          // We have to catch all exceptions in the worker thread(s), otherwise
          // the program will immediately terminate. When there was an exception
          // and the queue still expects results from this worker thread
          // (especially if the queue is ordered), we have to finish the queue.
          // If we just call `finish` then the producer will see a noop when
          // popping from the queue. When we use `pushException` the call to
          // `pop` will rethrow the exception.
          try {
            // In theory, `pushException` might throw if something goes really
            // wrong with the underlying mutex. In practice this should never
            // happen, but we demonstrate the really safe way here.
            queue.pushException(std::current_exception());
          } catch (...) {
            // `finish()` can never fail.
            queue.finish();
          }
        }
      };
      ad_utility::JThread thread{threadFunction};
      // This cleanup is important in case the consumer throws an exception. We
      // then first have to `finish` the queue, s.t. the producer threads can
      // join. We then can join and destroy the worker threads and finally
      // destroy the queue. So the order of declaration is important:
      // 1. Queue, 2. WorkerThreads, 3. `Cleanup` that finishes the queue.
      absl::Cleanup cleanup{[&queue] { queue.finish(); }};

      for ([[maybe_unused]] auto i : ql::views::iota(0u, numValues)) {
        auto opt = queue.pop();
        if (!opt) {
          return;
        }
      }
      // When throwing, the `Cleanup` calls `finish` and the producers can run
      // to completion because their calls to `push` will return false.
      throw std::runtime_error{"Consumer died"};
    };
    if (workerThrows) {
      AD_EXPECT_THROW_WITH_MESSAGE(throwingProcedure(),
                                   ::testing::StartsWith("Producer"));
    } else {
      AD_EXPECT_THROW_WITH_MESSAGE(throwingProcedure(),
                                   ::testing::StartsWith("Consumer"));
    }
  };
  runWithBothQueueTypes(std::bind_front(runTest, true));
  runWithBothQueueTypes(std::bind_front(runTest, false));
}

struct RunQueueManagerTest {
  template <typename TestType, typename Queue>
  void operator()(TestType testType, Queue&&) const {
    std::atomic<size_t> numPushed = 0;
    auto task =
        [&numPushed,
         &testType]() -> std::optional<decltype(makeQueueValue<Queue>()(3))> {
      auto makeValue = makeQueueValue<Queue>();
      if (testType == TestType::bothThrowImmediately) {
        throw std::runtime_error{"Producer"};
      }
      auto value = numPushed++;
      if (testType == TestType::producerThrows && value > numValues / 2) {
        throw std::runtime_error{"Producer"};
      }
      if (value < numValues) {
        return makeValue(value);
      } else {
        return std::nullopt;
      }
    };

    std::vector<size_t> result;
    size_t numPopped = 0;
    try {
      if (testType == TestType::bothThrowImmediately) {
        throw std::runtime_error{"Consumer"};
      }
      for (size_t value : queueManager<Queue>(queueSize, numThreads, task)) {
        ++numPopped;
        if (numPopped > numValues / 3) {
          if (testType == TestType::consumerThrows) {
            throw std::runtime_error{"Consumer"};
          } else if (testType == TestType::consumerFinishesEarly) {
            break;
          }
        }
        result.push_back(value);
        EXPECT_LE(numPushed, numPopped + queueSize + 1 + numThreads);
      }
      if (testType == TestType::consumerThrows ||
          testType == TestType::producerThrows) {
        FAIL() << "Should have thrown" << static_cast<unsigned>(testType);
      }
    } catch (const std::runtime_error& e) {
      if (testType == TestType::consumerThrows ||
          testType == TestType::bothThrowImmediately) {
        EXPECT_STREQ(e.what(), "Consumer");
      } else if (testType == TestType::producerThrows) {
        EXPECT_STREQ(e.what(), "Producer");
      } else {
        FAIL() << "Should not have thrown";
      }
    }

    if (testType == TestType::consumerFinishesEarly) {
      EXPECT_EQ(result.size(), numValues / 3);
    } else if (testType == TestType::normalExecution) {
      EXPECT_EQ(result.size(), numValues);
      // For the `OrderedThreadSafeQueue` we expect the result to already be in
      // order, for the `ThreadSafeQueue` the order is unspecified and we only
      // check the content.
      if (ad_utility::isInstantiation<Queue, ThreadSafeQueue>) {
        ql::ranges::sort(result);
      }
      EXPECT_THAT(result, ::testing::ElementsAreArray(
                              std::views::iota(0UL, numValues)));
    }
    // The probably most important test of all is that the destructors which are
    // run at the following closing brace never lead to a deadlock.
  }
};

// ________________________________________________________________
TEST(ThreadSafeQueue, queueManager) {
  enum class TestType {
    producerThrows,
    consumerThrows,
    normalExecution,
    consumerFinishesEarly,
    bothThrowImmediately
  };
  using enum TestType;
  runWithBothQueueTypes(std::bind_front(RunQueueManagerTest{}, consumerThrows));
  runWithBothQueueTypes(std::bind_front(RunQueueManagerTest{}, producerThrows));
  runWithBothQueueTypes(
      std::bind_front(RunQueueManagerTest{}, consumerFinishesEarly));
  runWithBothQueueTypes(
      std::bind_front(RunQueueManagerTest{}, normalExecution));
  runWithBothQueueTypes(
      std::bind_front(RunQueueManagerTest{}, bothThrowImmediately));
}
