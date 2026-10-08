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

#include <atomic>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <range/v3/range/conversion.hpp>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "util/GlobalExecutor.h"
#include "util/Random.h"
#include "util/views/AsyncTransformView.h"

namespace {
using ad_utility::AsyncTransformView;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using namespace std::chrono_literals;

// The time that the tests wait for something that is expected to happen. It is
// deliberately generous, because it is only waited for in full if the awaited
// event never happens, which is a test failure anyway.
constexpr auto timeout = 10s;
}  // namespace

// _____________________________________________________________________________
TEST(AsyncTransformView, resultsAreYieldedInOrder) {
  boost::asio::thread_pool pool{4};
  // The random sleeps make the transformations complete in an order that
  // differs from the order of the elements.
  auto transformation = [](int i) {
    ad_utility::SlowRandomIntGenerator<int> sleepMicros{0, 500};
    std::this_thread::sleep_for(std::chrono::microseconds{sleepMicros()});
    return std::to_string(2 * i);
  };
  std::vector<std::string> expected;
  for (int i : ql::views::iota(0, 200)) {
    expected.push_back(std::to_string(2 * i));
  }
  // Fewer, as many, and more elements in flight than there are threads, and
  // more elements in flight than there are elements.
  for (size_t maxNumElementsInFlight : {1, 2, 4, 7, 1000}) {
    AsyncTransformView view{ql::views::iota(0, 200), transformation,
                            maxNumElementsInFlight, pool.get_executor()};
    EXPECT_THAT(::ranges::to_vector(view), ElementsAreArray(expected));
    // An exhausted view stays exhausted.
    EXPECT_FALSE(view.get().has_value());
  }
}

// _____________________________________________________________________________
TEST(AsyncTransformView, emptyInput) {
  boost::asio::thread_pool pool{2};
  std::atomic<size_t> numCalls = 0;
  AsyncTransformView view{std::vector<int>{},
                          [&numCalls](int i) {
                            ++numCalls;
                            return i;
                          },
                          3, pool.get_executor()};
  EXPECT_TRUE(::ranges::to_vector(view).empty());
  EXPECT_FALSE(view.get().has_value());
  EXPECT_EQ(numCalls, 0);
}

// _____________________________________________________________________________
TEST(AsyncTransformView, invalidNumberOfElementsInFlight) {
  boost::asio::thread_pool pool{1};
  EXPECT_ANY_THROW((AsyncTransformView{ql::views::iota(0, 3), std::identity{},
                                       0, pool.get_executor()}));
}

// _____________________________________________________________________________
TEST(AsyncTransformView, inputIsNotOwnedWhenPassedAsRefView) {
  boost::asio::thread_pool pool{2};
  std::vector<int> input = ::ranges::to_vector(ql::views::iota(0, 10));
  // The view only skips the odd elements, the actual input stays untouched,
  // because the view only refers to it.
  auto even = ql::ranges::ref_view{input} |
              ql::views::filter([](int i) { return i % 2 == 0; });
  AsyncTransformView view{std::move(even), [](int i) { return i + 100; }, 2,
                          pool.get_executor()};
  EXPECT_THAT(::ranges::to_vector(view), ElementsAre(100, 102, 104, 106, 108));
  EXPECT_THAT(input,
              ElementsAreArray(::ranges::to_vector(ql::views::iota(0, 10))));
}

// _____________________________________________________________________________
TEST(AsyncTransformView, moveOnlyElementsAndResults) {
  boost::asio::thread_pool pool{3};
  std::vector<std::unique_ptr<int>> input;
  for (int i : ql::views::iota(0, 20)) {
    input.push_back(std::make_unique<int>(i));
  }
  // Both the element and the result are move-only, and the result takes over
  // the ownership of the element.
  auto transformation = [](std::unique_ptr<int> ptr) {
    *ptr *= 3;
    return std::pair{std::move(ptr), std::string{"x"}};
  };
  AsyncTransformView view{std::move(input), transformation, 3,
                          pool.get_executor()};
  int expected = 0;
  for (auto& [ptr, str] : view) {
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(*ptr, 3 * expected);
    EXPECT_EQ(str, "x");
    ++expected;
  }
  EXPECT_EQ(expected, 20);
}

// _____________________________________________________________________________
TEST(AsyncTransformView, elementsAreTransformedConcurrently) {
  constexpr size_t numInFlight = 3;
  boost::asio::thread_pool pool{numInFlight};
  // Every transformation waits until `numInFlight` transformations are running
  // at the same time. This can only succeed if the view hands that many
  // elements to the pool before it waits for the first result.
  std::mutex mutex;
  std::condition_variable cv;
  size_t numStarted = 0;
  auto transformation = [&](int i) {
    std::unique_lock lock{mutex};
    ++numStarted;
    cv.notify_all();
    bool allStarted =
        cv.wait_for(lock, timeout, [&]() { return numStarted >= numInFlight; });
    return std::pair{i, allStarted};
  };
  AsyncTransformView view{ql::views::iota(0, static_cast<int>(numInFlight)),
                          transformation, numInFlight, pool.get_executor()};
  int expected = 0;
  for (auto& [i, allStarted] : view) {
    EXPECT_EQ(i, expected);
    EXPECT_TRUE(allStarted);
    ++expected;
  }
  EXPECT_EQ(expected, static_cast<int>(numInFlight));
}

// _____________________________________________________________________________
TEST(AsyncTransformView, readsAheadLazilyAndBounded) {
  boost::asio::thread_pool pool{4};
  constexpr size_t numInFlight = 3;
  constexpr int numElements = 20;
  // Count how many elements have been read from the input so far.
  size_t numRead = 0;
  auto countedInput =
      ql::views::iota(0, numElements) | ql::views::transform([&numRead](int i) {
        ++numRead;
        return i;
      });
  AsyncTransformView view{std::move(countedInput), std::identity{}, numInFlight,
                          pool.get_executor()};
  // Nothing is read before the first element is requested.
  EXPECT_EQ(numRead, 0);
  size_t numYielded = 0;
  while (auto element = view.get()) {
    EXPECT_EQ(element.value(), static_cast<int>(numYielded));
    ++numYielded;
    // At most `numInFlight` elements are read before they are yielded, the
    // yielded one included.
    EXPECT_LE(numRead, numYielded - 1 + numInFlight);
    EXPECT_GE(numRead,
              std::min<size_t>(numYielded - 1 + numInFlight, numElements));
  }
  EXPECT_EQ(numYielded, numElements);
  EXPECT_EQ(numRead, numElements);
}

// _____________________________________________________________________________
TEST(AsyncTransformView, exceptionInTransformation) {
  boost::asio::thread_pool pool{2};
  auto transformation = [](int i) {
    if (i == 5) {
      throw std::runtime_error{"transformation failed"};
    }
    return i;
  };
  for (size_t maxNumElementsInFlight : {1, 3, 10}) {
    AsyncTransformView view{ql::views::iota(0, 10), transformation,
                            maxNumElementsInFlight, pool.get_executor()};
    // The results before the failing element are yielded normally.
    for (int i : ql::views::iota(0, 5)) {
      auto element = view.get();
      ASSERT_TRUE(element.has_value());
      EXPECT_EQ(element.value(), i);
    }
    EXPECT_THROW(view.get(), std::runtime_error);
  }
}

// _____________________________________________________________________________
TEST(AsyncTransformView, exceptionInInput) {
  boost::asio::thread_pool pool{2};
  auto throwingInput = []() {
    return ql::views::iota(0, 10) | ql::views::transform([](int i) {
             if (i == 4) {
               throw std::runtime_error{"input failed"};
             }
             return i;
           });
  };
  // With a single element in flight, the first results are yielded before the
  // input throws.
  {
    AsyncTransformView view{throwingInput(), std::identity{}, 1,
                            pool.get_executor()};
    for (int i : ql::views::iota(0, 4)) {
      auto element = view.get();
      ASSERT_TRUE(element.has_value());
      EXPECT_EQ(element.value(), i);
    }
    EXPECT_THROW(view.get(), std::runtime_error);
  }
  // With more elements in flight than there are elements before the failing
  // one, the input already throws when the first element is requested.
  {
    AsyncTransformView view{throwingInput(), std::identity{}, 10,
                            pool.get_executor()};
    EXPECT_THROW(view.get(), std::runtime_error);
  }
}

// _____________________________________________________________________________
TEST(AsyncTransformView, destructionWithPendingElements) {
  boost::asio::thread_pool pool{2};
  // The transformations are slow, so that several of them are still pending
  // when the view is destroyed. The destructor has to wait for all of them,
  // because they use the transformation, which is owned by the view.
  std::atomic<size_t> numStarted = 0;
  std::atomic<size_t> numFinished = 0;
  auto transformation = [&numStarted, &numFinished,
                         data = std::make_shared<int>(42)](int i) {
    ++numStarted;
    std::this_thread::sleep_for(5ms);
    // Touch the state of the transformation after the sleep.
    int result = i + *data;
    ++numFinished;
    return result;
  };
  {
    AsyncTransformView view{ql::views::iota(0, 100), transformation, 5,
                            pool.get_executor()};
    auto element = view.get();
    ASSERT_TRUE(element.has_value());
    EXPECT_EQ(element.value(), 42);
  }
  // The view has read exactly five elements ahead, and the destructor has
  // waited for all of them.
  EXPECT_EQ(numStarted, 5);
  EXPECT_EQ(numFinished, 5);
}

// _____________________________________________________________________________
TEST(AsyncTransformView, globalExecutorAndTypeErasure) {
  // The view can be type-erased via `InputRangeTypeErased`, which makes it
  // movable, and it works with the global executor.
  using View =
      AsyncTransformView<decltype(ql::views::iota(0, 0)), std::negate<>>;
  ad_utility::InputRangeTypeErased<int> erased{
      std::make_unique<View>(ql::views::iota(0, 50), std::negate<>{}, 4,
                             ad_utility::globalExecutor())};
  auto moved = std::move(erased);
  std::vector<int> expected;
  for (int i : ql::views::iota(0, 50)) {
    expected.push_back(-i);
  }
  EXPECT_THAT(::ranges::to_vector(moved), ElementsAreArray(expected));
}
