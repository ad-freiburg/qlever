// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "../util/AllocatorTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/TestException.h"
#include "backports/algorithm.h"
#include "backports/asio.h"
#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/CancellationHandle.h"
#include "util/blockSort/ParallelQuicksort.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"

// The tests of this file are organized around one fuzzer per element type
// (`fuzzSort`), which sorts inputs of random shapes and sizes with random
// tuning parameters and failing comparators, and compares the result with
// `std::sort`. The small tuning parameters make every code path of the
// algorithm reachable with small inputs.
//
// NOTE: All sorts of one element type use the same comparator type
// (`FuzzCompare`), so that they all use (and cover) the same instantiation of
// the algorithm.
namespace {

namespace net = boost::asio;
using namespace ad_utility::blockSort::detail;
using namespace ad_utility::blockSort::detail::quicksort;

// The thread pool shared by all tests.
constexpr uint32_t numPoolThreads = 8;
boost::asio::thread_pool& threadPool() {
  static boost::asio::thread_pool pool{numPoolThreads};
  return pool;
}

// The message of the exception that a `FuzzCompare` throws.
constexpr const char* comparisonFailed = "comparison failed";

// A comparator whose behavior is chosen at runtime: it sorts by `Less` in
// ascending or descending order, and it can throw on its `throwAt`-th call
// (counting from zero, over all copies).
template <typename Less>
struct FuzzCompare {
  bool descending_ = false;
  std::shared_ptr<std::atomic<int64_t>> callsUntilThrow_ =
      std::make_shared<std::atomic<int64_t>>(-1);

  // Throw on the `throwAt`-th call from now on, or never if it is negative.
  void throwAt(int64_t throwAt) { callsUntilThrow_->store(throwAt); }

  template <typename A, typename B>
  bool operator()(const A& a, const B& b) const {
    if (callsUntilThrow_->fetch_sub(1, std::memory_order_relaxed) == 0) {
      throw TestException{comparisonFailed};
    }
    return descending_ ? Less{}(b, a) : Less{}(a, b);
  }
};

// The keys of the test inputs. They are smaller than 2^30, so that each value
// type below can represent them in an order-preserving way.
using Key = uint64_t;
constexpr Key maxKey = (Key{1} << 30) - 1;

// The value types, each with its conversion from and to keys.
struct Uint32Values {
  using Container = std::vector<uint32_t>;
  using Less = std::less<>;
  static Container fromKeys(const std::vector<Key>& keys) {
    return {keys.begin(), keys.end()};
  }
  static std::vector<Key> toKeys(const Container& values) {
    return {values.begin(), values.end()};
  }
  static auto range(Container& values) { return ql::span<uint32_t>{values}; }
};

struct StringValues {
  using Container = std::vector<std::string>;
  using Less = std::less<>;
  // Zero-padded, so that the lexicographic order is the numeric order, and long
  // enough to not fit into the small string buffer.
  static Container fromKeys(const std::vector<Key>& keys) {
    Container values;
    for (Key key : keys) {
      values.push_back(absl::StrFormat("value_%010d_with_padding", key));
    }
    return values;
  }
  static std::vector<Key> toKeys(const Container& values) {
    std::vector<Key> keys;
    for (const auto& value : values) {
      keys.push_back(std::stoull(value.substr(6, 10)));
    }
    return keys;
  }
  static auto range(Container& values) { return ql::span<std::string>{values}; }
};

// Compares the rows of an `IdTable` by the bits of all their columns. Comparing
// the `Id`s themselves would require the `engine` library.
struct RowLess {
  template <typename A, typename B>
  bool operator()(const A& a, const B& b) const {
    for (size_t col = 0; col < 3; ++col) {
      if (a[col].getBits() != b[col].getBits()) {
        return a[col].getBits() < b[col].getBits();
      }
    }
    return false;
  }
};

// The rows of a column-major `IdTable`, whose iterators hand out proxy
// references. Each key is split into three columns of 10 bits.
template <int NumStaticCols>
struct IdTableValues {
  using Container = IdTableStatic<NumStaticCols>;
  using Less = RowLess;
  static Container fromKeys(const std::vector<Key>& keys) {
    Container table{3, ad_utility::testing::makeAllocator()};
    table.resize(keys.size());
    for (size_t row = 0; row < keys.size(); ++row) {
      for (size_t col = 0; col < 3; ++col) {
        auto part = (keys[row] >> (10 * (2 - col))) & 1023;
        table(row, col) = Id::makeFromInt(static_cast<int64_t>(part));
      }
    }
    return table;
  }
  static std::vector<Key> toKeys(const Container& table) {
    std::vector<Key> keys;
    for (const auto& row : table) {
      Key key = 0;
      for (size_t col = 0; col < 3; ++col) {
        key = (key << 10) | static_cast<Key>(row[col].getInt());
      }
      keys.push_back(key);
    }
    return keys;
  }
  static auto range(Container& table) {
    return ql::ranges::subrange{table.begin(), table.end()};
  }
};

// The shapes of the fuzzed inputs, each of which is a special case somewhere
// in the algorithm (sorted parts, reversed parts, runs of equal elements, ...).
enum class Shape {
  Random,
  FewDistinct,
  AllEqual,
  Sorted,
  Reversed,
  Sawtooth,
  OrganPipe,
  SwappedHalves,
  NearlySorted,
  SortedBlocksShuffled,
};
constexpr size_t numShapes = 10;

// `numKeys` keys of the given `shape`.
std::vector<Key> makeKeys(Shape shape, size_t numKeys, std::mt19937_64& gen) {
  std::vector<Key> keys(numKeys);
  auto random = [&gen](Key upperBound) {
    return std::uniform_int_distribution<Key>{0, upperBound}(gen);
  };
  switch (shape) {
    case Shape::Random:
      ql::ranges::generate(keys, [&] { return random(maxKey); });
      break;
    case Shape::FewDistinct:
      ql::ranges::generate(keys, [&] { return random(4); });
      break;
    case Shape::AllEqual:
      ql::ranges::fill(keys, random(maxKey));
      break;
    case Shape::Sorted:
    case Shape::Reversed:
    case Shape::NearlySorted:
      ql::ranges::generate(keys, [&] { return random(maxKey); });
      ql::ranges::sort(keys);
      if (shape == Shape::Reversed) {
        ql::ranges::reverse(keys);
      } else if (shape == Shape::NearlySorted && numKeys > 1) {
        for (size_t i = 0; i < 1 + numKeys / 1000; ++i) {
          std::swap(keys[random(numKeys - 1)], keys[random(numKeys - 1)]);
        }
      }
      break;
    case Shape::Sawtooth: {
      Key period = 1 + random(1000);
      for (size_t i = 0; i < numKeys; ++i) {
        keys[i] = i % period;
      }
      break;
    }
    case Shape::OrganPipe:
      for (size_t i = 0; i < numKeys; ++i) {
        keys[i] = std::min(i, numKeys - 1 - i);
      }
      break;
    case Shape::SwappedHalves:
      // Two sorted halves, all elements of the first one bigger than those of
      // the second one.
      for (size_t i = 0; i < numKeys; ++i) {
        keys[i] = (i + numKeys / 2) % numKeys;
      }
      break;
    case Shape::SortedBlocksShuffled: {
      // Sorted runs of random length, in random order.
      ql::ranges::generate(keys, [&] { return random(maxKey); });
      ql::ranges::sort(keys);
      size_t runLength = 1 + random(500);
      for (size_t i = 0; i + runLength < numKeys; i += runLength) {
        size_t other = random((numKeys - runLength) / runLength) * runLength;
        std::swap_ranges(keys.begin() + i, keys.begin() + i + runLength,
                         keys.begin() + other);
      }
      break;
    }
  }
  return keys;
}

// Run `awaitable` on the shared thread pool, wait for it, and rethrow its
// exception.
void runOnPool(net::awaitable<void> awaitable) {
  net::co_spawn(threadPool().get_executor(), std::move(awaitable),
                net::use_future)
      .get();
}

// Sort `values` with `parallelQuicksort` on the shared thread pool, or with
// `serialQuicksort` if `serial` is set, both not cancellable.
template <typename Values>
void runSortOn(typename Values::Container& values,
               const FuzzCompare<typename Values::Less>& comp,
               size_t maxElementsPerTask, bool serial = false) {
  auto range = Values::range(values);
  if (serial) {
    serialQuicksort(range.begin(), range.end(), comp, maxElementsPerTask,
                    ad_utility::noop);
    return;
  }
  SortState state{comp, maxElementsPerTask, threadPool().get_executor(),
                  ad_utility::noop};
  runOnPool(parallelQuicksort(state, range.begin(), range.end()));
}

// Sort `numIterations` random inputs of `Values` with random tuning
// parameters and shapes, and expect the result of `std::sort`. With
// `withFailures`, the comparator throws at a random point (or not at all if the
// sort needs fewer comparisons), in which case the exception has to reach the
// caller. With `serial`, `serialQuicksort` is fuzzed instead.
template <typename Values>
void fuzzSort(size_t numIterations, bool withFailures, bool serial) {
  auto seed = std::random_device{}();
  SCOPED_TRACE(absl::StrCat("seed=", seed));
  std::mt19937_64 gen{seed};
  auto random = [&gen](size_t lower, size_t upper) {
    return std::uniform_int_distribution<size_t>{lower, upper}(gen);
  };
  for (size_t iteration = 0; iteration < numIterations; ++iteration) {
    size_t maxElementsPerTask = random(16, 128);
    // Big enough for several levels of parallel recursion.
    size_t numKeys =
        random(0, 3) == 0 ? random(0, 40) : random(0, 100 * maxElementsPerTask);
    auto shape = static_cast<Shape>(random(0, numShapes - 1));
    SCOPED_TRACE(absl::StrCat(
        "iteration=", iteration, " shape=", static_cast<int>(shape),
        " numKeys=", numKeys, " maxElementsPerTask=", maxElementsPerTask,
        " serial=", serial));
    auto keys = makeKeys(shape, numKeys, gen);
    auto values = Values::fromKeys(keys);

    FuzzCompare<typename Values::Less> comp;
    comp.descending_ = random(0, 3) == 0;
    if (withFailures) {
      comp.throwAt(static_cast<int64_t>(random(0, 20 * numKeys)));
    }
    ql::ranges::sort(keys);
    if (comp.descending_) {
      ql::ranges::reverse(keys);
    }

    // `throwAt` may exceed the number of comparisons of the sort, so the sort
    // may or may not throw, hence no `AD_EXPECT_THROW_WITH_MESSAGE`.
    bool threw = false;
    try {
      runSortOn<Values>(values, comp, maxElementsPerTask, serial);
    } catch (const TestException& e) {
      EXPECT_STREQ(e.what(), comparisonFailed);
      threw = true;
    }
    if (threw) {
      EXPECT_TRUE(withFailures);
    } else {
      EXPECT_EQ(Values::toKeys(values), keys);
    }
  }
}

// The fuzzer for one value type, parallel and serial, with and without
// failures, and the rejection of too small tasks (a task must sort at least 16
// elements on its own, see `computePivotAndMoveToFront`) with the same
// comparator.
template <typename Values>
void testValueType(size_t numIterations) {
  for (bool serial : {false, true}) {
    fuzzSort<Values>(numIterations, false, serial);
    fuzzSort<Values>(numIterations, true, serial);
  }
  AD_EXPECT_THROW_WITH_MESSAGE(
      (SortState{FuzzCompare<typename Values::Less>{}, 15,
                 threadPool().get_executor(), ad_utility::noop}),
      ::testing::HasSubstr("maxElementsPerTask_ >= 16"));
  auto values = Values::fromKeys({3, 1, 2});
  AD_EXPECT_THROW_WITH_MESSAGE(
      runSortOn<Values>(values, {}, 15, true),
      ::testing::HasSubstr("maxElementsPerPart >= 16"));
}

// Spawn a child into `group` that only finishes 50 ms after it has started,
// and wait until it has started.
void spawnSlowChild(TaskGroup& group, std::atomic<bool>& finished) {
  std::atomic<bool> started{false};
  group.spawnFunction([&started, &finished] {
    started = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    finished = true;
  });
  while (!started) {
    std::this_thread::yield();
  }
}

// A coroutine that throws.
net::awaitable<void> throwingCoroutine() {
  throw TestException{"coroutine failed"};
  co_return;
}

// Await `withChildren` with `numChildren` children on the shared thread pool,
// and return whether the awaiting coroutine runs on `strand` afterwards.
net::awaitable<bool> isOnStrandAfterJoin(
    const net::strand<ql::any_io_executor>& strand, size_t numChildren) {
  ql::any_io_executor executor = threadPool().get_executor();
  std::atomic<bool> stopped{false};
  co_await TaskGroup::withChildren(
      executor, stopped, [numChildren](TaskGroup& group) {
        for (size_t i = 0; i < numChildren; ++i) {
          group.spawnFunction([] {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
          });
        }
      });
  co_return strand.running_in_this_thread();
}

// A coroutine that sets `flag`.
net::awaitable<void> setFlag(std::atomic<bool>& flag) {
  flag = true;
  co_return;
}

}  // namespace

// Small elements that are cheap to move.
TEST(ParallelQuicksort, uint32) { testValueType<Uint32Values>(400); }

// Elements that are not trivially copyable.
TEST(ParallelQuicksort, strings) { testValueType<StringValues>(200); }

// The rows of an `IdTable` with a static and a dynamic number of columns.
TEST(ParallelQuicksort, idTableRows) {
  testValueType<IdTableValues<3>>(150);
  testValueType<IdTableValues<0>>(150);
}

// The default number of elements per task shrinks with the size of the
// elements, down to a minimum.
TEST(ParallelQuicksort, defaultMaxElementsPerTask) {
  EXPECT_EQ(defaultMaxElementsPerTask<char>(), size_t{1} << 18);
  EXPECT_EQ(defaultMaxElementsPerTask<uint32_t>(), size_t{1} << 17);
  EXPECT_EQ((defaultMaxElementsPerTask<std::array<char, 64>>()),
            size_t{1} << 15);
  EXPECT_EQ((defaultMaxElementsPerTask<std::array<char, 4096>>()),
            size_t{1} << 13);
}

// The parallel and the serial quicksort throw a `CancellationException` when
// their handle is cancelled before or during the sort, and otherwise sort.
TEST(ParallelQuicksort, cancellation) {
  using ad_utility::CancellationException;
  using ad_utility::CancellationHandle;
  using ad_utility::CancellationState;
  constexpr size_t maxElementsPerTask = 64;
  std::mt19937_64 gen{3};
  auto keys = makeKeys(Shape::Random, 100 * maxElementsPerTask, gen);
  auto expected = keys;
  ql::ranges::sort(expected);

  // Sort a copy of the `keys` with the comparator `comp` and the `handle`.
  auto sortParallel = [&keys](CancellationHandle<>& handle, auto comp) {
    auto values = Uint32Values::fromKeys(keys);
    SortState state{comp, maxElementsPerTask, threadPool().get_executor(),
                    [&handle] { handle.throwIfCancelled(); }};
    runOnPool(parallelQuicksort(state, values.begin(), values.end()));
    return Uint32Values::toKeys(values);
  };
  auto sortSerial = [&keys](CancellationHandle<>& handle, auto comp) {
    auto values = Uint32Values::fromKeys(keys);
    serialQuicksort(values.begin(), values.end(), comp, maxElementsPerTask,
                    [&handle] { handle.throwIfCancelled(); });
    return Uint32Values::toKeys(values);
  };

  auto test = [&expected](auto sort) {
    CancellationHandle<> handle;
    EXPECT_EQ(sort(handle, std::less<>{}), expected);
    handle.cancel(CancellationState::MANUAL);
    EXPECT_THROW(sort(handle, std::less<>{}), CancellationException);
    // Cancelled by the comparator during the first partition.
    CancellationHandle<> handle2;
    std::atomic<size_t> numComparisons = 0;
    auto cancellingLess = [&handle2, &numComparisons](uint32_t a, uint32_t b) {
      if (++numComparisons == 100) {
        handle2.cancel(CancellationState::TIMEOUT);
      }
      return a < b;
    };
    EXPECT_THROW(sort(handle2, cancellingLess), CancellationException);
  };
  test(sortParallel);
  test(sortSerial);
}

// The linear passes call `checkCancellation` once per
// `ELEMENTS_PER_CANCELLATION_CHECK` elements (and after the last, partial
// chunk), and an exception of `checkCancellation` aborts them.
TEST(ParallelQuicksort, linearPassesCheckCancellation) {
  auto throwing = [] { throw TestException{"cancelled"}; };
  size_t numChecks = 0;
  auto counting = [&numChecks] { ++numChecks; };
  constexpr size_t big = 3 * ELEMENTS_PER_CANCELLATION_CHECK;
  std::vector<uint32_t> ascending(big);
  std::iota(ascending.begin(), ascending.end(), 0u);
  auto descending = ascending;
  ql::ranges::reverse(descending);
  std::less<> less;

  EXPECT_TRUE(isSorted(ascending.begin(), ascending.end(), less, counting));
  EXPECT_EQ(numChecks, 3);
  numChecks = 0;
  EXPECT_TRUE(
      isDescending(descending.begin(), descending.end(), less, counting));
  EXPECT_EQ(numChecks, 3);
  numChecks = 0;
  auto values = descending;
  reverseRange(values.begin(), values.end(), counting);
  EXPECT_EQ(values, ascending);
  EXPECT_EQ(numChecks, 3);
  EXPECT_THROW(std::ignore =
                   isSorted(ascending.begin(), ascending.end(), less, throwing),
               TestException);
  EXPECT_THROW(reverseRange(values.begin(), values.end(), throwing),
               TestException);

  // An inversion is found anywhere, and ranges of any size are reversed.
  values = ascending;
  std::swap(values[ELEMENTS_PER_CANCELLATION_CHECK - 1],
            values[ELEMENTS_PER_CANCELLATION_CHECK]);
  EXPECT_FALSE(isSorted(values.begin(), values.end(), less, counting));
  for (size_t size : {size_t{0}, size_t{1}, size_t{7}, big + 1}) {
    std::vector<uint32_t> input(size);
    std::iota(input.begin(), input.end(), 0u);
    auto expected = input;
    ql::ranges::reverse(expected);
    reverseRange(input.begin(), input.end(), counting);
    EXPECT_EQ(input, expected) << size;
  }

  // The partition of a big range.
  values = ascending;
  ql::ranges::shuffle(values, std::mt19937_64{5});
  auto copy = values;
  computePivotAndMoveToFront(values.begin(), values.end(), less);
  EXPECT_THROW(std::ignore =
                   hoarePartition(values.begin(), values.end(), less, throwing),
               TestException);
  // Even a cancelled partition keeps all elements.
  ql::ranges::sort(values);
  EXPECT_EQ(values, ascending);
  computePivotAndMoveToFront(copy.begin(), copy.end(), less);
  auto [leftEnd, rightBegin] =
      hoarePartition(copy.begin(), copy.end(), less, ad_utility::noop);
  EXPECT_TRUE(ql::ranges::all_of(copy.begin(), leftEnd,
                                 [&](uint32_t x) { return x <= *leftEnd; }));
  EXPECT_TRUE(ql::ranges::all_of(rightBegin, copy.end(),
                                 [&](uint32_t x) { return x >= *leftEnd; }));
}

// `median3` sorts its three elements in place and returns the middle one, for
// all orders of the elements, also with duplicates.
TEST(ParallelQuicksort, median3) {
  for (std::array<int, 3> sorted :
       {std::array{1, 2, 3}, std::array{1, 2, 2}, std::array{1, 1, 2}}) {
    auto values = sorted;
    do {
      auto input = values;
      auto median = median3(input.begin(), input.begin() + 1, input.begin() + 2,
                            std::less<>{});
      EXPECT_EQ(median, input.begin() + 1) << ::testing::PrintToString(values);
      EXPECT_EQ(input, sorted) << ::testing::PrintToString(values);
    } while (std::next_permutation(values.begin(), values.end()));
  }
}

// Concurrent sorts on the same executor don't interfere.
TEST(ParallelQuicksort, concurrentSortsOnTheSameExecutor) {
  constexpr size_t numSorts = 4;
  std::vector<std::vector<Key>> keys;
  std::vector<Uint32Values::Container> inputs;
  std::mt19937_64 gen{11};
  for (size_t i = 0; i < numSorts; ++i) {
    keys.push_back(makeKeys(Shape::Random, 50'000, gen));
    inputs.push_back(Uint32Values::fromKeys(keys.back()));
  }
  std::vector<std::thread> callers;
  for (auto& input : inputs) {
    callers.emplace_back([&input] { runSortOn<Uint32Values>(input, {}, 64); });
  }
  for (auto& caller : callers) {
    caller.join();
  }
  for (size_t i = 0; i < numSorts; ++i) {
    ql::ranges::sort(keys[i]);
    EXPECT_EQ(Uint32Values::toKeys(inputs[i]), keys[i]);
  }
}

// If both the body and a child throw, the first of the two exceptions reaches
// the caller.
TEST(TaskGroup, keepsFirstException) {
  ql::any_io_executor executor = threadPool().get_executor();
  std::atomic<bool> stopped{false};
  std::atomic<bool> childStarted{false};
  auto body = [&childStarted](TaskGroup& group) {
    group.spawnFunction([&childStarted] {
      childStarted = true;
      throw TestException{"child failed"};
    });
    while (!childStarted) {
      std::this_thread::yield();
    }
    throw TestException{"body failed"};
  };
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      runOnPool(TaskGroup::withChildren(executor, stopped, body)),
      ::testing::AnyOf(::testing::StrEq("child failed"),
                       ::testing::StrEq("body failed")),
      TestException);
}

// `withChildren` runs all children and waits for them, also for those that are
// still running when the body is done.
TEST(TaskGroup, waitsForChildren) {
  ql::any_io_executor executor = threadPool().get_executor();
  std::atomic<bool> stopped{false};
  std::atomic<bool> slowFinished{false};
  std::atomic<bool> flag{false};
  runOnPool(TaskGroup::withChildren(executor, stopped, [&](TaskGroup& group) {
    spawnSlowChild(group, slowFinished);
    group.spawn(setFlag(flag));
  }));
  EXPECT_TRUE(slowFinished);
  EXPECT_TRUE(flag);
  EXPECT_FALSE(stopped);

  // A group without children.
  runOnPool(TaskGroup::withChildren(executor, stopped, [](TaskGroup&) {}));

  // A child that spawns another child into its own group.
  std::atomic<bool> grandchildRan{false};
  runOnPool(TaskGroup::withChildren(executor, stopped, [&](TaskGroup& group) {
    group.spawnFunction(
        [&group, &grandchildRan] { group.spawn(setFlag(grandchildRan)); });
  }));
  EXPECT_TRUE(grandchildRan);
}

// The first exception of the body or of a child reaches the caller, but only
// after all children have finished, and it stops the sort.
TEST(TaskGroup, propagatesExceptions) {
  ql::any_io_executor executor = threadPool().get_executor();
  auto expectThrows = [&](auto body, std::string_view message) {
    std::atomic<bool> stopped{false};
    std::atomic<bool> slowFinished{false};
    AD_EXPECT_THROW_WITH_MESSAGE(
        runOnPool(TaskGroup::withChildren(executor, stopped,
                                          [&](TaskGroup& group) {
                                            spawnSlowChild(group, slowFinished);
                                            body(group);
                                          })),
        ::testing::StrEq(message));
    EXPECT_TRUE(slowFinished);
    EXPECT_TRUE(stopped);
  };
  expectThrows([](TaskGroup&) { throw TestException{"body failed"}; },
               "body failed");
  expectThrows(
      [](TaskGroup& group) {
        group.spawnFunction([] { throw TestException{"function failed"}; });
      },
      "function failed");
  expectThrows([](TaskGroup& group) { group.spawn(throwingCoroutine()); },
               "coroutine failed");

  // Both children of `runConcurrently`.
  std::atomic<bool> stopped{false};
  std::atomic<bool> flag{false};
  AD_EXPECT_THROW_WITH_MESSAGE(
      runOnPool(TaskGroup::runConcurrently(executor, stopped,
                                           throwingCoroutine(), setFlag(flag))),
      ::testing::StrEq("coroutine failed"));
  std::atomic<bool> stopped2{false};
  AD_EXPECT_THROW_WITH_MESSAGE(
      runOnPool(TaskGroup::runConcurrently(executor, stopped2, setFlag(flag),
                                           throwingCoroutine())),
      ::testing::StrEq("coroutine failed"));
  EXPECT_TRUE(flag);
}

// Once the sort is stopped, spawned children don't start anymore.
TEST(TaskGroup, skipsChildrenWhenStopped) {
  ql::any_io_executor executor = threadPool().get_executor();
  std::atomic<bool> stopped{true};
  std::atomic<bool> ran{false};
  runOnPool(
      TaskGroup::withChildren(executor, stopped, [&ran](TaskGroup& group) {
        group.spawnFunction([&ran] { ran = true; });
        group.spawn(setFlag(ran));
      }));
  EXPECT_FALSE(ran);
  // Only the spawned child of `runConcurrently` is skipped.
  std::atomic<bool> inlinedRan{false};
  runOnPool(TaskGroup::runConcurrently(executor, stopped, setFlag(inlinedRan),
                                       setFlag(ran)));
  EXPECT_TRUE(inlinedRan);
  EXPECT_FALSE(ran);
}

// The parent is resumed on the executor of its coroutine (here a strand), not
// on the thread of the child that finished last. Without children, the parent
// is resumed from within `join()`, which is a separate code path.
TEST(TaskGroup, resumesParentOnItsExecutor) {
  auto strand =
      net::make_strand(ql::any_io_executor{threadPool().get_executor()});
  for (size_t numChildren : {0, 1, 5}) {
    EXPECT_TRUE(net::co_spawn(strand, isOnStrandAfterJoin(strand, numChildren),
                              net::use_future)
                    .get())
        << "numChildren=" << numChildren;
  }
}
