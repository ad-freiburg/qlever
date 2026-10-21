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
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
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
#include "util/blockSort/BlockIndirectSort.h"

// The tests of this file are organized around one fuzzer per element type
// (`fuzzSort`), which sorts inputs of random shapes and sizes with random
// tuning parameters, thread counts, and failing comparators, and compares the
// result with `std::sort`. The small tuning parameters make every code path of
// the algorithm reachable with small inputs.
//
// NOTE: All sorts of one element type use the same comparator type
// (`FuzzCompare`), so that they all use (and cover) the same instantiation of
// the algorithm.
namespace {

namespace net = boost::asio;
using ad_utility::blockSort::blockIndirectSort;
using ad_utility::blockSort::blockIndirectSortAsync;
using namespace ad_utility::blockSort::detail;

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
// in the algorithm (sorted parts, reversed parts, runs of equal elements, a
// tail that belongs elsewhere, blocks that don't overlap, ...).
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

// Sort `values` with `runSort` (so with explicit tuning parameters) on the
// shared thread pool.
template <typename Values>
void runSortOn(typename Values::Container& values,
               const FuzzCompare<typename Values::Less>& comp,
               uint32_t numThreads, SortParams params) {
  auto range = Values::range(values);
  runSort(range.begin(), range.end(), comp, numThreads,
          threadPool().get_executor(), params);
}

// Sort `numIterations` random inputs of `Values` with random tuning
// parameters, thread counts and shapes, and expect the result of `std::sort`.
// With `withFailures`, the comparator throws at a random point (or not at all
// if the sort needs fewer comparisons), in which case the exception has to
// reach the caller.
template <typename Values>
void fuzzSort(size_t numIterations, bool withFailures, size_t maxBlockSize) {
  auto seed = std::random_device{}();
  SCOPED_TRACE(absl::StrCat("seed=", seed));
  std::mt19937_64 gen{seed};
  auto random = [&gen](size_t lower, size_t upper) {
    return std::uniform_int_distribution<size_t>{lower, upper}(gen);
  };
  for (size_t iteration = 0; iteration < numIterations; ++iteration) {
    SortParams params{random(1, maxBlockSize), random(16, 128)};
    auto numThreads = static_cast<uint32_t>(random(1, 12));
    // Sizes around the one from which the block indirect part is used.
    size_t blockPathSize =
        size_t{minNumThreadsForBlocks} * params.blockSize * BLOCKS_PER_TASK;
    size_t numKeys =
        random(0, 3) == 0 ? random(0, 40) : random(0, 3 * blockPathSize);
    if (random(0, 3) == 0) {
      // An input without a tail.
      numKeys -= numKeys % params.blockSize;
    }
    auto shape = static_cast<Shape>(random(0, numShapes - 1));
    SCOPED_TRACE(absl::StrCat(
        "iteration=", iteration, " shape=", static_cast<int>(shape),
        " numKeys=", numKeys, " blockSize=", params.blockSize,
        " maxElementsPerTask=", params.maxElementsPerTask,
        " numThreads=", numThreads));
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

    bool threw = false;
    try {
      runSortOn<Values>(values, comp, numThreads, params);
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

// Run `awaitable` on the shared thread pool, wait for it, and rethrow its
// exception.
void runOnPool(net::awaitable<void> awaitable) {
  net::co_spawn(threadPool().get_executor(), std::move(awaitable),
                net::use_future)
      .get();
}

// The tests for one value type: the fuzzer with and without failures, and the
// checks of the public interface, blocking and awaitable (with the default
// tuning parameters).
template <typename Values>
void testValueType(size_t numIterations, size_t maxBlockSize) {
  fuzzSort<Values>(numIterations, false, maxBlockSize);
  fuzzSort<Values>(numIterations, true, maxBlockSize);
  using Less = FuzzCompare<typename Values::Less>;
  auto executor = threadPool().get_executor();

  // The public interface rejects an empty executor.
  auto values = Values::fromKeys({3, 1, 2});
  AD_EXPECT_THROW_WITH_MESSAGE(
      blockIndirectSort(Values::range(values), Less{}, numPoolThreads,
                        ql::any_io_executor{}),
      ::testing::HasSubstr("exec"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      runOnPool(blockIndirectSortAsync(Values::range(values), Less{},
                                       numPoolThreads, ql::any_io_executor{})),
      ::testing::HasSubstr("exec"));

  // The public interface sorts.
  std::mt19937_64 gen{42};
  auto keys = makeKeys(Shape::Random, 10'000, gen);
  auto sortedKeys = keys;
  ql::ranges::sort(sortedKeys);
  values = Values::fromKeys(keys);
  blockIndirectSort(Values::range(values), Less{}, numPoolThreads, executor);
  EXPECT_EQ(Values::toKeys(values), sortedKeys);
  // An lvalue container is sorted in place, not a copy of it.
  values = Values::fromKeys(keys);
  runOnPool(blockIndirectSortAsync(values, Less{}, numPoolThreads, executor));
  EXPECT_EQ(Values::toKeys(values), sortedKeys);
}

}  // namespace

// _____________________________________________________________________________
// Small elements that are cheap to move.
TEST(BlockIndirectSort, uint32) { testValueType<Uint32Values>(400, 16); }

// _____________________________________________________________________________
// Elements that are not trivially copyable.
TEST(BlockIndirectSort, strings) { testValueType<StringValues>(200, 8); }

// _____________________________________________________________________________
// The rows of an `IdTable` with a static and a dynamic number of columns.
TEST(BlockIndirectSort, idTableRows) {
  testValueType<IdTableValues<3>>(150, 4);
  testValueType<IdTableValues<0>>(150, 4);
}

// _____________________________________________________________________________
// The public interface with its default tuning parameters and an input that is
// big enough for all of its phases.
TEST(BlockIndirectSort, defaultParameters) {
  size_t numKeys =
      size_t{numPoolThreads} * blockSizeFor<uint32_t>() * BLOCKS_PER_TASK;
  // Distinct keys in random order, so that the expected result is known without
  // a (slow) reference sort.
  std::vector<Key> expected(numKeys);
  std::iota(expected.begin(), expected.end(), Key{0});
  auto values = Uint32Values::fromKeys(expected);
  ql::ranges::shuffle(values, std::mt19937_64{7});
  // An lvalue range is sorted in place.
  blockIndirectSort(values, FuzzCompare<std::less<>>{}, numPoolThreads,
                    threadPool().get_executor());
  EXPECT_EQ(Uint32Values::toKeys(values), expected);
}

// _____________________________________________________________________________
// Both functions of the public interface accept lvalue containers as well as
// views like `ql::span`.
namespace {
template <typename Range>
constexpr bool canSort = requires(Range&& range) {
  blockIndirectSort(std::forward<Range>(range), std::less<>{}, 1,
                    ql::any_io_executor{});
};
template <typename Range>
constexpr bool canSortAsync = requires(Range&& range) {
  blockIndirectSortAsync(std::forward<Range>(range), std::less<>{}, 1,
                         ql::any_io_executor{});
};
static_assert(canSort<std::vector<uint32_t>&>);
static_assert(canSort<ql::span<uint32_t>>);
static_assert(canSortAsync<std::vector<uint32_t>&>);
static_assert(canSortAsync<ql::span<uint32_t>>);
}  // namespace

// _____________________________________________________________________________
// Concurrent sorts on the same executor don't interfere.
TEST(BlockIndirectSort, concurrentSortsOnTheSameExecutor) {
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
    callers.emplace_back([&input] {
      runSortOn<Uint32Values>(input, {}, numPoolThreads, {16, 64});
    });
  }
  for (auto& caller : callers) {
    caller.join();
  }
  for (size_t i = 0; i < numSorts; ++i) {
    ql::ranges::sort(keys[i]);
    EXPECT_EQ(Uint32Values::toKeys(inputs[i]), keys[i]);
  }
}

// _____________________________________________________________________________
// `ScratchBuffers` allocates a new buffer if all are taken, and reuses a buffer
// once it has been returned.
TEST(BlockIndirectSort, scratchBuffers) {
  ScratchBuffers<int> buffers{3, 7};
  int* reused = nullptr;
  {
    auto first = buffers.acquire();
    auto second = buffers.acquire();
    EXPECT_NE(first.range().first, second.range().first);
    EXPECT_EQ(std::vector(first.range().first, first.range().last),
              std::vector({7, 7, 7}));
    reused = first.range().first;
  }
  // The buffer that was returned last is handed out first.
  auto third = buffers.acquire();
  EXPECT_EQ(third.range().first, reused);
}
