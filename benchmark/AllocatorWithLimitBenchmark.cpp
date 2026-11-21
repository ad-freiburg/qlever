// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Measure the overhead of the memory-limit enforcing `AllocatorWithLimit`
// compared to `std::allocator` for small allocations, which are typical for
// strings and small vectors. All threads share the same memory limit, as do all
// the threads of a query in the server. Each thread performs the same number of
// operations, so without any overhead, the time would be independent of the
// number of threads (as long as there are enough cores).

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "util/AllocatorWithLimit.h"
#include "util/CompilerExtensions.h"

namespace ad_benchmark {
namespace {

using namespace ad_utility::memory_literals;

constexpr size_t numOperationsPerThread = 1'000'000;
const std::vector<size_t> numThreadsToTest{1, 4, 16, 32};

// Prevent the compiler from optimizing away the measured operations.
std::atomic<size_t> sink = 0;

template <typename Allocator>
using String = std::basic_string<char, std::char_traits<char>, Allocator>;

// Allocate and free via `::operator new` and `::operator delete` in functions
// that are not inlined. With a plain `std::allocator`, the compiler is allowed
// to elide matching allocations and deallocations, which would make the
// baseline unrealistically fast.
AD_NO_INLINE void* allocateBytes(size_t n) { return ::operator new(n); }
AD_NO_INLINE void deallocateBytes(void* p) { ::operator delete(p); }

// A stateless allocator that uses the two functions above.
template <typename T>
struct NonElidableAllocator {
  using value_type = T;
  NonElidableAllocator() = default;
  template <typename U>
  NonElidableAllocator(const NonElidableAllocator<U>&) {}
  T* allocate(size_t n) {
    return static_cast<T*>(allocateBytes(n * sizeof(T)));
  }
  void deallocate(T* p, size_t) { deallocateBytes(p); }
  template <typename U>
  bool operator==(const NonElidableAllocator<U>&) const {
    return true;
  }
  template <typename U>
  bool operator!=(const NonElidableAllocator<U>&) const {
    return false;
  }
};

// Copy and destroy a string that is too long for the small-string
// optimization. This requires one allocation, one deallocation, and one copy
// of the allocator.
template <typename CharAllocator>
AD_NO_INLINE void copyStrings(const CharAllocator& allocator) {
  String<CharAllocator> original(
      "a string that is too long for the small-string optimization", allocator);
  size_t sum = 0;
  for (size_t i = 0; i < numOperationsPerThread; ++i) {
    String<CharAllocator> copy{original};
    copy[i % copy.size()] = 'b';
    sum += static_cast<size_t>(copy[i % copy.size()]);
  }
  sink += sum;
}

// Fill a small vector by `push_back`, which allocates and frees blocks of
// growing sizes.
template <typename CharAllocator>
AD_NO_INLINE void fillSmallVectors(const CharAllocator& allocator) {
  using IntAllocator =
      typename std::allocator_traits<CharAllocator>::template rebind_alloc<int>;
  size_t sum = 0;
  constexpr size_t vectorSize = 32;
  for (size_t i = 0; i < numOperationsPerThread / vectorSize; ++i) {
    std::vector<int, IntAllocator> vec{IntAllocator{allocator}};
    for (size_t j = 0; j < vectorSize; ++j) {
      vec.push_back(static_cast<int>(i + j));
    }
    sum += static_cast<size_t>(vec.back());
  }
  sink += sum;
}

// Allocate and free 60 bytes via the same allocator, without copying it.
template <typename CharAllocator>
AD_NO_INLINE void allocateDirectly(const CharAllocator& allocator) {
  CharAllocator copy{allocator};
  size_t sum = 0;
  for (size_t i = 0; i < numOperationsPerThread; ++i) {
    char* p = copy.allocate(60);
    p[i % 60] = 'a';
    sum += static_cast<size_t>(p[i % 60]);
    copy.deallocate(p, 60);
  }
  sink += sum;
}

// Only copy and destroy the allocator.
template <typename CharAllocator>
AD_NO_INLINE void copyAllocators(const CharAllocator& allocator) {
  size_t sum = 0;
  for (size_t i = 0; i < numOperationsPerThread; ++i) {
    CharAllocator copy{allocator};
    sum += copy == allocator;
  }
  sink += sum;
}

// Run `workload(allocator)` on `numThreads` threads concurrently.
template <typename Allocator, typename Workload>
void runOnThreads(size_t numThreads, const Allocator& allocator,
                  const Workload& workload) {
  std::vector<std::thread> threads;
  for (size_t i = 0; i < numThreads; ++i) {
    threads.emplace_back([&]() { workload(allocator); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

class AllocatorWithLimitBenchmark : public BenchmarkInterface {
 public:
  [[nodiscard]] std::string name() const final {
    return "Small allocations via `AllocatorWithLimit` vs. `std::allocator`";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    // A limit that is never reached, as for the server.
    // The owning allocator is alive during all measurements, so the
    // non-owning copy can be used, as it is done for the allocator of the
    // server.
    auto limitedAllocator = ad_utility::makeAllocatorWithLimit<char>(100_GB);
    auto nonOwningAllocator = limitedAllocator.nonOwningCopy();
    NonElidableAllocator<char> stdAllocator;

    auto addTable = [&](const std::string& name, const auto& workload) {
      std::vector<std::string> rowNames;
      for (size_t numThreads : numThreadsToTest) {
        rowNames.push_back(std::to_string(numThreads) + " threads");
      }
      auto& table = results.addTable(
          name, rowNames,
          {"", "std::allocator", "AllocatorWithLimit (owning)",
           "AllocatorWithLimit (non-owning)", "Slowdown factor (non-owning)"});
      for (size_t row = 0; row < numThreadsToTest.size(); ++row) {
        size_t numThreads = numThreadsToTest[row];
        table.addMeasurement(row, 1, [&]() {
          runOnThreads(numThreads, stdAllocator, workload);
        });
        table.addMeasurement(row, 2, [&]() {
          runOnThreads(numThreads, limitedAllocator, workload);
        });
        table.addMeasurement(row, 3, [&]() {
          runOnThreads(numThreads, nonOwningAllocator, workload);
        });
        table.setEntry(
            row, 4,
            table.getEntry<float>(row, 3) / table.getEntry<float>(row, 1));
      }
    };

    addTable("Allocate and free 60 bytes (no allocator copy)",
             [](const auto& allocator) { allocateDirectly(allocator); });
    addTable("Copy a 60-character string",
             [](const auto& allocator) { copyStrings(allocator); });
    addTable("Fill a vector<int> of size 32 by push_back",
             [](const auto& allocator) { fillSmallVectors(allocator); });
    addTable("Copy the allocator (no allocation)",
             [](const auto& allocator) { copyAllocators(allocator); });
    return results;
  }
};
}  // namespace

AD_REGISTER_BENCHMARK(AllocatorWithLimitBenchmark);
}  // namespace ad_benchmark
