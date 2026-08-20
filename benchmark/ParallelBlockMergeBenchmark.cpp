// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>

#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/Log.h"
#include "util/ParallelBlockMerge.h"
#include "util/Random.h"

// Benchmarks for `ad_utility::parallelBlockMerge::parallelBlockMergeToRange`.
// Three axes are varied, each of them along a dimension that a real caller
// actually moves along:
//
// 1. The number `k` of presorted runs (4, 16, 64, 256) at a constant total
//    number of elements. The index sorter merges a handful of runs, the
//    vocabulary merger tens to hundreds of them.
// 2. The cost of the comparator: a plain `std::less<size_t>` on `size_t` versus
//    a deliberately expensive comparison of strings. The latter is the case
//    that matters most, because the comparator of the vocabulary merger is a
//    full ICU collation which dominates its runtime.
// 3. The available parallelism: a serial `InlineMergeScheduler` as the
//    reference, and a `TaskQueueMergeScheduler` with 1, 2, 4, and 8 threads.
//
// In an optimized build, the whole benchmark takes roughly half a minute on a
// many-core machine (of which about a third is the generation and the sorting
// of the input data) and needs about one gigabyte of memory. A `Debug` build is
// an order of magnitude slower and its numbers are not meaningful.
namespace ad_benchmark {

using namespace ad_utility::parallelBlockMerge;

namespace {

// The total number of elements that is merged for the cheap comparator. Larger
// than for the expensive one, because a single comparison is much cheaper.
constexpr size_t NUM_ELEMENTS_CHEAP = 16'000'000;

// The total number of elements that is merged for the expensive comparator.
constexpr size_t NUM_ELEMENTS_EXPENSIVE = 4'000'000;

// The numbers of presorted runs for which the merge is benchmarked.
constexpr std::array<size_t, 4> RUN_COUNTS{4, 16, 64, 256};

// The numbers of threads of the `TaskQueueMergeScheduler`.
constexpr std::array<size_t, 4> THREAD_COUNTS{1, 2, 4, 8};

// The number of elements in a single (virtual) input block.
//
// NOTE: This must be chosen small enough that `numChunks * numRuns *
// VIRTUAL_BLOCK_SIZE` stays well below the total number of elements. The reason
// is that a chunk reads one partial input block per run at each of its two
// boundaries, and although the superfluous elements of such a block are never
// merged, they are still read. If that overhead is not negligible, then the
// merge stops scaling with the number of threads, because the overhead grows
// with the number of chunks and therefore with the number of threads.
constexpr size_t VIRTUAL_BLOCK_SIZE = 512;

// A deliberately expensive comparator on strings. It compares the strings
// character-wise, ignoring the case as well as all non-alphanumeric characters,
// and falls back to the plain string comparison for ties, such that it is a
// total order. This is a (much cheaper, but still much more expensive than a
// `memcmp`) stand-in for the ICU collation of the vocabulary merger.
struct ExpensiveStringComparator {
  // Return `true` if `a` is smaller than `b`.
  bool operator()(const std::string& a, const std::string& b) const {
    int comparison = compare(a, b);
    if (comparison != 0) {
      return comparison < 0;
    }
    return a < b;
  }

 private:
  // Return a negative value if `a` is smaller than `b`, a positive value if it
  // is greater, and zero if they are equal wrt this collation.
  static int compare(const std::string& a, const std::string& b) {
    size_t i = 0;
    size_t j = 0;
    while (true) {
      while (i < a.size() && !isRelevant(a[i])) {
        ++i;
      }
      while (j < b.size() && !isRelevant(b[j])) {
        ++j;
      }
      if (i == a.size() || j == b.size()) {
        if (i == a.size() && j == b.size()) {
          return 0;
        }
        return i == a.size() ? -1 : 1;
      }
      int weightA = weight(a[i]);
      int weightB = weight(b[j]);
      if (weightA != weightB) {
        return weightA < weightB ? -1 : 1;
      }
      ++i;
      ++j;
    }
  }

  // Return `true` if the character contributes to the comparison at all.
  static bool isRelevant(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) != 0;
  }

  // Return the collation weight of a single (relevant) character.
  static int weight(char c) {
    return std::tolower(static_cast<unsigned char>(c));
  }
};

// The presorted runs of a single configuration. The `spans_` refer into the
// `storage_`, such that handing the runs to a `VectorRunsInput` does not copy
// the actual data.
template <typename T>
struct Runs {
  std::vector<std::vector<T>> storage_;
  std::vector<ql::span<const T>> spans_;
};

// The input policy for the runs of type `T`.
template <typename T>
using Input = VectorRunsInput<ql::span<const T>>;

static_assert(BlockedRunsInput<Input<size_t>>);
static_assert(BlockedRunsInput<Input<std::string>>);

// Distribute the elements of the globally `sorted` vector randomly over
// `numRuns` runs. Every resulting run is sorted, because the elements are
// appended in the order in which they appear in `sorted`. This models the runs
// of an external sort, each of which covers the complete value range.
template <typename T>
Runs<T> makeRuns(const std::vector<T>& sorted, size_t numRuns) {
  Runs<T> result;
  result.storage_.resize(numRuns);
  ad_utility::FastRandomIntGenerator<uint64_t> generator;
  for (const auto& element : sorted) {
    result.storage_[generator() % numRuns].push_back(element);
  }
  result.spans_.reserve(numRuns);
  for (const auto& run : result.storage_) {
    result.spans_.emplace_back(run.data(), run.size());
  }
  return result;
}

// Return a cheap checksum of a single merged element. The checksums are
// accumulated and logged, so that the compiler cannot elide the merge.
size_t elementChecksum(size_t element) { return element; }

// ____________________________________________________________________________
size_t elementChecksum(const std::string& element) {
  return element.size() + static_cast<unsigned char>(element.front());
}

// Merge the `runs` with the given `comparator` and `scheduler`, and return the
// accumulated checksum of all merged elements.
template <typename T, typename Comparator>
size_t mergeAndComputeChecksum(const Runs<T>& runs, Comparator comparator,
                               SharedMergeScheduler scheduler) {
  auto blocks = parallelBlockMergeToRange</*moveElements=*/false>(
      Input<T>{runs.spans_, VIRTUAL_BLOCK_SIZE}, std::move(comparator),
      MergeOptions{}, std::move(scheduler));
  size_t checksum = 0;
  for (const auto& block : blocks) {
    for (const auto& element : block) {
      checksum += elementChecksum(element);
    }
  }
  return checksum;
}

// Return `numElements` random `size_t` values in sorted order.
std::vector<size_t> makeSortedNumbers(size_t numElements) {
  ad_utility::FastRandomIntGenerator<uint64_t> generator;
  std::vector<size_t> result;
  result.reserve(numElements);
  for (size_t i = 0; i < numElements; ++i) {
    result.push_back(generator());
  }
  ql::ranges::sort(result);
  return result;
}

// Return `numElements` random strings, sorted according to the `comparator`.
// The strings are long enough to not fit into the small string optimization,
// which is also the case for most of the strings of the vocabulary.
std::vector<std::string> makeSortedStrings(
    size_t numElements, const ExpensiveStringComparator& comparator) {
  static constexpr std::string_view alphabet =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_.";
  ad_utility::FastRandomIntGenerator<uint64_t> generator;
  std::vector<std::string> result;
  result.reserve(numElements);
  for (size_t i = 0; i < numElements; ++i) {
    size_t length = 16 + generator() % 24;
    std::string element;
    element.reserve(length);
    for (size_t j = 0; j < length; ++j) {
      element.push_back(alphabet[generator() % alphabet.size()]);
    }
    result.push_back(std::move(element));
  }
  ql::ranges::sort(result, comparator);
  return result;
}
}  // namespace

// The benchmarks of the parallel block-wise k-way merge, see the comment at the
// top of this file for the axes that are covered.
class ParallelBlockMergeBenchmark : public BenchmarkInterface {
 private:
  // The accumulated checksum of all merges. It is logged at the very end, so
  // that the compiler cannot optimize the merging away.
  size_t checksum_ = 0;

 public:
  // ___________________________________________________________________________
  std::string name() const final {
    return "Benchmarks for the parallel block-wise k-way merge";
  }

  // ___________________________________________________________________________
  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    addTableFor(results, "Cheap comparator (`std::less` on `size_t`)",
                makeSortedNumbers(NUM_ELEMENTS_CHEAP), std::less<size_t>{});
    ExpensiveStringComparator expensiveComparator;
    addTableFor(results,
                "Expensive comparator (collation-like on `std::string`)",
                makeSortedStrings(NUM_ELEMENTS_EXPENSIVE, expensiveComparator),
                expensiveComparator);

    AD_LOG_INFO << "The checksum of all merged elements was " << checksum_
                << std::endl;
    return results;
  }

 private:
  // Add a table for the given `comparator` to the `results`. It has one row per
  // entry of `RUN_COUNTS` and one column for the serial reference plus one
  // column per entry of `THREAD_COUNTS`. The `sorted` elements are distributed
  // anew over the runs for every row, but they are generated and sorted only
  // once, because that setup is not part of the measurement.
  template <typename T, typename Comparator>
  void addTableFor(BenchmarkResults& results, const std::string& descriptor,
                   const std::vector<T>& sorted, const Comparator& comparator) {
    std::vector<std::string> rowNames;
    for (size_t numRuns : RUN_COUNTS) {
      rowNames.push_back(absl::StrCat(numRuns));
    }
    // NOTE: The first column is filled with the row names by the
    // infrastructure, so the measurements start at column one.
    std::vector<std::string> columnNames{"number of runs", "serial (inline)"};
    for (size_t numThreads : THREAD_COUNTS) {
      columnNames.push_back(absl::StrCat("task queue, ", numThreads, " thr"));
    }
    auto& table = results.addTable(descriptor, rowNames, columnNames);

    for (size_t row = 0; row < RUN_COUNTS.size(); ++row) {
      Runs<T> runs = makeRuns(sorted, RUN_COUNTS.at(row));
      auto measure = [this, &table, &runs, &comparator, row](
                         size_t column, SharedMergeScheduler scheduler) {
        table.addMeasurement(
            row, column, [this, &runs, &comparator, &scheduler]() {
              checksum_ += mergeAndComputeChecksum(runs, comparator, scheduler);
            });
      };
      measure(1, std::make_shared<InlineMergeScheduler>());
      for (size_t i = 0; i < THREAD_COUNTS.size(); ++i) {
        // NOTE: The scheduler is created outside of the measured function, so
        // that the creation of its threads is not part of the measurement.
        // NOTE: A `TaskQueueMergeScheduler` with a single thread offers no
        // parallelism at all, so the merge falls back to the same serial code
        // path as the `InlineMergeScheduler`. That column is therefore expected
        // to be (almost) identical to the serial reference.
        measure(i + 2,
                std::make_shared<TaskQueueMergeScheduler>(THREAD_COUNTS.at(i)));
      }
    }
  }
};

AD_REGISTER_BENCHMARK(ParallelBlockMergeBenchmark);
}  // namespace ad_benchmark
