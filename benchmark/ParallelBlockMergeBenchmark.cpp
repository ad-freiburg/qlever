// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/thread_pool.hpp>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../test/util/AllocatorTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Id.h"
#include "global/IndexTypes.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExternalSortFunctors.h"
#include "util/Log.h"
#include "util/Random.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

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
// 3. The available parallelism: the serial merge as the reference, and a
//    `boost::asio::thread_pool` with 1, 2, 4, and 8 threads.
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

// The numbers of threads of the thread pool that runs the merge.
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

// Merge the `runs` with the given `comparator` on the `executor` with the given
// `parallelism`, and return the accumulated checksum of all merged elements. A
// `parallelism` of one takes the serial code path and never touches the
// `executor`.
template <typename T, typename Comparator>
size_t mergeAndComputeChecksum(const Runs<T>& runs, Comparator comparator,
                               net::any_io_executor executor,
                               size_t parallelism) {
  auto blocks = parallelBlockMergeToRange</*moveElements=*/false>(
      std::move(executor), Input<T>{runs.spans_, VIRTUAL_BLOCK_SIZE},
      std::move(comparator), MergeOptions{}, parallelism);
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
    std::vector<std::string> columnNames{"number of runs", "serial"};
    for (size_t numThreads : THREAD_COUNTS) {
      columnNames.push_back(absl::StrCat("pool, ", numThreads, " thr"));
    }
    auto& table = results.addTable(descriptor, rowNames, columnNames);

    for (size_t row = 0; row < RUN_COUNTS.size(); ++row) {
      Runs<T> runs = makeRuns(sorted, RUN_COUNTS.at(row));
      auto measure = [this, &table, &runs, &comparator, row](
                         size_t column, size_t parallelism) {
        // NOTE: The pool is created (and thus its threads are started) outside
        // of the measured function.
        std::optional<net::thread_pool> pool;
        net::any_io_executor executor;
        if (parallelism > 1) {
          pool.emplace(parallelism);
          executor = pool->get_executor();
        }
        table.addMeasurement(
            row, column, [this, &runs, &comparator, &executor, parallelism]() {
              checksum_ += mergeAndComputeChecksum(runs, comparator, executor,
                                                   parallelism);
            });
        if (pool.has_value()) {
          pool->join();
        }
      };
      measure(1, 1);
      for (size_t i = 0; i < THREAD_COUNTS.size(); ++i) {
        // NOTE: A single thread offers no parallelism at all, so the merge
        // falls back to the same serial code path as the reference column. That
        // column is therefore expected to be (almost) identical to that
        // reference.
        measure(i + 2, THREAD_COUNTS.at(i));
      }
    }
  }
};

AD_REGISTER_BENCHMARK(ParallelBlockMergeBenchmark);

// Benchmarks for the merge phase of the `CompressedExternalIdTableSorter`,
// which is the sorter that the index builder uses. In contrast to the
// benchmarks above, this exercises the complete production code path: the
// presorted runs live compressed on disk, every merged block has to be read
// and decompressed, the elements are multi-column `IdTable` rows that are
// stored column-major, and the comparators are the real ones from
// `index/ExternalSortFunctors.h`.
//
// Only the merge phase is measured. The construction of the sorter and the
// pushing of the rows (which sorts, compresses and writes the runs) happen
// outside of every measured region, and a warm-up merge (also outside of the
// measurement) makes sure that the last run has really been written to disk
// before the first measurement starts.
//
// The following axes are covered, see the individual tables for the details:
// the available parallelism (the serial merge, and a
// `boost::asio::thread_pool` with 1 to 16 threads), the number of columns, the
// distribution of the `Id`s (and thus the number of columns that the
// comparator has to look at), the number of presorted runs, the size of a
// single compressed block, and the size of a single output block.
//
// The last axis is the most important one for the achievable parallelism,
// because the sorter has to split its memory limit between the output blocks
// and the number of chunks that it merges concurrently: every chunk in flight
// holds one decompressed input block per run plus a few output blocks, so
// larger output blocks directly mean fewer concurrent chunks. Small output
// blocks are not free either, because the per-block overhead, the
// synchronization in the sink and the input blocks at the chunk boundaries
// (which are decompressed by both of the adjacent chunks) then start to
// dominate. The sorter resolves that tradeoff in
// `CompressedExternalIdTableSorter::computeMergePhaseParameters`, which the
// logging of this benchmark mirrors, see `mergePhaseParametersFor`. The number
// of rows per chunk is
// `numRows / (numThreads * MergeOptions::targetChunksPerThread)`.
//
// In an optimized build this benchmark takes about eight minutes (of which
// more than half is the generation and the sorting of the input data), needs
// about 1.5 GB of RAM, and creates up to 2 GB of temporary files, which are
// deleted again as soon as a single configuration is done. A `Debug` build is
// an order of magnitude slower and its numbers are not meaningful.
namespace compressedIdTableMerge {
namespace {

using ad_utility::CompressedExternalIdTable;
using ad_utility::CompressedExternalIdTableSorter;
using ad_utility::MemorySize;

// The total number of rows that is pushed into each sorter. Chosen such that a
// single serial merge takes a few seconds, which is long enough to not measure
// noise, and short enough to keep the total runtime of the benchmark sane.
constexpr size_t NUM_ROWS = 48'000'000;

// The number of compressed blocks per presorted run. The size of a single
// compressed block is derived from this, see `blocksizeCompressionFor` below.
constexpr size_t BLOCKS_PER_RUN = 512;

// The numbers of threads of the thread pool that runs the merge.
constexpr std::array<size_t, 5> THREAD_COUNTS_ID_TABLE{1, 2, 4, 8, 16};

// The distribution of the generated `Id`s.
enum class Distribution {
  // Every column is uniformly distributed over a large range. The first column
  // that the comparator looks at then almost always decides, so the comparator
  // effectively is a single comparison of two `Id`s.
  Uniform,
  // Model the triples of a real knowledge graph: there are few distinct
  // predicates, many subjects, and even more objects, and there is only a
  // single graph. During the merge, all runs are at (almost) the same position
  // of the sorted order, so their predicates and often also their subjects are
  // equal, and the comparator has to walk two to three columns.
  Skewed
};

// The number of distinct values per column of the `Skewed` distribution.
constexpr size_t NUM_PREDICATES = 100;
constexpr size_t NUM_SUBJECTS = 20'000;
constexpr size_t NUM_OBJECTS = 10'000'000;
// The number of distinct values of the columns that the comparators ignore.
constexpr size_t NUM_PAYLOAD_VALUES = 1'000;
// The size of the range of the `Uniform` distribution.
constexpr uint64_t UNIFORM_RANGE = uint64_t{1} << 40;

// Generate random rows according to a `Distribution`.
class RowGenerator {
 private:
  ad_utility::FastRandomIntGenerator<uint64_t> generator_;
  Distribution distribution_;

 public:
  // ___________________________________________________________________________
  explicit RowGenerator(Distribution distribution)
      : distribution_{distribution} {}

  // Overwrite `row` with the next random row.
  void next(ql::span<Id> row) {
    auto makeId = [](uint64_t value) {
      return Id::makeFromVocabIndex(VocabIndex::make(value));
    };
    if (distribution_ == Distribution::Uniform) {
      for (Id& id : row) {
        id = makeId(generator_() % UNIFORM_RANGE);
      }
      return;
    }
    row[0] = makeId(generator_() % NUM_SUBJECTS);
    row[1] = makeId(generator_() % NUM_PREDICATES);
    row[2] = makeId(generator_() % NUM_OBJECTS);
    for (size_t i = 3; i < row.size(); ++i) {
      // There is only a single graph, so the graph column never breaks a tie.
      row[i] = makeId(i == ADDITIONAL_COLUMN_GRAPH_ID
                          ? 0
                          : generator_() % NUM_PAYLOAD_VALUES);
    }
  }
};

// The configuration of the data of a single sorter.
struct DataConfig {
  // The number of presorted runs that the sorter creates.
  size_t numRuns_;
  // The distribution of the generated `Id`s.
  Distribution distribution_;
  // The size of a single compressed block. If this is `nullopt`, then it is
  // derived from `BLOCKS_PER_RUN`, see `blocksizeCompressionFor` below.
  std::optional<MemorySize> blocksizeCompression_ = std::nullopt;
};

// The configuration of a single measured merge.
struct MergeConfig {
  // The number of rows of a single output block. If this is `nullopt`, then the
  // sorter derives it from its memory limit, which is what the index builder
  // does.
  std::optional<size_t> outputBlockSize_ = std::nullopt;
  // Ignore the memory limit when the sorter computes how many chunks may be
  // merged concurrently. This is not what the index builder does; it is used
  // for the last table, which measures how much parallelism the merge would
  // offer if the memory limit did not cap it.
  bool ignoreMemoryLimit_ = false;
};

// Return the number of rows of a single presorted run.
constexpr size_t rowsPerRunFor(size_t numRuns) { return NUM_ROWS / numRuns; }

// Return the memory limit for which the sorter creates runs of exactly
// `rowsPerRun` rows. The sorter derives its run size as `memory / (numColumns *
// sizeof(Id) * 2)`, because it holds two runs at the same time (one that is
// being filled and one that is being sorted and written in the background).
MemorySize memoryFor(size_t rowsPerRun, size_t numColumns) {
  return MemorySize::bytes(rowsPerRun * numColumns * sizeof(Id) * 2);
}

// Return the size of a single compressed block, such that a single run
// consists of `BLOCKS_PER_RUN` blocks. Note that keeping the number of blocks
// per run (and not the size of a block) constant is the realistic choice: in a
// real index build a run has hundreds of millions of rows and therefore
// thousands of blocks, and it is this ratio (and not the absolute block size)
// that determines how well the merge can be split into independent chunks.
MemorySize blocksizeCompressionFor(const DataConfig& config) {
  if (config.blocksizeCompression_.has_value()) {
    return config.blocksizeCompression_.value();
  }
  return MemorySize::bytes(rowsPerRunFor(config.numRuns_) * sizeof(Id) /
                           BLOCKS_PER_RUN);
}

// The number of output blocks that the sorter keeps in the pipeline between
// the merge and the caller, see
// `CompressedExternalIdTableSorter::numBufferedOutputBlocks_`.
constexpr size_t NUM_BUFFERED_OUTPUT_BLOCKS = 4;

// The two numbers that the sorter derives from its memory limit for the merge
// phase, see `mergePhaseParametersFor` below.
struct MergePhaseParameters {
  // The number of rows of a single output block.
  size_t outputBlockSize_;
  // The number of chunks that are merged concurrently.
  size_t maxInFlightChunks_;
};

// Return the parameters that the sorter derives for the merge phase of the
// given configuration, if it may merge at most `mergeParallelism` chunks
// concurrently. This mirrors
// `CompressedExternalIdTableSorter::computeMergePhaseParameters`, so that the
// benchmark can log the numbers that the measured merge really uses.
MergePhaseParameters mergePhaseParametersFor(const DataConfig& config,
                                             size_t numColumns,
                                             const MergeConfig& mergeConfig,
                                             size_t mergeParallelism) {
  if (mergeConfig.ignoreMemoryLimit_) {
    // The sorter then yields five rows at a time (unless the caller has pinned
    // the size of the output blocks) and lets all the chunks that the
    // parallelism offers be in flight.
    return {mergeConfig.outputBlockSize_.value_or(5), mergeParallelism};
  }
  const MemorySize memory =
      memoryFor(rowsPerRunFor(config.numRuns_), numColumns);
  // One decompressed input block per run, for a single chunk.
  const MemorySize inputMemoryPerChunk =
      config.numRuns_ * numColumns * blocksizeCompressionFor(config);
  // The largest output block that leaves room for `numInFlight` concurrent
  // chunks, or `nullopt` if the input blocks of those chunks alone already
  // exceed the memory limit.
  auto largestOutputBlockSize =
      [&](size_t numInFlight) -> std::optional<size_t> {
    const MemorySize inputMemory = inputMemoryPerChunk * numInFlight;
    if (inputMemory >= memory) {
      return std::nullopt;
    }
    const size_t numOutputBlocks =
        NUM_BUFFERED_OUTPUT_BLOCKS +
        ad_utility::MERGE_PHASE_OUTPUT_BLOCKS_PER_CHUNK * numInFlight;
    // The sorter never uses more than 1 GB for a single output block, see
    // `CompressedExternalIdTableSorter::maxOutputBlocksize_`.
    const MemorySize perBlock = std::min(
        (memory - inputMemory) / numOutputBlocks, MemorySize::gigabytes(1));
    return perBlock.getBytes() / (sizeof(Id) * numColumns);
  };
  // The sorter uses as much parallelism as the memory limit allows, but never
  // at the price of output blocks below `MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE`
  // rows. An output block size that the caller has pinned takes the place of
  // that minimum, because only the number of chunks is then left to derive.
  const size_t minOutputBlockSize = mergeConfig.outputBlockSize_.value_or(
      ad_utility::MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE);
  auto blockSize = [&mergeConfig](size_t derived) {
    return mergeConfig.outputBlockSize_.value_or(derived);
  };
  for (size_t numInFlight = mergeParallelism; numInFlight > 1; --numInFlight) {
    auto numRows = largestOutputBlockSize(numInFlight);
    if (numRows.has_value() && numRows.value() >= minOutputBlockSize) {
      return {blockSize(numRows.value()), numInFlight};
    }
  }
  // A single chunk (which is exactly the serial merge) gets all the memory that
  // is left. NOTE: If not even that fits, then the sorter throws, which the
  // logged zero makes visible.
  return {blockSize(largestOutputBlockSize(1).value_or(0)), 1};
}

// Return a unique name for a temporary file of this benchmark.
std::string makeFilename() {
  static ad_utility::FastRandomIntGenerator<uint64_t> generator;
  static std::atomic<size_t> counter = 0;
  static const uint64_t prefix = generator();
  return absl::StrCat("ParallelBlockMergeBenchmark.idTable.", prefix, ".",
                      counter++, ".tmp");
}

// Log everything that is needed to interpret a single measured row: the number
// of presorted runs, the memory limit, the number of chunks that may be merged
// concurrently, and the number of output blocks per chunk. The last two numbers
// determine the achievable parallelism, see the comment at the top of this
// namespace.
void logConfiguration(const std::string& name, size_t numColumns,
                      const DataConfig& dataConfig,
                      const MergeConfig& mergeConfig) {
  const size_t rowsPerRun = rowsPerRunFor(dataConfig.numRuns_);
  const MemorySize memory = memoryFor(rowsPerRun, numColumns);
  const MemorySize blocksize = blocksizeCompressionFor(dataConfig);
  // All the numbers below are logged for the largest thread count, which is the
  // one for which the merge has to scale.
  const size_t mergeParallelism = THREAD_COUNTS_ID_TABLE.back();
  const MergePhaseParameters parameters = mergePhaseParametersFor(
      dataConfig, numColumns, mergeConfig, mergeParallelism);
  // The number of chunks is `numThreads * targetChunksPerThread`, so these are
  // the number of rows and the number of output blocks of a single chunk.
  const size_t rowsPerChunk =
      NUM_ROWS /
      (mergeParallelism * ad_utility::parallelBlockMerge::
                              DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD);
  const size_t blocksPerChunk =
      (rowsPerChunk + parameters.outputBlockSize_ - 1) /
      std::max<size_t>(1, parameters.outputBlockSize_);
  AD_LOG_INFO << "[config] " << name << ": " << numColumns << " columns, "
              << dataConfig.numRuns_ << " runs of " << rowsPerRun
              << " rows each, memory limit " << memory.asString()
              << ", compressed blocksize " << blocksize.asString()
              << ", output blocksize " << parameters.outputBlockSize_
              << " rows, at most " << parameters.maxInFlightChunks_
              << " chunks in flight, " << rowsPerChunk << " rows and "
              << blocksPerChunk << " output block(s) per chunk at "
              << mergeParallelism << " threads" << std::endl;
}

// Push `numRows` random rows into the `sorter`. This sorts, compresses and
// writes the presorted runs, and is therefore never part of a measurement.
template <size_t NumCols, typename Sorter>
void pushRows(Sorter& sorter, size_t numRows, Distribution distribution) {
  RowGenerator generator{distribution};
  std::array<Id, NumCols> row{};
  for (size_t i = 0; i < numRows; ++i) {
    generator.next(ql::span<Id>{row});
    sorter.push(row);
  }
}

// Merge the runs of the `sorter` and return the accumulated checksum of the
// first column of all merged rows. This is the function that is measured.
template <size_t NumCols, typename Sorter>
size_t mergeAndComputeChecksum(Sorter& sorter,
                               std::optional<size_t> outputBlockSize) {
  size_t checksum = 0;
  for (const auto& block :
       sorter.template getSortedBlocks<NumCols>(outputBlockSize)) {
    for (const Id& id : block.getColumn(0)) {
      checksum += id.getBits();
    }
  }
  return checksum;
}

// Merge the runs of the `sorter` and return the bits of all merged rows in
// row-major order. Used for the correctness check below.
template <size_t NumCols, typename Sorter>
std::vector<uint64_t> mergeAndMaterialize(Sorter& sorter) {
  std::vector<uint64_t> result;
  for (const auto& block :
       sorter.template getSortedBlocks<NumCols>(std::nullopt)) {
    for (size_t i = 0; i < block.numRows(); ++i) {
      for (size_t col = 0; col < NumCols; ++col) {
        result.push_back(block(i, col).getBits());
      }
    }
  }
  return result;
}

// A sorter that is filled with random rows, together with the temporary file
// that stores its presorted runs. The file is deleted when this object is
// destroyed.
template <typename Comparator, size_t NumCols>
class FilledSorter {
 public:
  using Sorter = CompressedExternalIdTableSorter<Comparator, NumCols>;

 private:
  std::string filename_ = makeFilename();
  std::unique_ptr<Sorter> sorter_;

 public:
  // Create the sorter for the given `config` and push the rows. A warm-up merge
  // (which is not measured by anyone) flushes the last presorted run to disk
  // and warms up the page cache.
  explicit FilledSorter(const DataConfig& config)
      : sorter_{std::make_unique<Sorter>(
            filename_, memoryFor(rowsPerRunFor(config.numRuns_), NumCols),
            ad_utility::testing::makeAllocator(),
            blocksizeCompressionFor(config))} {
    // The sorted result is extracted several times, so it must not be moved out
    // of the sorter by the first merge.
    sorter_->moveResultOnMerge() = false;
    pushRows<NumCols>(*sorter_,
                      rowsPerRunFor(config.numRuns_) * config.numRuns_,
                      config.distribution_);
    // The warm-up merge is serial, so that it needs no executor at all.
    sorter_->setMergeExecutor(net::any_io_executor{}, 1);
    [[maybe_unused]] size_t checksum =
        mergeAndComputeChecksum<NumCols>(*sorter_, std::nullopt);
  }

  // The sorter must not be copied or moved, the file is deleted exactly once.
  FilledSorter(const FilledSorter&) = delete;
  FilledSorter& operator=(const FilledSorter&) = delete;

  // Destroy the sorter (which closes and deletes the file) and then make sure
  // that the file is really gone.
  ~FilledSorter() {
    sorter_.reset();
    ad_utility::deleteFile(filename_, false);
  }

  // ___________________________________________________________________________
  Sorter& sorter() { return *sorter_; }
};
}  // namespace
}  // namespace compressedIdTableMerge

// The benchmarks of the merge phase of the `CompressedExternalIdTableSorter`,
// see the comment above `namespace compressedIdTableMerge` for the axes that
// are covered.
class CompressedIdTableMergeBenchmark : public BenchmarkInterface {
 private:
  using DataConfig = compressedIdTableMerge::DataConfig;
  using MergeConfig = compressedIdTableMerge::MergeConfig;
  using Distribution = compressedIdTableMerge::Distribution;

  // The accumulated checksum of all merges. It is logged at the very end, so
  // that the compiler cannot optimize the merging away.
  size_t checksum_ = 0;

 public:
  // ___________________________________________________________________________
  std::string name() const final {
    return "Benchmarks for merging compressed external IdTables";
  }

  // ___________________________________________________________________________
  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    checkThatParallelResultEqualsSerialResult();
    addNumColumnsTable(results);
    addNumRunsTable(results);
    addBlocksizeTable(results);
    addOutputBlockSizeTable(results);
    addReadThroughTable(results);
    AD_LOG_INFO << "The checksum of all merged rows was " << checksum_
                << std::endl;
    return results;
  }

 private:
  // Return the names of the columns of all tables that compare the available
  // parallelism.
  // NOTE: The first column is filled with the row names by the infrastructure,
  // so the measurements start at column one.
  static std::vector<std::string> parallelismColumnNames(
      std::string nameOfFirstColumn) {
    std::vector<std::string> result{std::move(nameOfFirstColumn), "serial"};
    for (size_t numThreads : compressedIdTableMerge::THREAD_COUNTS_ID_TABLE) {
      result.push_back(absl::StrCat("pool, ", numThreads, " thr"));
    }
    return result;
  }

  // Measure one merge per degree of parallelism into the given `row` of the
  // `table`. The `sorter` is already filled, so that neither the pushing of the
  // rows nor the flushing of the last run is part of a measurement.
  template <typename Comparator, size_t NumCols>
  void addParallelismMeasurements(
      ResultTable& table, size_t row,
      compressedIdTableMerge::FilledSorter<Comparator, NumCols>& filledSorter,
      const MergeConfig& mergeConfig) {
    using namespace compressedIdTableMerge;
    auto& sorter = filledSorter.sorter();
    ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING =
        mergeConfig.ignoreMemoryLimit_;
    absl::Cleanup resetMemoryLimit = []() {
      ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING =
          false;
    };
    auto measure = [this, &table, &sorter, &mergeConfig, row](
                       size_t column, size_t parallelism) {
      // NOTE: The pool is created (and thus its threads are started) outside of
      // the measured function. A `parallelism` of one merges serially and
      // therefore needs no pool at all.
      std::optional<net::thread_pool> pool;
      if (parallelism > 1) {
        pool.emplace(parallelism);
        sorter.setMergeExecutor(pool->get_executor(), parallelism);
      } else {
        sorter.setMergeExecutor(net::any_io_executor{}, 1);
      }
      table.addMeasurement(row, column, [this, &sorter, &mergeConfig]() {
        checksum_ += mergeAndComputeChecksum<NumCols>(
            sorter, mergeConfig.outputBlockSize_);
      });
      if (pool.has_value()) {
        pool->join();
      }
    };
    measure(1, 1);
    for (size_t i = 0; i < THREAD_COUNTS_ID_TABLE.size(); ++i) {
      measure(i + 2, THREAD_COUNTS_ID_TABLE.at(i));
    }
  }

  // Create a sorter for the `dataConfig` and measure one row of the `table`.
  template <typename Comparator, size_t NumCols>
  void addRowWithOwnSorter(ResultTable& table, size_t row,
                           const std::string& name,
                           const DataConfig& dataConfig,
                           const MergeConfig& mergeConfig = {}) {
    using namespace compressedIdTableMerge;
    logConfiguration(name, NumCols, dataConfig, mergeConfig);
    FilledSorter<Comparator, NumCols> filledSorter{dataConfig};
    addParallelismMeasurements<Comparator, NumCols>(table, row, filledSorter,
                                                    mergeConfig);
  }

  // Add the table for the number of columns and the distribution of the `Id`s.
  void addNumColumnsTable(BenchmarkResults& results) {
    using namespace compressedIdTableMerge;
    std::vector<std::string> rowNames{"3 cols, uniform", "4 cols, uniform",
                                      "6 cols, uniform", "3 cols, skewed",
                                      "4 cols, skewed",  "6 cols, skewed"};
    auto& table = results.addTable(
        absl::StrCat("Number of columns and distribution of the `Id`s (",
                     NUM_ROWS, " rows, 16 runs)"),
        rowNames, parallelismColumnNames("columns, distribution"));
    size_t row = 0;
    for (Distribution distribution :
         {Distribution::Uniform, Distribution::Skewed}) {
      DataConfig config{16, distribution};
      addRowWithOwnSorter<SortByPSONoGraphColumn, 3>(table, row,
                                                     rowNames.at(row), config);
      ++row;
      addRowWithOwnSorter<SortByPSO, NumColumnsIndexBuilding>(
          table, row, rowNames.at(row), config);
      ++row;
      addRowWithOwnSorter<SortByPSO, NumColumnsIndexBuilding + 2>(
          table, row, rowNames.at(row), config);
      ++row;
    }
  }

  // Add the table for the number of presorted runs. Note that the number of
  // runs cannot be varied independently of the memory limit, because the sorter
  // derives the size of a run from the memory limit. The memory limit therefore
  // shrinks by a factor of four from row to row, which in turn reduces the
  // number of chunks that can be merged concurrently (this is logged for every
  // row, see `logConfiguration`).
  void addNumRunsTable(BenchmarkResults& results) {
    using namespace compressedIdTableMerge;
    constexpr std::array<size_t, 3> runCounts{4, 16, 64};
    std::vector<std::string> rowNames;
    for (Distribution distribution :
         {Distribution::Uniform, Distribution::Skewed}) {
      for (size_t numRuns : runCounts) {
        rowNames.push_back(absl::StrCat(
            numRuns, " runs, ",
            distribution == Distribution::Uniform ? "uniform" : "skewed"));
      }
    }
    auto& table = results.addTable(
        absl::StrCat("Number of presorted runs (", NUM_ROWS, " rows, ",
                     NumColumnsIndexBuilding, " columns)"),
        rowNames, parallelismColumnNames("runs, distribution"));
    size_t row = 0;
    for (Distribution distribution :
         {Distribution::Uniform, Distribution::Skewed}) {
      for (size_t numRuns : runCounts) {
        addRowWithOwnSorter<SortByPSO, NumColumnsIndexBuilding>(
            table, row, rowNames.at(row), DataConfig{numRuns, distribution});
        ++row;
      }
    }
  }

  // Add the table for the size of a single compressed block. Smaller blocks
  // mean more (but smaller) reads and a finer granularity for the splitting of
  // the merge into independent chunks, larger blocks mean fewer reads and a
  // better compression ratio.
  void addBlocksizeTable(BenchmarkResults& results) {
    using namespace compressedIdTableMerge;
    constexpr std::array<size_t, 3> blocksizesInKilobytes{8, 64, 512};
    std::vector<std::string> rowNames;
    for (size_t blocksize : blocksizesInKilobytes) {
      rowNames.push_back(absl::StrCat(blocksize, " kB"));
    }
    auto& table = results.addTable(
        absl::StrCat("Size of a single compressed block (", NUM_ROWS, " rows, ",
                     NumColumnsIndexBuilding, " columns, 16 runs, skewed)"),
        rowNames, parallelismColumnNames("blocksize"));
    for (size_t row = 0; row < blocksizesInKilobytes.size(); ++row) {
      DataConfig config{16, Distribution::Skewed,
                        MemorySize::kilobytes(blocksizesInKilobytes.at(row))};
      addRowWithOwnSorter<SortByPSO, NumColumnsIndexBuilding>(
          table, row, rowNames.at(row), config);
    }
  }

  // Add the table for the size of a single output block, which is the axis that
  // decides how well the merge scales, see the comment at the top. All rows use
  // the same data (and therefore the same sorter), only the merge phase
  // differs. The last two rows additionally ignore the memory limit, so that
  // all the chunks that the parallelism offers are in flight no matter how
  // large the output blocks are.
  void addOutputBlockSizeTable(BenchmarkResults& results) {
    using namespace compressedIdTableMerge;
    constexpr size_t NumCols = NumColumnsIndexBuilding;
    const DataConfig dataConfig{16, Distribution::Skewed};
    std::vector<std::string> rowNames{
        "automatic",   "750 000 rows",        "100 000 rows",
        "25 000 rows", "750 000, no mem cap", "100 000, no mem cap"};
    std::vector<MergeConfig> mergeConfigs{
        {std::nullopt, false}, {750'000, false}, {100'000, false},
        {25'000, false},       {750'000, true},  {100'000, true}};
    auto& table = results.addTable(
        absl::StrCat("Size of a single output block (", NUM_ROWS, " rows, ",
                     NumCols, " columns, 16 runs, skewed)"),
        rowNames, parallelismColumnNames("output blocksize"));
    FilledSorter<SortByPSO, NumCols> filledSorter{dataConfig};
    for (size_t row = 0; row < mergeConfigs.size(); ++row) {
      logConfiguration(rowNames.at(row), NumCols, dataConfig,
                       mergeConfigs.at(row));
      addParallelismMeasurements<SortByPSO, NumCols>(table, row, filledSorter,
                                                     mergeConfigs.at(row));
    }
  }

  // Add the reference table that reads and decompresses exactly the same data
  // without merging it (via a `CompressedExternalIdTable`, which streams the
  // blocks through a single background thread). The difference to the serial
  // merge is the actual cost of the merging, and the ratio tells us how much of
  // the merge is bound by the decompression.
  void addReadThroughTable(BenchmarkResults& results) {
    using namespace compressedIdTableMerge;
    std::vector<std::string> rowNames{
        "uniform, default blocksize", "skewed, default blocksize",
        "skewed, 8 kB blocks", "skewed, 512 kB blocks"};
    std::vector<DataConfig> configs{
        {16, Distribution::Uniform},
        {16, Distribution::Skewed},
        {16, Distribution::Skewed, MemorySize::kilobytes(8)},
        {16, Distribution::Skewed, MemorySize::kilobytes(512)}};
    auto& table = results.addTable(
        absl::StrCat(
            "Reference: sequential read and decompress without merging (",
            NUM_ROWS, " rows, ", NumColumnsIndexBuilding, " columns, 16 runs)"),
        rowNames, {"configuration", "read + decompress"});
    for (size_t row = 0; row < configs.size(); ++row) {
      addReadThroughMeasurement(table, row, rowNames.at(row), configs.at(row));
    }
  }

  // Add a single measurement to the reference table above.
  void addReadThroughMeasurement(ResultTable& table, size_t row,
                                 const std::string& name,
                                 const DataConfig& config) {
    using namespace compressedIdTableMerge;
    constexpr size_t NumCols = NumColumnsIndexBuilding;
    const size_t rowsPerRun = rowsPerRunFor(config.numRuns_);
    const size_t numRows = rowsPerRun * config.numRuns_;
    logConfiguration(absl::StrCat("read-through, ", name), NumCols, config, {});

    const std::string filename = makeFilename();
    absl::Cleanup cleanup = [&filename] {
      ad_utility::deleteFile(filename, false);
    };
    CompressedExternalIdTable<NumCols> idTable{
        filename, memoryFor(rowsPerRun, NumCols),
        ad_utility::testing::makeAllocator(), blocksizeCompressionFor(config)};
    pushRows<NumCols>(idTable, numRows, config.distribution_);
    table.addMeasurement(row, 1, [this, &idTable]() {
      size_t checksum = 0;
      for (const auto& idTableRow : idTable.getRows()) {
        checksum += idTableRow[0].getBits();
      }
      checksum_ += checksum;
    });
  }

  // Check (outside of every measurement) that the parallel merge yields exactly
  // the same result as the serial merge. The comparators are total orders on
  // the columns that they compare, and all other columns of this benchmark are
  // constant, so the results have to be identical row by row.
  void checkThatParallelResultEqualsSerialResult() {
    using namespace compressedIdTableMerge;
    constexpr size_t NumCols = NumColumnsIndexBuilding;
    constexpr size_t numRuns = 8;
    constexpr size_t rowsPerRun = 500'000;

    const std::string filename = makeFilename();
    absl::Cleanup cleanup = [&filename] {
      ad_utility::deleteFile(filename, false);
    };
    CompressedExternalIdTableSorter<SortByPSO, NumCols> sorter{
        filename, memoryFor(rowsPerRun, NumCols),
        ad_utility::testing::makeAllocator(),
        MemorySize::bytes(rowsPerRun * sizeof(Id) / BLOCKS_PER_RUN)};
    sorter.moveResultOnMerge() = false;
    pushRows<NumCols>(sorter, numRuns * rowsPerRun, Distribution::Skewed);

    sorter.setMergeExecutor(net::any_io_executor{}, 1);
    auto serialResult = mergeAndMaterialize<NumCols>(sorter);
    net::thread_pool pool{8};
    sorter.setMergeExecutor(pool.get_executor(), 8);
    auto parallelResult = mergeAndMaterialize<NumCols>(sorter);
    pool.join();
    AD_CORRECTNESS_CHECK(serialResult.size() == numRuns * rowsPerRun * NumCols);
    if (serialResult != parallelResult) {
      throw std::runtime_error{
          "The parallel merge of the `CompressedExternalIdTableSorter` yielded "
          "a different result than the serial merge. This is a bug, the "
          "benchmark results below are meaningless."};
    }
    AD_LOG_INFO << "The parallel merge yields exactly the same result as the "
                   "serial merge."
                << std::endl;
  }
};

AD_REGISTER_BENCHMARK(CompressedIdTableMergeBenchmark);
}  // namespace ad_benchmark
