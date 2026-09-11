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

#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/program_options.hpp>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "backports/span.h"
#include "engine/CallFixedSize.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Id.h"
#include "global/IndexTypes.h"
#include "global/RuntimeParameters.h"
#include "index/ConstantsIndexBuilding.h"
#include "util/AllocatorWithLimit.h"
#include "util/File.h"
#include "util/Log.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ProgramOptionsHelpers.h"
#include "util/ProgressBar.h"
#include "util/Random.h"
#include "util/ResourceMonitor.h"
#include "util/Timer.h"
#include "util/jthread.h"

// A standalone benchmark for a single large external sort with the
// `ad_utility::CompressedExternalIdTableSorter`, which is the data structure
// that dominates the sorting phase of a QLever index build.
//
// The benchmark pushes randomly generated rows into a sorter with the given
// memory limit, and then consumes the sorted result blockwise in a thread of
// its own, summing up the bits of the first column as a sanity check (the sum
// is invariant under sorting, so it must be exactly the sum that the pushing
// phase computed).
//
// In contrast to `ParallelBlockMergeBenchmark.cpp`, which sweeps many
// configurations with the benchmark infrastructure, this binary runs exactly
// one configuration that is completely specified on the command line, so that
// it can be run under `perf`, inside a cgroup with a limited amount of RAM, or
// against a temporary directory on a specific disk. See the `--help` output
// for all options.
namespace {

namespace po = boost::program_options;
namespace net = boost::asio;

using ad_utility::CompressedExternalIdTableSorter;
using ad_utility::MemorySize;
using ad_utility::Timer;
using namespace ad_utility::memory_literals;

// The distribution of the generated `Id`s. The two distributions differ
// enormously in how expensive the comparator is (see `RowGenerator::next`) as
// well as in how well the presorted runs and the spilled output blocks of the
// merge phase compress, so a configuration is only meaningful together with
// its distribution.
enum class Distribution {
  // Every column is uniformly distributed over a large range, so the first
  // column that the comparator looks at almost always decides. This is the
  // hard case for the compression and the easy case for the comparator.
  Uniform,
  // Model the triples of a real knowledge graph: few distinct predicates, many
  // subjects, even more objects, and a single graph. During the merge all runs
  // are at almost the same position of the sorted order, so the comparator has
  // to walk two to three columns. This is what a real index build produces.
  Skewed,
  // All rows are equal, which is the extreme case of the above: the comparator
  // always walks all columns, and the data compresses perfectly.
  Constant
};

// The number of distinct values per column of the `Skewed` distribution. These
// are the same values that `ParallelBlockMergeBenchmark.cpp` uses, so that the
// numbers of the two benchmarks are comparable.
constexpr size_t NUM_PREDICATES = 100;
constexpr size_t NUM_SUBJECTS = 20'000;
constexpr size_t NUM_OBJECTS = 10'000'000;
// The number of distinct values of the columns beyond the first three.
constexpr size_t NUM_PAYLOAD_VALUES = 1'000;

// The interval at which `PeakRssTracker` (see below) samples the RSS.
constexpr std::chrono::milliseconds RSS_SAMPLING_INTERVAL{50};

// The complete configuration of a single run, see `parseCommandLine` for the
// documentation of the individual options.
struct Config {
  MemorySize memoryLimit_;
  size_t numColumns_;
  size_t numRows_;
  size_t numThreads_;
  size_t mergeParallelism_;
  size_t numSortColumns_;
  Distribution distribution_;
  size_t uniformRangeBits_;
  MemorySize blocksizeCompression_;
  std::optional<size_t> outputBlockSize_;
  ad_utility::CompressedBlockFile::Compression mergeSpillCompression_;
  std::optional<MemorySize> allocatorLimit_;
  bool staticColumns_;
  bool ignoreMemoryLimit_;
  bool verifySorted_;
  size_t numMerges_;
  size_t progressBatchSize_;
  std::string tempDir_;
  std::optional<uint64_t> seed_;
  std::optional<std::string> resourceUsageLog_;
};

// Compare two rows lexicographically by their first `numSortColumns_` columns.
// This is the runtime-configurable equivalent of the `SortByPSO` and friends
// from `index/ExternalSortFunctors.h`, which the index build uses.
struct RowComparator {
  // The number of columns that are compared. The default of zero (which makes
  // all rows compare equal, so that the sorter has nothing to do) only exists
  // because the sorter requires the comparator to be default-constructible;
  // this benchmark always passes an explicit positive value.
  size_t numSortColumns_ = 0;

  // Return `true` if `a` is smaller than `b`.
  template <typename Row1, typename Row2>
  bool operator()(const Row1& a, const Row2& b) const {
    for (size_t i = 0; i < numSortColumns_; ++i) {
      const Id& left = a[i];
      const Id& right = b[i];
      auto comparison = left.compareWithoutLocalVocab(right);
      if (comparison != 0) {
        return comparison < 0;
      }
    }
    return false;
  }
};

// Generate random rows according to a `Distribution`.
class RowGenerator {
 private:
  ad_utility::FastRandomIntGenerator<uint64_t> generator_;
  Distribution distribution_;
  // The size of the range of the `Uniform` distribution.
  uint64_t uniformRange_;

 public:
  // ___________________________________________________________________________
  RowGenerator(Distribution distribution, size_t uniformRangeBits,
               std::optional<uint64_t> seed)
      : generator_{seed.has_value()
                       ? ad_utility::RandomSeed::make(seed.value())
                       : ad_utility::RandomSeed::make(std::random_device{}())},
        distribution_{distribution},
        uniformRange_{uniformRangeBits >= 64
                          ? std::numeric_limits<uint64_t>::max()
                          : (uint64_t{1} << uniformRangeBits)} {}

  // Overwrite `row` with the next random row.
  void next(ql::span<Id> row) {
    auto makeId = [](uint64_t value) {
      return Id::makeFromVocabIndex(VocabIndex::make(value));
    };
    switch (distribution_) {
      case Distribution::Uniform:
        for (Id& id : row) {
          id = makeId(generator_() % uniformRange_);
        }
        return;
      case Distribution::Constant:
        for (Id& id : row) {
          id = makeId(42);
        }
        return;
      case Distribution::Skewed:
        break;
    }
    row[0] = makeId(generator_() % NUM_SUBJECTS);
    if (row.size() > 1) {
      row[1] = makeId(generator_() % NUM_PREDICATES);
    }
    if (row.size() > 2) {
      row[2] = makeId(generator_() % NUM_OBJECTS);
    }
    for (size_t i = 3; i < row.size(); ++i) {
      // There is only a single graph, so the graph column never breaks a tie.
      row[i] = makeId(i == ADDITIONAL_COLUMN_GRAPH_ID
                          ? 0
                          : generator_() % NUM_PAYLOAD_VALUES);
    }
  }
};

// Sample the resident set size (RSS) of this process on a background thread and
// remember the largest value that was seen. Note that the RSS is the number
// that the memory limit of the sorter is supposed to bound, so it is the number
// that makes a configuration comparable to a real index build (which typically
// runs with much less RAM than the machine has).
class PeakRssTracker {
 private:
  std::atomic<uint64_t> peakRssBytes_ = 0;
  std::atomic<bool> stopped_ = false;
  // NOTE: The thread must be declared last, such that it is joined (by its
  // destructor) before the members that it accesses are destroyed.
  ad_utility::JThread thread_;

 public:
  // Start the sampling.
  PeakRssTracker()
      : thread_{[this]() {
          while (!stopped_.load()) {
            sample();
            std::this_thread::sleep_for(RSS_SAMPLING_INTERVAL);
          }
          sample();
        }} {}

  // Stop the sampling and join the thread.
  ~PeakRssTracker() { stopped_.store(true); }

  // Take a single sample right now. Used to make sure that the RSS at the end
  // of a phase is part of the peak, no matter where the sampling thread
  // currently is.
  void sample() {
    auto rss = ad_utility::resource_monitor::currentRssBytes();
    if (!rss.has_value()) {
      return;
    }
    uint64_t previous = peakRssBytes_.load();
    while (previous < rss.value() &&
           !peakRssBytes_.compare_exchange_weak(previous, rss.value())) {
    }
  }

  // The largest RSS that was seen so far.
  MemorySize peakRss() {
    sample();
    return MemorySize::bytes(peakRssBytes_.load());
  }
};

// The result of consuming the sorted output of a single merge phase.
struct MergeResult {
  // The sum of the bits of the first column of all merged rows. Must be equal
  // to the same sum over the rows that were pushed, because sorting only
  // permutes the rows.
  uint64_t checksum_ = 0;
  // The total number of rows and the number of blocks that the merge yielded.
  size_t numRows_ = 0;
  size_t numBlocks_ = 0;
};

// Throw an exception with the given `message` if the `condition` does not hold.
// Used for the checks on the command-line arguments that
// `boost::program_options` cannot express, such that invalid input leads to a
// plain error message and not to a failed assertion.
void checkOption(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error{std::string{message}};
  }
}

// Parse the command line. Return `std::nullopt` if the program should exit
// immediately (because `--help` was given), and throw on invalid input.
std::optional<Config> parseCommandLine(int argc, char** argv) {
  Config config{};
  std::string distribution = "skewed";
  std::string mergeSpillCompression = "-5";
  size_t mergeParallelism = 0;
  size_t numSortColumns = 0;

  ad_utility::ParameterToProgramOptionFactory optionFactory{
      &globalRuntimeParameters};

  po::options_description options{
      "Options for largeExternalIdTableSortBenchmark"};
  auto add = [&options](auto&&... args) {
    options.add_options()(AD_FWD(args)...);
  };
  add("help,h", "Produce this help message.");

  // The four parameters that define the size of the benchmark.
  add("memory-limit,m", po::value(&config.memoryLimit_)->required(),
      "The memory limit of the sorter, for example `2 GB` (required). It "
      "determines the size of the presorted runs and therefore how many of "
      "them the merge phase has to merge, as well as how many chunks that "
      "phase may merge concurrently.");
  add("num-columns,c", po::value(&config.numColumns_)->required(),
      "The number of columns of the sorted table (required).");
  add("num-rows,r", po::value(&config.numRows_)->required(),
      "The number of rows that are pushed into the sorter (required).");
  add("num-threads,j", po::value(&config.numThreads_)->required(),
      "The number of threads that run the merge phase (required). A value of "
      "one means that the merge runs serially in the thread that consumes the "
      "sorted output, without any thread pool at all.");

  // The number of threads of the pool and the number of chunks that are merged
  // concurrently can be varied independently, see the comment on
  // `CompressedExternalIdTableSorter::computeMergePhaseParameters` for why
  // more threads than chunks can be faster.
  add("merge-parallelism", po::value(&mergeParallelism),
      "The number of chunks that the merge phase merges concurrently. Defaults "
      "to `--num-threads`; a smaller value leaves threads of the pool free for "
      "the bookkeeping and the compression of the spilled output blocks. NOTE: "
      "The memory limit may reduce this number further, in which case a "
      "warning is logged.");

  // The shape of the data.
  add("distribution,d", po::value(&distribution)->default_value(distribution),
      "The distribution of the generated `Id`s: `skewed` (the realistic case: "
      "few predicates, many subjects, even more objects, so the comparator has "
      "to walk several columns and the data compresses well), `uniform` (every "
      "column uniformly distributed over a large range) or `constant` (all "
      "rows equal).");
  add("uniform-range-bits",
      po::value(&config.uniformRangeBits_)->default_value(40),
      "For the `uniform` distribution: the number of bits of the range that "
      "the `Id`s are drawn from.");
  add("num-sort-columns", po::value(&numSortColumns),
      "The number of leading columns that the comparator compares. Defaults to "
      "`--num-columns`; a smaller value models the payload columns of a real "
      "index build, which the comparator ignores.");
  add("seed", po::value<uint64_t>(),
      "The seed of the random generator. Defaults to a random seed.");

  // The parameters of the sorter itself.
  add("blocksize-compression,b",
      po::value(&config.blocksizeCompression_)
          ->default_value(ad_utility::DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE),
      "The size of a single compressed block of a presorted run. The merge "
      "phase holds one decompressed block per run and per concurrent chunk, so "
      "this value directly limits how much parallelism the memory limit "
      "allows.");
  add("output-blocksize", po::value<size_t>(),
      "The number of rows of a single block of the sorted output. By default "
      "the sorter derives this from its memory limit, which is what the index "
      "build does.");
  add("merge-spill-compression",
      po::value(&mergeSpillCompression)->default_value(mergeSpillCompression),
      "How the merge phase compresses the output blocks that it spills to "
      "disk: a ZSTD level (negative levels are `zstd --fast`) or `none` to "
      "store them uncompressed.");
  add("num-merges", po::value(&config.numMerges_)->default_value(1),
      "Merge (and consume) the presorted runs this many times. Useful to "
      "measure the merge phase alone with a warm page cache. Note that this "
      "keeps the sorted result inside the sorter instead of moving it out, "
      "which is slightly more expensive for inputs that fit into a single "
      "block.");
  add("allocator-limit", po::value<std::string>(),
      "An additional hard limit for the allocator of all `IdTable`s. Exceeding "
      "it throws an exception instead of degrading the performance, which is a "
      "way to check that the sorter really respects its memory limit. "
      "Unlimited by default.");
  add("static-columns", po::bool_switch(&config.staticColumns_),
      absl::StrCat(
          "Use a statically known number of columns if `--num-columns` is at "
          "most ",
          DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE,
          " (the compile-time maximum of this build), which is what the index "
          "build does. Without this flag the number of columns is dynamic, "
          "which makes every access to a row slightly more expensive.")
          .c_str());
  add("ignore-memory-limit", po::bool_switch(&config.ignoreMemoryLimit_),
      "Let the merge phase ignore the memory limit when it computes its "
      "parameters. It then merges as many chunks as `--merge-parallelism` "
      "allows and yields tiny output blocks, which is only useful to measure "
      "how much parallelism the merge would offer if memory were free.");

  // Everything else.
  add("temp-dir,t", po::value(&config.tempDir_)->default_value("."),
      "The directory for the temporary files of the sorter (the presorted runs "
      "and the spilled output blocks of the merge phase). All of them are "
      "deleted again when the run is over.");
  add("verify-sorted", po::value(&config.verifySorted_)->default_value(true),
      "Check that the merged output really is sorted. This costs one "
      "comparison per row, which is much less than the merge itself.");
  add("progress-batch-size",
      po::value(&config.progressBatchSize_)
          ->default_value(DEFAULT_PROGRESS_BAR_BATCH_SIZE),
      "The number of rows between two updates of the progress bars.");
  add("resource-usage-log", po::value<std::string>(),
      "Write a TSV log of the RSS and the CPU usage of this process to this "
      "file.");
  add("log-level",
      optionFactory.getProgramOption<&RuntimeParameters::logLevel_>(),
      "Runtime log level: FATAL, ERROR, WARN, INFO, DEBUG, TIMING, or TRACE.");

  po::variables_map optionsMap;
  po::store(po::parse_command_line(argc, argv, options), optionsMap);
  if (optionsMap.count("help")) {
    std::cout << options << std::endl;
    return std::nullopt;
  }
  po::notify(optionsMap);

  // Post-process the options that need it.
  checkOption(config.numColumns_ > 0, "--num-columns must be positive");
  checkOption(config.numRows_ > 0, "--num-rows must be positive");
  checkOption(config.numThreads_ > 0, "--num-threads must be positive");
  checkOption(config.numMerges_ > 0, "--num-merges must be positive");
  checkOption(config.progressBatchSize_ > 0,
              "--progress-batch-size must be positive");
  // The sorter fills a presorted run with `memoryLimit / (numColumns *
  // sizeof(Id) * 2)` rows, which has to be at least one row.
  checkOption(
      config.memoryLimit_.getBytes() >= config.numColumns_ * sizeof(Id) * 2,
      "The memory limit is too small to hold a single row per "
      "presorted run, increase --memory-limit");

  config.mergeParallelism_ =
      mergeParallelism == 0 ? config.numThreads_ : mergeParallelism;
  config.numSortColumns_ =
      numSortColumns == 0 ? config.numColumns_ : numSortColumns;
  checkOption(config.numSortColumns_ <= config.numColumns_,
              "--num-sort-columns must not exceed --num-columns");

  if (distribution == "uniform") {
    config.distribution_ = Distribution::Uniform;
  } else if (distribution == "skewed") {
    config.distribution_ = Distribution::Skewed;
  } else if (distribution == "constant") {
    config.distribution_ = Distribution::Constant;
  } else {
    throw std::runtime_error{
        absl::StrCat("Invalid value for --distribution: `", distribution,
                     "`, must be one of `uniform`, `skewed`, and `constant`")};
  }
  checkOption(config.uniformRangeBits_ > 0,
              "--uniform-range-bits must be positive");

  if (mergeSpillCompression == "none") {
    config.mergeSpillCompression_ = ad_utility::NO_BLOCK_COMPRESSION;
  } else {
    config.mergeSpillCompression_ = std::stoi(mergeSpillCompression);
  }

  if (optionsMap.count("output-blocksize")) {
    config.outputBlockSize_ = optionsMap["output-blocksize"].as<size_t>();
    checkOption(config.outputBlockSize_.value() > 0,
                "--output-blocksize must be positive");
  }
  if (optionsMap.count("allocator-limit")) {
    config.allocatorLimit_ =
        MemorySize::parse(optionsMap["allocator-limit"].as<std::string>());
  }
  if (optionsMap.count("seed")) {
    config.seed_ = optionsMap["seed"].as<uint64_t>();
  }
  if (optionsMap.count("resource-usage-log")) {
    config.resourceUsageLog_ =
        optionsMap["resource-usage-log"].as<std::string>();
  }
  return config;
}

// The name of the file that stores the presorted runs. The names of the spill
// files of the merge phase are derived from it by the sorter.
std::string makeFilename(const Config& config) {
  ad_utility::FastRandomIntGenerator<uint64_t> generator;
  return absl::StrCat(config.tempDir_,
                      "/largeExternalIdTableSortBenchmark.idTable.",
                      generator(), ".tmp");
}

// Log the configuration together with the parameters that the sorter derives
// from it, such that the output of a run is self-contained.
void logConfiguration(const Config& config) {
  auto distributionName = [&config]() -> std::string_view {
    switch (config.distribution_) {
      case Distribution::Uniform:
        return "uniform";
      case Distribution::Skewed:
        return "skewed";
      case Distribution::Constant:
        return "constant";
    }
    AD_FAIL();
  };
  const MemorySize totalSize =
      MemorySize::bytes(config.numRows_ * config.numColumns_ * sizeof(Id));
  // The sorter fills a run with this many rows, because it holds two runs at
  // the same time: one that is being filled and one that is being sorted and
  // written to disk in the background.
  const size_t rowsPerRun =
      config.memoryLimit_.getBytes() / (config.numColumns_ * sizeof(Id) * 2);
  const size_t numRuns = (config.numRows_ + rowsPerRun - 1) / rowsPerRun;
  AD_LOG_INFO << "Sorting "
              << ad_utility::withThousandSeparators(config.numRows_)
              << " rows of " << config.numColumns_ << " columns ("
              << totalSize.asString() << " of data, " << distributionName()
              << " distribution), comparing the first "
              << config.numSortColumns_ << " column(s)" << std::endl;
  AD_LOG_INFO << "Memory limit " << config.memoryLimit_.asString()
              << ", which gives " << numRuns << " presorted run(s) of "
              << ad_utility::withThousandSeparators(rowsPerRun)
              << " rows, with compressed blocks of "
              << config.blocksizeCompression_.asString() << std::endl;
  AD_LOG_INFO << "The merge phase runs on "
              << (config.mergeParallelism_ == 1
                      ? std::string{"the consuming thread (serial merge)"}
                      : absl::StrCat(config.numThreads_, " thread(s) with at ",
                                     "most ", config.mergeParallelism_,
                                     " concurrent chunks"))
              << ", spilling its output blocks with "
              << (config.mergeSpillCompression_.has_value()
                      ? absl::StrCat("ZSTD level ",
                                     config.mergeSpillCompression_.value())
                      : std::string{"no compression"})
              << std::endl;
}

// Push `config.numRows_` random rows into the `sorter` and return the sum of
// the bits of their first column.
template <typename Sorter>
uint64_t pushRows(Sorter& sorter, const Config& config) {
  RowGenerator generator{config.distribution_, config.uniformRangeBits_,
                         config.seed_};
  std::vector<Id> row(config.numColumns_);
  uint64_t checksum = 0;
  size_t numRowsPushed = 0;
  ad_utility::ProgressBar progressBar{
      numRowsPushed, "Rows pushed: ", config.progressBatchSize_};
  for (size_t i = 0; i < config.numRows_; ++i) {
    generator.next(ql::span<Id>{row});
    checksum += row[0].getBits();
    sorter.push(row);
    ++numRowsPushed;
    if (progressBar.update()) {
      AD_LOG_INFO << progressBar.getProgressString() << std::flush;
    }
  }
  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;
  return checksum;
}

// Consume the sorted output of the `sorter` blockwise, sum up the bits of the
// first column, and (if configured) check that the output really is sorted.
// This function is called in a thread of its own, see `runBenchmark`.
template <size_t NumStaticCols, typename Sorter>
MergeResult consumeSortedBlocks(Sorter& sorter, const Config& config) {
  MergeResult result;
  const RowComparator comparator{config.numSortColumns_};
  // The last row of the previous block, for the sortedness check across the
  // block boundaries.
  std::vector<Id> previousRow;
  ad_utility::ProgressBar progressBar{
      result.numRows_, "Rows merged: ", config.progressBatchSize_};
  for (const auto& block : sorter.template getSortedBlocks<NumStaticCols>(
           config.outputBlockSize_)) {
    for (const Id& id : block.getColumn(0)) {
      result.checksum_ += id.getBits();
    }
    if (config.verifySorted_ && block.numRows() > 0) {
      if (!previousRow.empty() && comparator(block[0], previousRow)) {
        throw std::runtime_error{
            "The merged output is not sorted (at a block boundary)"};
      }
      for (size_t i = 1; i < block.numRows(); ++i) {
        if (comparator(block[i], block[i - 1])) {
          throw std::runtime_error{"The merged output is not sorted"};
        }
      }
      previousRow.assign(block[block.numRows() - 1].begin(),
                         block[block.numRows() - 1].end());
    }
    result.numRows_ += block.numRows();
    ++result.numBlocks_;
    if (progressBar.update()) {
      AD_LOG_INFO << progressBar.getProgressString() << std::flush;
    }
  }
  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;
  return result;
}

// Run the complete benchmark. `NumStaticCols` is the statically known number of
// columns, or zero for a dynamic number of columns, see `--static-columns`.
template <size_t NumStaticCols>
void runBenchmark(const Config& config) {
  AD_CORRECTNESS_CHECK(NumStaticCols == 0 ||
                       NumStaticCols == config.numColumns_);
  using Sorter = CompressedExternalIdTableSorter<RowComparator, NumStaticCols>;

  PeakRssTracker rssTracker;
  const std::string filename = makeFilename(config);
  // The sorter deletes its files itself, this is only a safety net for the case
  // that it throws.
  absl::Cleanup cleanup = [&filename]() {
    ad_utility::deleteFile(filename, false);
  };

  auto allocator = config.allocatorLimit_.has_value()
                       ? ad_utility::makeAllocatorWithLimit<Id>(
                             config.allocatorLimit_.value())
                       : ad_utility::makeUnlimitedAllocator<Id>();
  Sorter sorter{filename,
                config.numColumns_,
                config.memoryLimit_,
                std::move(allocator),
                config.blocksizeCompression_,
                RowComparator{config.numSortColumns_}};
  sorter.setMergeSpillCompression(config.mergeSpillCompression_);
  if (config.numMerges_ > 1) {
    // The result must not be moved out of the sorter, else the second merge
    // would throw.
    sorter.moveResultOnMerge() = false;
  }

  // A serial merge needs no executor at all; it runs in the thread that
  // consumes the sorted output.
  std::optional<net::thread_pool> pool;
  if (config.mergeParallelism_ == 1) {
    sorter.setMergeExecutor(net::any_io_executor{}, 1);
  } else {
    pool.emplace(config.numThreads_);
    sorter.setMergeExecutor(pool->get_executor(), config.mergeParallelism_);
  }
  absl::Cleanup joinPool = [&pool]() {
    if (pool.has_value()) {
      pool->join();
    }
  };

  logConfiguration(config);

  // Phase 1: Push the rows. This creates the presorted runs.
  Timer pushTimer{Timer::Started};
  const uint64_t checksumPushed = pushRows(sorter, config);
  pushTimer.stop();
  const MemorySize peakRssAfterPush = rssTracker.peakRss();
  AD_LOG_INFO << "Pushing took " << Timer::toSeconds(pushTimer.value())
              << " s, peak RSS so far " << peakRssAfterPush.asString()
              << std::endl;

  // Phase 2: Merge the runs and consume the sorted output in a thread of its
  // own. NOTE: The consuming thread must not be a thread of the `pool`, see
  // `parallelBlockMerge::parallelBlockMergeToRange`.
  for (size_t i = 0; i < config.numMerges_; ++i) {
    Timer mergeTimer{Timer::Started};
    auto future = std::async(std::launch::async, [&sorter, &config]() {
      return consumeSortedBlocks<NumStaticCols>(sorter, config);
    });
    // Rethrows exceptions from the consuming thread, in particular a failed
    // sortedness check.
    const MergeResult result = future.get();
    mergeTimer.stop();
    const double seconds = Timer::toSeconds(mergeTimer.value());
    if (result.numRows_ != config.numRows_) {
      throw std::runtime_error{absl::StrCat(
          "The merge yielded ", result.numRows_, " rows instead of the ",
          config.numRows_, " rows that were pushed")};
    }
    if (result.checksum_ != checksumPushed) {
      throw std::runtime_error{absl::StrCat(
          "The checksum of the merged rows (", result.checksum_,
          ") differs from the checksum of the pushed rows (", checksumPushed,
          "), so the sorter has lost or corrupted rows")};
    }
    AD_LOG_INFO << "Merge " << (i + 1) << " of " << config.numMerges_
                << " took " << seconds << " s ("
                << ad_utility::withThousandSeparators(static_cast<size_t>(
                       static_cast<double>(config.numRows_) / seconds))
                << " rows/s) and yielded "
                << ad_utility::withThousandSeparators(result.numBlocks_)
                << " block(s) of "
                << ad_utility::withThousandSeparators(
                       result.numRows_ / std::max(size_t{1}, result.numBlocks_))
                << " rows on average, checksum " << result.checksum_
                << std::endl;
  }
  AD_LOG_INFO << "The checksums of the pushed and the merged rows agree, and "
                 "the merged output is"
              << (config.verifySorted_ ? " sorted"
                                       : " not checked for being "
                                         "sorted")
              << ". Peak RSS " << rssTracker.peakRss().asString() << std::endl;
}
}  // namespace

// ____________________________________________________________________________
int main(int argc, char** argv) {
  std::optional<Config> config;
  try {
    config = parseCommandLine(argc, argv);
  } catch (const std::exception& e) {
    AD_LOG_ERROR << e.what() << std::endl;
    AD_LOG_ERROR << "Run with --help for the list of options." << std::endl;
    return EXIT_FAILURE;
  }
  if (!config.has_value()) {
    return EXIT_SUCCESS;
  }

  ad_utility::ResourceMonitor resourceMonitor;
  if (config.value().resourceUsageLog_.has_value()) {
    resourceMonitor.start(config.value().resourceUsageLog_.value(),
                          ad_utility::ResourceMonitor::Mode::Truncate,
                          std::chrono::milliseconds{100});
  }

  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING =
      config.value().ignoreMemoryLimit_;

  try {
    if (config.value().staticColumns_) {
      // For a number of columns that is too large for this build, this falls
      // back to the dynamic case (`NumStaticCols == 0`), see `callFixedSize`.
      ad_utility::callFixedSizeVi(static_cast<int>(config.value().numColumns_),
                                  [&config](auto NUM_COLUMNS) {
                                    runBenchmark<NUM_COLUMNS>(config.value());
                                  });
    } else {
      runBenchmark<0>(config.value());
    }
  } catch (const std::exception& e) {
    AD_LOG_ERROR << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
