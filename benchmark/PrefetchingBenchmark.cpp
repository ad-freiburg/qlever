// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#if defined(__linux__)
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/PrefetchingBatchResolver.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/VocabIndex.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "parser/LiteralOrIri.h"
#include "util/CompactStringVector.h"
#include "util/Exception.h"
#include "util/Timer.h"

namespace ad_benchmark {
namespace {

using namespace ql::engine::prefetch;

// _____________________________________________________________________________
// Hardware Performance Counter Monitor for L1/L2/L3 cache misses and cycles.
// Encapsulates Linux `perf_event_open` syscalls with RAII lifecycle.
class HardwarePerformanceMonitor {
 public:
  struct CounterSample {
    uint64_t cpuCycles{0};
    uint64_t instructions{0};
    uint64_t l1dReadAccesses{0};
    uint64_t l1dReadMisses{0};
    uint64_t llcReadAccesses{0};
    uint64_t llcReadMisses{0};
    uint64_t hwCacheMisses{0};
    uint64_t hwCacheReferences{0};
    double durationSeconds{0.0};

    [[nodiscard]] double l1dMissRatePercent() const noexcept {
      return l1dReadAccesses > 0 ? (100.0 * static_cast<double>(l1dReadMisses) /
                                    static_cast<double>(l1dReadAccesses))
                                 : 0.0;
    }

    [[nodiscard]] double llcMissRatePercent() const noexcept {
      return llcReadAccesses > 0 ? (100.0 * static_cast<double>(llcReadMisses) /
                                    static_cast<double>(llcReadAccesses))
                                 : 0.0;
    }

    [[nodiscard]] double ipc() const noexcept {
      return cpuCycles > 0 ? (static_cast<double>(instructions) /
                              static_cast<double>(cpuCycles))
                           : 0.0;
    }
  };

 private:
  bool supported_{false};
  int fdCycles_{-1};
  int fdInstructions_{-1};
  int fdL1dAccess_{-1};
  int fdL1dMiss_{-1};
  int fdLlcAccess_{-1};
  int fdLlcMiss_{-1};
  int fdCacheMiss_{-1};
  int fdCacheRef_{-1};

  std::chrono::high_resolution_clock::time_point startTime_;
  std::chrono::high_resolution_clock::time_point stopTime_;

#if defined(__linux__)
  static int openPerfCounter(uint32_t type, uint64_t config, int groupFd = -1) {
    struct perf_event_attr pe;
    std::memset(&pe, 0, sizeof(struct perf_event_attr));
    pe.type = type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = config;
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;

    long fd = syscall(__NR_perf_event_open, &pe, 0, -1, groupFd, 0);
    return static_cast<int>(fd);
  }

  static uint64_t readCounter(int fd) {
    if (fd < 0) return 0;
    uint64_t count = 0;
    ssize_t res = ::read(fd, &count, sizeof(uint64_t));
    return res == sizeof(uint64_t) ? count : 0;
  }
#endif

 public:
  HardwarePerformanceMonitor() {
#if defined(__linux__)
    fdCycles_ = openPerfCounter(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
    fdInstructions_ =
        openPerfCounter(PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);

    // L1 Data Cache Access & Miss
    uint64_t l1dAccessConfig = PERF_COUNT_HW_CACHE_L1D |
                               (PERF_COUNT_HW_CACHE_OP_READ << 8) |
                               (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16);
    uint64_t l1dMissConfig = PERF_COUNT_HW_CACHE_L1D |
                             (PERF_COUNT_HW_CACHE_OP_READ << 8) |
                             (PERF_COUNT_HW_CACHE_RESULT_MISS << 16);
    fdL1dAccess_ = openPerfCounter(PERF_TYPE_HW_CACHE, l1dAccessConfig);
    fdL1dMiss_ = openPerfCounter(PERF_TYPE_HW_CACHE, l1dMissConfig);

    // Last Level Cache (LLC/L3) Access & Miss
    uint64_t llcAccessConfig = PERF_COUNT_HW_CACHE_LL |
                               (PERF_COUNT_HW_CACHE_OP_READ << 8) |
                               (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16);
    uint64_t llcMissConfig = PERF_COUNT_HW_CACHE_LL |
                             (PERF_COUNT_HW_CACHE_OP_READ << 8) |
                             (PERF_COUNT_HW_CACHE_RESULT_MISS << 16);
    fdLlcAccess_ = openPerfCounter(PERF_TYPE_HW_CACHE, llcAccessConfig);
    fdLlcMiss_ = openPerfCounter(PERF_TYPE_HW_CACHE, llcMissConfig);

    fdCacheMiss_ =
        openPerfCounter(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES);
    fdCacheRef_ =
        openPerfCounter(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_REFERENCES);

    supported_ = (fdCycles_ >= 0);
#else
    supported_ = false;
#endif
  }

  // Owns raw perf-event file descriptors: copying or moving would close them
  // twice.
  HardwarePerformanceMonitor(const HardwarePerformanceMonitor&) = delete;
  HardwarePerformanceMonitor& operator=(const HardwarePerformanceMonitor&) =
      delete;
  HardwarePerformanceMonitor(HardwarePerformanceMonitor&&) = delete;
  HardwarePerformanceMonitor& operator=(HardwarePerformanceMonitor&&) = delete;

  ~HardwarePerformanceMonitor() {
#if defined(__linux__)
    auto closeFd = [](int& fd) {
      if (fd >= 0) {
        ::close(fd);
        fd = -1;
      }
    };
    closeFd(fdCycles_);
    closeFd(fdInstructions_);
    closeFd(fdL1dAccess_);
    closeFd(fdL1dMiss_);
    closeFd(fdLlcAccess_);
    closeFd(fdLlcMiss_);
    closeFd(fdCacheMiss_);
    closeFd(fdCacheRef_);
#endif
  }

  [[nodiscard]] bool isSupported() const noexcept { return supported_; }

  void start() {
#if defined(__linux__)
    if (supported_) {
      auto resetAndEnable = [](int fd) {
        if (fd >= 0) {
          ioctl(fd, PERF_EVENT_IOC_RESET, 0);
          ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
        }
      };
      resetAndEnable(fdCycles_);
      resetAndEnable(fdInstructions_);
      resetAndEnable(fdL1dAccess_);
      resetAndEnable(fdL1dMiss_);
      resetAndEnable(fdLlcAccess_);
      resetAndEnable(fdLlcMiss_);
      resetAndEnable(fdCacheMiss_);
      resetAndEnable(fdCacheRef_);
    }
#endif
    startTime_ = std::chrono::high_resolution_clock::now();
  }

  [[nodiscard]] CounterSample stop() {
    stopTime_ = std::chrono::high_resolution_clock::now();
    CounterSample sample;
    sample.durationSeconds =
        std::chrono::duration<double>(stopTime_ - startTime_).count();

#if defined(__linux__)
    if (supported_) {
      auto disableFd = [](int fd) {
        if (fd >= 0) {
          ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
        }
      };
      disableFd(fdCycles_);
      disableFd(fdInstructions_);
      disableFd(fdL1dAccess_);
      disableFd(fdL1dMiss_);
      disableFd(fdLlcAccess_);
      disableFd(fdLlcMiss_);
      disableFd(fdCacheMiss_);
      disableFd(fdCacheRef_);

      sample.cpuCycles = readCounter(fdCycles_);
      sample.instructions = readCounter(fdInstructions_);
      sample.l1dReadAccesses = readCounter(fdL1dAccess_);
      sample.l1dReadMisses = readCounter(fdL1dMiss_);
      sample.llcReadAccesses = readCounter(fdLlcAccess_);
      sample.llcReadMisses = readCounter(fdLlcMiss_);
      sample.hwCacheMisses = readCounter(fdCacheMiss_);
      sample.hwCacheReferences = readCounter(fdCacheRef_);
    }
#endif
    return sample;
  }
};

// _____________________________________________________________________________
// Checksum over looked-up bytes. Returned (not discarded) by every measured
// lambda so the optimizer cannot eliminate the looked-up data (same idiom as
// `VocabBatchLookupBenchmark::checksumViews`).
size_t checksumViews(const std::vector<std::string_view>& views) {
  size_t hash = 0;
  for (std::string_view view : views) {
    hash += view.size();
    for (char c : view) {
      hash = hash * 1315423911u + static_cast<unsigned char>(c);
    }
  }
  return hash;
}

// _____________________________________________________________________________
// Benchmark suite measuring Software Cache Prefetching vs Baseline lookup.
class PrefetchingBenchmark : public BenchmarkInterface {
 private:
  static constexpr size_t NUM_VOCAB_ENTRIES = 1'000'000;
  static constexpr size_t NUM_LOOKUP_IDS = 500'000;

  CompactVectorOfStrings<char> vocabWords_;
  std::vector<Id> lookupIds_;

 public:
  PrefetchingBenchmark() { setupSyntheticVocabulary(); }

  [[nodiscard]] std::string name() const override {
    return "Optimization 16: Software Cache Prefetching Benchmark";
  }

  // Generate 1M realistic vocabulary words and 500k uniformly randomized lookup
  // IDs
  void setupSyntheticVocabulary() {
    std::vector<std::string> words;
    words.reserve(NUM_VOCAB_ENTRIES);

    for (size_t i = 0; i < NUM_VOCAB_ENTRIES; ++i) {
      if (i % 3 == 0) {
        words.push_back("<http://example.org/entity/resource_" +
                        std::to_string(i) + ">");
      } else if (i % 3 == 1) {
        words.push_back("\"Literal text description value " +
                        std::to_string(i) +
                        "\"^^<http://www.w3.org/2001/XMLSchema#string>");
      } else {
        words.push_back("\"Label " + std::to_string(i) + "\"@en");
      }
    }

    vocabWords_.build(words);

    // Uniform random distribution across entire 1M vocabulary to simulate
    // DRAM/L3 cache miss pressure
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<uint64_t> dist(0, NUM_VOCAB_ENTRIES - 1);

    lookupIds_.reserve(NUM_LOOKUP_IDS);

    for (size_t i = 0; i < NUM_LOOKUP_IDS; ++i) {
      uint64_t vocabIndex = dist(rng);
      lookupIds_.push_back(
          Id::makeFromVocabIndex(VocabIndex::make(vocabIndex)));
    }
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results;
    auto& group = results.addGroup(
        "Random Vocabulary Resolution across 500,000 IDs (1M Vocabulary)");

    HardwarePerformanceMonitor perfMonitor;

    // 1. Baseline: Standard sequential lookup without software prefetching
    {
      std::vector<std::string_view> resolved(NUM_LOOKUP_IDS);
      HardwarePerformanceMonitor::CounterSample sample;

      auto& m = group.addMeasurement(
          "Baseline (Standard Sequential Lookup, No Prefetch)", [&]() {
            perfMonitor.start();
            const auto offsets = vocabWords_.offsetsSpan();
            const auto data = vocabWords_.dataSpan();

            for (size_t i = 0; i < NUM_LOOKUP_IDS; ++i) {
              const size_t wordIdx = lookupIds_[i].getVocabIndex().get();
              const auto curOffset = offsets[wordIdx];
              const auto nextOffset = offsets[wordIdx + 1];
              resolved[i] = std::string_view(data.data() + curOffset,
                                             nextOffset - curOffset);
            }

            sample = perfMonitor.stop();
            return checksumViews(resolved);
          });

      const double mResolutionsPerSec =
          (static_cast<double>(NUM_LOOKUP_IDS) / 1e6) / sample.durationSeconds;
      const double nsPerLookup =
          (sample.durationSeconds * 1e9) / static_cast<double>(NUM_LOOKUP_IDS);

      m.metadata().addKeyValuePair("num-lookups", NUM_LOOKUP_IDS);
      m.metadata().addKeyValuePair("throughput-M-res-per-sec",
                                   mResolutionsPerSec);
      m.metadata().addKeyValuePair("latency-ns-per-lookup", nsPerLookup);
      m.metadata().addKeyValuePair("duration-ms",
                                   sample.durationSeconds * 1000.0);
      if (perfMonitor.isSupported()) {
        m.metadata().addKeyValuePair("l1d-miss-rate-pct",
                                     sample.l1dMissRatePercent());
        m.metadata().addKeyValuePair("llc-miss-rate-pct",
                                     sample.llcMissRatePercent());
        m.metadata().addKeyValuePair("ipc", sample.ipc());
      }
    }

    // 2. Evaluated Prefetched Lookups with varying distances (K = 4, 8, 16, 32)
    const std::vector<size_t> testDistances = {4, 8, 16, 32};

    for (size_t distance : testDistances) {
      std::vector<std::string_view> resolved(NUM_LOOKUP_IDS);
      HardwarePerformanceMonitor::CounterSample sample;
      PrefetchingBatchResolver resolver(
          PrefetchConfig{.prefetchDistance = distance});

      std::string label =
          "Prefetched Lookup (Pipelined K = " + std::to_string(distance) +
          " rows ahead)";

      // Index extraction is setup, not lookup work: allocating and filling
      // `rawIndices` here keeps memory allocation overhead out of the timed
      // measurement region below.
      std::vector<size_t> rawIndices(NUM_LOOKUP_IDS);
      for (size_t i = 0; i < NUM_LOOKUP_IDS; ++i) {
        rawIndices[i] = lookupIds_[i].getVocabIndex().get();
      }

      auto& m = group.addMeasurement(label, [&]() {
        perfMonitor.start();

        resolver.resolveCompactVectorPipelined(
            vocabWords_, rawIndices,
            [&resolved](size_t i, size_t, std::string_view view) {
              resolved[i] = view;
            });

        sample = perfMonitor.stop();
        return checksumViews(resolved);
      });

      const double mResolutionsPerSec =
          (static_cast<double>(NUM_LOOKUP_IDS) / 1e6) / sample.durationSeconds;
      const double nsPerLookup =
          (sample.durationSeconds * 1e9) / static_cast<double>(NUM_LOOKUP_IDS);

      m.metadata().addKeyValuePair("prefetch-distance-K", distance);
      m.metadata().addKeyValuePair("throughput-M-res-per-sec",
                                   mResolutionsPerSec);
      m.metadata().addKeyValuePair("latency-ns-per-lookup", nsPerLookup);
      m.metadata().addKeyValuePair("duration-ms",
                                   sample.durationSeconds * 1000.0);
      if (perfMonitor.isSupported()) {
        m.metadata().addKeyValuePair("l1d-miss-rate-pct",
                                     sample.l1dMissRatePercent());
        m.metadata().addKeyValuePair("llc-miss-rate-pct",
                                     sample.llcMissRatePercent());
        m.metadata().addKeyValuePair("ipc", sample.ipc());
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(PrefetchingBenchmark);

}  // namespace
}  // namespace ad_benchmark
