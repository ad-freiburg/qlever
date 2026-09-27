// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <new>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#if defined(__linux__)
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/AlignedAllocator.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Random.h"
#include "util/StreamingBufferWriter.h"
#include "util/Timer.h"

namespace ad_benchmark {

using ad_utility::AlignedAllocator;
using ad_utility::StreamingBufferWriter;

// _____________________________________________________________________________
// Portable compiler barrier that keeps a value alive for benchmarking
// purposes (prevents dead-code elimination of the accumulation loops below).
// Uses inline assembly where the toolchain supports it and falls back to
// `std::atomic_signal_fence` otherwise, so the benchmark also compiles with
// toolchains without GNU-style inline assembly (e.g. MSVC).
inline void doNotOptimizeAway(uint64_t value) {
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" : : "r"(value) : "memory");
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
  (void)value;
#endif
}

// _____________________________________________________________________________
// Lightweight Hardware Perf Counter wrapper using Linux perf_event_open.
// Deliberately kept local to this benchmark: no other benchmark uses
// hardware counters, so a shared utility header would be speculative
// generality. Promote it once a second user appears.
class PerfCounter {
 private:
  int fd_{-1};
  bool enabled_{false};

 public:
  // The counter owns the perf file descriptor: copying would double-close
  // it, so copies are deleted and moves transfer ownership.
  PerfCounter(const PerfCounter&) = delete;
  PerfCounter& operator=(const PerfCounter&) = delete;
  PerfCounter(PerfCounter&& other) noexcept
      : fd_{other.fd_}, enabled_{other.enabled_} {
    other.fd_ = -1;
    other.enabled_ = false;
  }
  PerfCounter& operator=(PerfCounter&& other) noexcept {
    if (this != &other) {
#if defined(__linux__)
      if (fd_ >= 0) {
        ::close(fd_);
      }
#endif
      fd_ = other.fd_;
      enabled_ = other.enabled_;
      other.fd_ = -1;
      other.enabled_ = false;
    }
    return *this;
  }

  PerfCounter(uint32_t type, uint64_t config) {
#if defined(__linux__) && defined(SYS_perf_event_open)
    struct perf_event_attr pe {};
    pe.type = type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = config;
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;

    fd_ = static_cast<int>(syscall(SYS_perf_event_open, &pe, 0, -1, -1, 0));
    if (fd_ >= 0) {
      enabled_ = true;
    }
#else
    (void)type;
    (void)config;
#endif
  }

  ~PerfCounter() {
#if defined(__linux__)
    if (fd_ >= 0) {
      ::close(fd_);
    }
#endif
  }

  void start() {
#if defined(__linux__)
    if (enabled_) {
      ::ioctl(fd_, PERF_EVENT_IOC_RESET, 0);
      ::ioctl(fd_, PERF_EVENT_IOC_ENABLE, 0);
    }
#endif
  }

  // Stop the counter and return the counted events. A short read or a
  // failed read yields 0 (counter value unavailable); reads interrupted by a
  // signal (`EINTR`) are retried.
  uint64_t stop() {
#if defined(__linux__)
    if (enabled_) {
      ::ioctl(fd_, PERF_EVENT_IOC_DISABLE, 0);
      uint64_t count = 0;
      auto* out = reinterpret_cast<char*>(&count);
      size_t bytesRead = 0;
      while (bytesRead < sizeof(count)) {
        ssize_t result =
            ::read(fd_, out + bytesRead, sizeof(count) - bytesRead);
        if (result < 0) {
          if (errno == EINTR) {
            continue;
          }
          return 0;
        }
        if (result == 0) {
          return 0;
        }
        bytesRead += static_cast<size_t>(result);
      }
      return count;
    }
#endif
    return 0;
  }

  [[nodiscard]] bool isSupported() const noexcept { return enabled_; }
};

// _____________________________________________________________________________
// Benchmark Result Record
struct BenchmarkMetricResult {
  std::string method;
  size_t bufferSizeMB;
  size_t chunkSizeKB;
  double durationMs;
  double throughputGBPerSec;
  double probeLatencyNs;
  double estimatedCacheHitRatio;
  uint64_t l1dMisses;
};

// _____________________________________________________________________________
// Benchmark Suite: Standard memcpy vs Non-Temporal StreamingBufferWriter.
class StreamingBufferBenchmark : public BenchmarkInterface {
 private:
  // Runtime configuration, following the `BenchmarkInterface` convention
  // (see `JoinAlgorithmBenchmark`): scalar options bound to members.
  struct ConfigVariables {
    size_t bufferSizeMB_ = 256;
  };
  ConfigVariables configVariables_;

  static constexpr size_t VocabWorkingSetSize =
      8 * 1024 * 1024;  // 8 MB L3 cache warm set
  static constexpr size_t NumProbes = 100'000;

  // Rough cache-hit model for the vocabulary probe latency: a fast cache hit
  // costs a few nanoseconds, a DRAM miss an order of magnitude more. These
  // constants are architecture-dependent (calibrated for the development
  // machine) and only yield a qualitative estimate, not a measurement.
  static constexpr double kCacheHitLatencyNs = 3.0;
  static constexpr double kDramMissLatencyNs = 60.0;

  using AlignedBuf =
      std::vector<char, AlignedAllocator<char, std::allocator<char>, 64>>;

  // Shared per-run state: the export buffers, the vocabulary working set
  // with its probe indices, and the hardware miss counter.
  struct BenchmarkSetup {
    AlignedBuf srcBuffer;
    AlignedBuf destBuffer;
    std::vector<uint32_t> vocabTable;
    std::vector<size_t> probeIndices;
    PerfCounter l1MissCounter = PerfCounter(0, 0);
  };

 public:
  // The default constructor registers the `ConfigManager` options.
  StreamingBufferBenchmark() {
    getConfigManager().addOption(
        "buffer-size-mb",
        "Total export buffer size in megabytes. The benchmark allocates two "
        "such buffers plus an 8 MB vocabulary working set, so reduce this "
        "value on memory-constrained machines.",
        &configVariables_.bufferSizeMB_, size_t{256});
  }

  std::string name() const override {
    return "StreamingBufferWriter vs std::memcpy (Non-Temporal Export "
           "Streaming)";
  }

  // ___________________________________________________________________________
  // Warm up vocabulary probe cache lines into L1/L2/L3 CPU caches.
  static void warmCache(std::vector<uint32_t>& vocabData) {
    uint64_t sum = 0;
    for (size_t i = 0; i < vocabData.size(); i += 16) {
      sum += vocabData[i];
    }
    doNotOptimizeAway(sum);
  }

  // ___________________________________________________________________________
  // `Timer::value()` ticks in microseconds (see `util/Timer.h`). Convert
  // explicitly via `std::chrono` so the physical unit is documented at the
  // conversion site instead of hiding in a magic multiplication factor.
  static double toMilliseconds(ad_utility::timer::Timer::Duration duration) {
    using MilliDouble = std::chrono::duration<double, std::milli>;
    return std::chrono::duration_cast<MilliDouble>(duration).count();
  }
  static double toNanoseconds(ad_utility::timer::Timer::Duration duration) {
    using NanoDouble = std::chrono::duration<double, std::nano>;
    return std::chrono::duration_cast<NanoDouble>(duration).count();
  }

  // ___________________________________________________________________________
  // Probe vocabulary structures after export write and measure access latency.
  static double probeCacheLatency(const std::vector<uint32_t>& vocabData,
                                  const std::vector<size_t>& probeIndices) {
    uint64_t dummySink = 0;
    ad_utility::timer::Timer timer(ad_utility::timer::Timer::Started);

    for (const size_t idx : probeIndices) {
      dummySink += vocabData[idx];
    }

    timer.stop();
    doNotOptimizeAway(dummySink);

    const double totalNs = toNanoseconds(timer.value());
    return totalNs / static_cast<double>(probeIndices.size());
  }

  // ___________________________________________________________________________
  // Estimate the vocabulary cache-hit ratio from the probe latency. See the
  // documentation of `kCacheHitLatencyNs` for the model's limitations.
  static double estimateCacheHitRatio(double latencyNs) {
    return std::clamp(
        1.0 - (latencyNs - kCacheHitLatencyNs) / kDramMissLatencyNs, 0.05,
        0.99);
  }

  // ___________________________________________________________________________
  // Create the L1D miss counter where the platform supports it, otherwise a
  // disabled counter that measures 0.
  static PerfCounter makeL1MissCounter() {
#if defined(__linux__) && defined(PERF_COUNT_HW_CACHE_L1D)
    return PerfCounter(PERF_TYPE_HW_CACHE,
                       PERF_COUNT_HW_CACHE_L1D |
                           (PERF_COUNT_HW_CACHE_OP_READ << 8) |
                           (PERF_COUNT_HW_CACHE_RESULT_MISS << 16));
#else
    return PerfCounter(0, 0);
#endif
  }

  // ___________________________________________________________________________
  // Allocate and initialize the state shared by both benchmark variants.
  static BenchmarkSetup setupBenchmarkState(size_t bufferSizeBytes) {
    BenchmarkSetup setup;
    setup.srcBuffer.assign(bufferSizeBytes, 'Q');
    setup.destBuffer.assign(bufferSizeBytes, 0);

    setup.vocabTable.resize(VocabWorkingSetSize / sizeof(uint32_t));
    std::iota(setup.vocabTable.begin(), setup.vocabTable.end(), 1);

    setup.probeIndices.resize(NumProbes);
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<size_t> dist(0, setup.vocabTable.size() - 1);
    for (size_t& idx : setup.probeIndices) {
      idx = dist(rng);
    }

    // Baseline cache warm up
    warmCache(setup.vocabTable);
    setup.l1MissCounter = makeL1MissCounter();
    return setup;
  }

  // ___________________________________________________________________________
  // Run Standard memcpy Export Benchmark
  BenchmarkMetricResult runMemcpyBenchmark(size_t chunkSize,
                                           size_t bufferSizeBytes) const {
    BenchmarkSetup setup = setupBenchmarkState(bufferSizeBytes);

    setup.l1MissCounter.start();
    ad_utility::timer::Timer timer(ad_utility::timer::Timer::Started);

    // Stream the export buffer in chunks using standard memcpy (pollutes CPU
    // caches)
    for (size_t offset = 0; offset < bufferSizeBytes; offset += chunkSize) {
      const size_t currentChunk = std::min(chunkSize, bufferSizeBytes - offset);
      std::memcpy(setup.destBuffer.data() + offset,
                  setup.srcBuffer.data() + offset, currentChunk);
    }

    timer.stop();
    const uint64_t l1Misses = setup.l1MissCounter.stop();

    const double durationMs = toMilliseconds(timer.value());
    const double gb =
        static_cast<double>(bufferSizeBytes) / (1024.0 * 1024.0 * 1024.0);
    const double throughputGBPerSec =
        (durationMs > 0) ? (gb / (durationMs / 1000.0)) : 0.0;

    // Immediately measure vocabulary access latency post-export
    const double latencyNs =
        probeCacheLatency(setup.vocabTable, setup.probeIndices);
    const double hitRatio = estimateCacheHitRatio(latencyNs);

    return BenchmarkMetricResult{
        .method = "Standard memcpy",
        .bufferSizeMB = bufferSizeBytes / (1024 * 1024),
        .chunkSizeKB = chunkSize / 1024,
        .durationMs = durationMs,
        .throughputGBPerSec = throughputGBPerSec,
        .probeLatencyNs = latencyNs,
        .estimatedCacheHitRatio = hitRatio * 100.0,
        .l1dMisses = l1Misses,
    };
  }

  // ___________________________________________________________________________
  // Run StreamingBufferWriter Benchmark
  BenchmarkMetricResult runStreamingWriterBenchmark(
      size_t chunkSize, size_t bufferSizeBytes) const {
    BenchmarkSetup setup = setupBenchmarkState(bufferSizeBytes);

    setup.l1MissCounter.start();
    ad_utility::timer::Timer timer(ad_utility::timer::Timer::Started);

    StreamingBufferWriter writer(
        std::span<char>{setup.destBuffer.data(), setup.destBuffer.size()});

    // Stream the export buffer in chunks using non-temporal streaming stores
    // (bypasses CPU caches)
    for (size_t offset = 0; offset < bufferSizeBytes; offset += chunkSize) {
      const size_t currentChunk = std::min(chunkSize, bufferSizeBytes - offset);
      writer.write(setup.srcBuffer.data() + offset, currentChunk);
    }
    writer.flush();

    timer.stop();
    const uint64_t l1Misses = setup.l1MissCounter.stop();

    const double durationMs = toMilliseconds(timer.value());
    const double gb =
        static_cast<double>(bufferSizeBytes) / (1024.0 * 1024.0 * 1024.0);
    const double throughputGBPerSec =
        (durationMs > 0) ? (gb / (durationMs / 1000.0)) : 0.0;

    // Immediately measure vocabulary access latency post-export
    const double latencyNs =
        probeCacheLatency(setup.vocabTable, setup.probeIndices);
    const double hitRatio = estimateCacheHitRatio(latencyNs);

    return BenchmarkMetricResult{
        .method = "StreamingBufferWriter",
        .bufferSizeMB = bufferSizeBytes / (1024 * 1024),
        .chunkSizeKB = chunkSize / 1024,
        .durationMs = durationMs,
        .throughputGBPerSec = throughputGBPerSec,
        .probeLatencyNs = latencyNs,
        .estimatedCacheHitRatio = hitRatio * 100.0,
        .l1dMisses = l1Misses,
    };
  }

  // ___________________________________________________________________________
  // Print one result row. The percentage column streams the value with
  // `fixed`/`setprecision` directly instead of truncating a `to_string`
  // result, which broke for values outside [10, 100).
  static void printResultRow(const BenchmarkMetricResult& result,
                             const std::string& hitSuffix) {
    std::cout << std::left << std::setw(24) << result.method << std::setw(14)
              << (std::to_string(result.chunkSizeKB) + " KB") << std::fixed
              << std::setprecision(2) << std::setw(12) << result.durationMs
              << std::fixed << std::setprecision(2) << std::setw(18)
              << result.throughputGBPerSec << std::fixed << std::setprecision(2)
              << std::setw(18) << result.probeLatencyNs << std::fixed
              << std::setprecision(1) << std::setw(15)
              << result.estimatedCacheHitRatio << "%" << hitSuffix << "\n";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    AD_CONTRACT_CHECK(configVariables_.bufferSizeMB_ <=
                      std::numeric_limits<size_t>::max() / (1024 * 1024));
    const size_t bufferSizeBytes = configVariables_.bufferSizeMB_ * 1024 * 1024;
    if (bufferSizeBytes == 0) {
      AD_THROW("buffer-size-mb must be positive");
    }
    // Chunk sweep, clamped to the configured buffer size so small
    // `--buffer-size-mb` values on memory-constrained machines still yield
    // a meaningful sweep instead of oversized chunks.
    const std::vector<size_t> candidateChunkSizes = {
        64 * 1024,          // 64 KB
        1024 * 1024,        // 1 MB
        16 * 1024 * 1024,   // 16 MB
        256 * 1024 * 1024,  // 256 MB
    };
    std::vector<size_t> chunkSizes;
    for (const size_t candidate : candidateChunkSizes) {
      if (candidate <= bufferSizeBytes) {
        chunkSizes.push_back(candidate);
      }
    }
    if (chunkSizes.empty()) {
      chunkSizes.push_back(bufferSizeBytes);
    }

    std::cout << "\n==========================================================="
                 "=============================================\n"
              << " QLever Non-Temporal Streaming Stores Benchmark: Standard "
                 "memcpy vs StreamingBufferWriter\n"
              << " Total Export Buffer Size: "
              << (bufferSizeBytes / (1024 * 1024))
              << " MB | Vocabulary Warm Set: "
              << (VocabWorkingSetSize / (1024 * 1024)) << " MB\n"
              << "============================================================="
                 "===========================================\n";

    std::cout << std::left << std::setw(24) << "Method" << std::setw(14)
              << "Chunk Size" << std::setw(12) << "Time (ms)" << std::setw(18)
              << "Throughput(GB/s)" << std::setw(18) << "Vocab Probe(ns)"
              << std::setw(16) << "Cache Hit Est."
              << "\n"
              << std::string(102, '-') << "\n";

    for (const size_t chunkSize : chunkSizes) {
      const std::string desc =
          "Chunk " + std::to_string(chunkSize / 1024) + " KB";

      // The two export buffers plus the vocabulary set may exceed the
      // available memory on small machines. Skip the remaining sweep
      // gracefully instead of crashing with `std::bad_alloc`; measurements
      // collected so far are still reported.
      BenchmarkMetricResult memcpyRes{};
      BenchmarkMetricResult streamRes{};
      // `ResultGroup::addMeasurement(descriptor, lambda)` runs the lambda
      // synchronously in the `ResultEntry` constructor, so capturing the
      // loop-local result structs by reference is safe here.
      try {
        results.addMeasurement(
            "memcpy: " + desc,
            [this, chunkSize, bufferSizeBytes, &memcpyRes]() {
              memcpyRes = runMemcpyBenchmark(chunkSize, bufferSizeBytes);
            });

        results.addMeasurement("StreamingWriter: " + desc, [this, chunkSize,
                                                            bufferSizeBytes,
                                                            &streamRes]() {
          streamRes = runStreamingWriterBenchmark(chunkSize, bufferSizeBytes);
        });
      } catch (const std::bad_alloc&) {
        std::cerr << "Skipping chunk size " << chunkSize / 1024
                  << " KB: cannot allocate two "
                  << bufferSizeBytes / (1024 * 1024)
                  << " MB buffers on this machine. Reduce the sweep via "
                     "--buffer-size-mb (e.g. --buffer-size-mb 16)."
                  << std::endl;
        break;
      }

      printResultRow(memcpyRes, "");
      printResultRow(streamRes, " (HOT)");
      std::cout << std::string(102, '.') << "\n";
    }

    std::cout << "============================================================="
                 "===========================================\n\n";

    return results;
  }
};

AD_REGISTER_BENCHMARK(StreamingBufferBenchmark);

}  // namespace ad_benchmark
