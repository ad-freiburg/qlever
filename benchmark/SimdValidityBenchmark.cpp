// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#if defined(__linux__)
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "engine/SimdValidityBitmask.h"
#include "global/Id.h"
#include "global/ValueId.h"

using namespace ad_utility::simd;

namespace {

// _____________________________________________________________________________
// Hardware branch performance counter using Linux perf_event.
class HardwarePerfCounter {
 private:
#if defined(__linux__)
  int branchFd_ = -1;
  int missFd_ = -1;
  bool isSupported_ = false;

  static int openPerfEvent(uint32_t type, uint64_t config) {
    struct perf_event_attr pe {};
    pe.type = type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = config;
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;
    return static_cast<int>(syscall(__NR_perf_event_open, &pe, 0, -1, -1, 0));
  }
#endif

 public:
  HardwarePerfCounter() {
#if defined(__linux__)
    branchFd_ =
        openPerfEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_INSTRUCTIONS);
    missFd_ = openPerfEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);
    isSupported_ = (branchFd_ >= 0 && missFd_ >= 0);
#endif
  }

  ~HardwarePerfCounter() {
#if defined(__linux__)
    if (branchFd_ >= 0) ::close(branchFd_);
    if (missFd_ >= 0) ::close(missFd_);
#endif
  }

  [[nodiscard]] bool isSupported() const noexcept {
#if defined(__linux__)
    return isSupported_;
#else
    return false;
#endif
  }

  void start() noexcept {
#if defined(__linux__)
    if (isSupported_) {
      ioctl(branchFd_, PERF_EVENT_IOC_RESET, 0);
      ioctl(missFd_, PERF_EVENT_IOC_RESET, 0);
      ioctl(branchFd_, PERF_EVENT_IOC_ENABLE, 0);
      ioctl(missFd_, PERF_EVENT_IOC_ENABLE, 0);
    }
#endif
  }

  void stop(uint64_t& branchCount, uint64_t& missCount) noexcept {
#if defined(__linux__)
    if (isSupported_) {
      ioctl(branchFd_, PERF_EVENT_IOC_DISABLE, 0);
      ioctl(missFd_, PERF_EVENT_IOC_DISABLE, 0);
      ssize_t r1 = read(branchFd_, &branchCount, sizeof(uint64_t));
      ssize_t r2 = read(missFd_, &missCount, sizeof(uint64_t));
      (void)r1;
      (void)r2;
    } else {
      branchCount = 0;
      missCount = 0;
    }
#else
    branchCount = 0;
    missCount = 0;
#endif
  }
};

// _____________________________________________________________________________
// Synthetic dataset representing 2,000,000 rows with 50% unbound OPTIONAL
// columns.
struct OptionalColumnDataset {
  std::vector<ValueId> columnData_;
  std::vector<std::string> stringTable_;

  static OptionalColumnDataset generate(size_t numRows = 2'000'000,
                                        double unboundFraction = 0.50,
                                        uint32_t seed = 42) {
    OptionalColumnDataset ds;
    ds.columnData_.resize(numRows);
    ds.stringTable_.reserve(1000);

    for (size_t i = 0; i < 1000; ++i) {
      ds.stringTable_.push_back("http://example.org/opt_val_" +
                                std::to_string(i));
    }

    std::mt19937 gen(seed);
    std::bernoulli_distribution isUnboundDist(unboundFraction);
    std::uniform_int_distribution<size_t> strDist(0,
                                                  ds.stringTable_.size() - 1);

    // Generate in blocks representing realistic SPARQL OPTIONAL matches
    for (size_t i = 0; i < numRows; ++i) {
      if (isUnboundDist(gen)) {
        ds.columnData_[i] = ValueId::makeUndefined();
      } else {
        ds.columnData_[i] =
            ValueId::makeFromVocabIndex(VocabIndex::make(strDist(gen)));
      }
    }
    return ds;
  }
};

// _____________________________________________________________________________
// Baseline 1: Traditional cell-by-cell isUndefined() checking with scalar
// branching.
struct CellByCellScalarExporter {
  static char* exportCsv(ql::span<const ValueId> column, char* out) noexcept {
    for (const auto& id : column) {
      if (id.isUndefined()) {
        *out++ = ',';
        *out++ = '\n';
      } else {
        // Bound value: write dummy value and delimiter
        *out++ = '1';
        *out++ = ',';
        *out++ = '\n';
      }
    }
    return out;
  }
};

// _____________________________________________________________________________
// Method 2: SIMD Validity Bitmask with 64-row vectorized fast path.
struct SimdValidityBitmaskExporter {
  static char* exportCsv(ql::span<const ValueId> column, char* out) noexcept {
    const size_t numRows = column.size();
    const size_t numBatches = numRows / 64;
    const ValueId* ptr = column.data();

    for (size_t b = 0; b < numBatches; ++b) {
      const ValueId* batchPtr = ptr + b * 64;
      ValidityBitmask64 mask = SimdValidityScanner::scanBatch64(batchPtr);

      if (mask.allUnbound()) [[likely]] {
        // Zero cell checks! 64 empty delimiter pairs written with single
        // vectorized stores
        out = SimdValidityScanner::writeUnboundRowsCsv(out, ',', '\n');
      } else if (mask.allValid()) {
        // All 64 bound
        for (size_t i = 0; i < 64; ++i) {
          *out++ = '1';
          *out++ = ',';
          *out++ = '\n';
        }
      } else {
        // Mixed: iterate through bitmask
        for (size_t i = 0; i < 64; ++i) {
          if (mask.isRowValid(i)) {
            *out++ = '1';
          }
          *out++ = ',';
          *out++ = '\n';
        }
      }
    }

    // Tail rows (< 64)
    size_t tailStart = numBatches * 64;
    for (size_t i = tailStart; i < numRows; ++i) {
      if (!column[i].isUndefined()) {
        *out++ = '1';
      }
      *out++ = ',';
      *out++ = '\n';
    }
    return out;
  }
};

// _____________________________________________________________________________
// Method 3: SIMD Direct Bitmask forEachValid Exporter
struct SimdForEachBitmaskExporter {
  static char* exportCsv(ql::span<const ValueId> column, char* out) noexcept {
    const size_t numRows = column.size();
    const size_t numBatches = numRows / 64;
    const ValueId* ptr = column.data();

    for (size_t b = 0; b < numBatches; ++b) {
      const ValueId* batchPtr = ptr + b * 64;
      ValidityBitmask64 mask = SimdValidityScanner::scanBatch64(batchPtr);

      if (mask.allUnbound()) {
        out = SimdValidityScanner::writeUnboundRowsCsv(out, ',', '\n');
      } else {
        for (size_t i = 0; i < 64; ++i) {
          if (mask.isRowValid(i)) {
            *out++ = '1';
          }
          *out++ = ',';
          *out++ = '\n';
        }
      }
    }

    size_t tailStart = numBatches * 64;
    for (size_t i = tailStart; i < numRows; ++i) {
      if (!column[i].isUndefined()) {
        *out++ = '1';
      }
      *out++ = ',';
      *out++ = '\n';
    }
    return out;
  }
};

struct BenchmarkResult {
  std::string name_;
  double elapsedMs_ = 0.0;
  double throughputMRowsPerSec_ = 0.0;
  double nsPerRow_ = 0.0;
  uint64_t totalBranches_ = 0;
  uint64_t branchMisses_ = 0;
  double branchMissRate_ = 0.0;
  double branchMissesPerRow_ = 0.0;
  size_t bytesWritten_ = 0;
};

template <typename Exporter>
BenchmarkResult runBenchmark(const std::string& name,
                             const OptionalColumnDataset& ds,
                             std::vector<char>& outputBuffer,
                             HardwarePerfCounter& perf, size_t iterations = 5) {
  // Warmup
  Exporter::exportCsv(ds.columnData_, outputBuffer.data());

  double totalMs = 0.0;
  uint64_t totalBranches = 0;
  uint64_t totalMisses = 0;
  size_t bytesWritten = 0;

  for (size_t iter = 0; iter < iterations; ++iter) {
    perf.start();
    auto t0 = std::chrono::high_resolution_clock::now();

    char* endPtr = Exporter::exportCsv(ds.columnData_, outputBuffer.data());

    auto t1 = std::chrono::high_resolution_clock::now();
    uint64_t branches = 0;
    uint64_t misses = 0;
    perf.stop(branches, misses);

    bytesWritten = static_cast<size_t>(endPtr - outputBuffer.data());
    totalMs += std::chrono::duration<double, std::milli>(t1 - t0).count();
    totalBranches += branches;
    totalMisses += misses;
  }

  double avgMs = totalMs / static_cast<double>(iterations);
  double totalRows = static_cast<double>(ds.columnData_.size());
  double mRowsPerSec = (totalRows / (avgMs / 1000.0)) / 1e6;
  double nsPerRow = (avgMs * 1e6) / totalRows;

  uint64_t avgBranches = totalBranches / iterations;
  uint64_t avgMisses = totalMisses / iterations;
  double missRate = avgBranches > 0 ? (100.0 * static_cast<double>(avgMisses) /
                                       static_cast<double>(avgBranches))
                                    : 0.0;
  double missesPerRow = static_cast<double>(avgMisses) / totalRows;

  return BenchmarkResult{name,     avgMs,        mRowsPerSec,
                         nsPerRow, avgBranches,  avgMisses,
                         missRate, missesPerRow, bytesWritten};
}

void printResults(const std::vector<BenchmarkResult>& results) {
  std::cout << "\n============================================================="
               "===========================================\n";
  std::cout << " Optimization 23: SIMD Validity Bitmasks Microbenchmark\n";
  std::cout
      << " Workload: 2,000,000 Rows with 50% Unbound OPTIONAL SPARQL Columns\n";
  std::cout << "==============================================================="
               "=========================================\n\n";

  std::cout << std::left << std::setw(32) << "Exporter Implementation"
            << std::right << std::setw(12) << "Time (ms)" << std::setw(18)
            << "Throughput (M/s)" << std::setw(14) << "ns / row"
            << std::setw(16) << "Branch Misses" << std::setw(14) << "Miss Rate"
            << std::setw(14) << "Misses/Row"
            << "\n";
  std::cout << std::string(120, '-') << "\n";

  for (const auto& r : results) {
    std::cout << std::left << std::setw(32) << r.name_ << std::right
              << std::fixed << std::setprecision(2) << std::setw(12)
              << r.elapsedMs_ << std::setw(18) << r.throughputMRowsPerSec_
              << std::setw(14) << r.nsPerRow_ << std::setw(16)
              << r.branchMisses_ << std::setw(13) << r.branchMissRate_ << "%"
              << std::setw(14) << std::setprecision(4) << r.branchMissesPerRow_
              << "\n";
  }

  std::cout << std::string(120, '-') << "\n\n";

  if (results.size() >= 2) {
    double baseThroughput = results[0].throughputMRowsPerSec_;
    double simdThroughput = results[1].throughputMRowsPerSec_;
    double speedup = simdThroughput / baseThroughput;
    std::cout << ">> SIMD Validity Bitmask Speedup over Cell-by-Cell Checking: "
              << std::fixed << std::setprecision(2) << speedup << "x (+"
              << ((speedup - 1.0) * 100.0) << "% throughput)\n";
  }
  std::cout << "==============================================================="
               "=========================================\n\n";
}

}  // namespace

int main(int argc, char** argv) {
  size_t numRows = 2'000'000;
  if (argc > 1) {
    numRows = std::stoull(argv[1]);
  }

  std::cout << "Generating synthetic OPTIONAL column dataset with " << numRows
            << " rows (50% unbound)...\n";
  auto dataset = OptionalColumnDataset::generate(numRows, 0.50);

  // Allocate buffer for formatted output
  std::vector<char> outputBuffer(numRows * 16);

  HardwarePerfCounter perf;
  if (perf.isSupported()) {
    std::cout
        << "[Hardware Performance Counters: Enabled (Linux perf_event)]\n";
  } else {
    std::cout << "[Hardware Performance Counters: Unavailable / Restricted - "
                 "timing only]\n";
  }

  std::vector<BenchmarkResult> results;

  std::cout << "Running Baseline: Cell-by-Cell isUndefined() Checking...\n";
  results.push_back(runBenchmark<CellByCellScalarExporter>(
      "Cell-by-Cell isUndefined()", dataset, outputBuffer, perf));

  std::cout
      << "Running Optimization: SIMD Validity Bitmask (AVX2 Fast Path)...\n";
  results.push_back(runBenchmark<SimdValidityBitmaskExporter>(
      "SIMD Validity Bitmask (AVX2)", dataset, outputBuffer, perf));

  std::cout << "Running Optimization: SIMD Bitmask Linear Scan...\n";
  results.push_back(runBenchmark<SimdForEachBitmaskExporter>(
      "SIMD Bitmask Scan", dataset, outputBuffer, perf));

  printResults(results);
  return 0;
}
