// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/RegisteredIoUringReader.h"

// Optional inclusion of QLever benchmark infrastructure
#if __has_include("../benchmark/infrastructure/Benchmark.h")
#include "../benchmark/infrastructure/Benchmark.h"
#define QLEVER_HAS_BENCHMARK_INFRASTRUCTURE 1
#endif

namespace ad_benchmark {
namespace {

using namespace ad_utility::export_prototypes;

// 1 GB simulated vocabulary file constants:
constexpr size_t kTotalFileSizeBytes = 1024ULL * 1024ULL * 1024ULL;  // 1 GB
constexpr size_t kBlockSizeBytes = 4096;                             // 4 KB
constexpr size_t kDefaultBatchBlocks = 256;  // 1 MB per batch (256 * 4KB)

// _____________________________________________________________________________
// Helper to generate a 1GB simulated vocabulary binary file on disk.
class SimulatedVocabularyFile {
 private:
  std::string filePath_;
  bool isCreated_ = false;

 public:
  explicit SimulatedVocabularyFile(
      std::string_view pathTemplate = "/tmp/qlever_vocab_sim_XXXXXX.bin") {
    // `mkstemps` needs a mutable, null-terminated buffer.
    std::string tempPath{pathTemplate};
    int fd = mkstemps(tempPath.data(), 4);
    if (fd < 0) {
      AD_THROW("mkstemps failed to create temporary vocabulary file");
    }
    filePath_ = std::move(tempPath);

    std::cout << "Generating 1GB simulated vocabulary data in: " << filePath_
              << " ... " << std::flush;

    // Allocate 4KB aligned write buffer
    void* writeBuf = nullptr;
    constexpr size_t writeChunkSize = 1024 * 1024;  // 1 MB chunks
    if (posix_memalign(&writeBuf, kDirectIoAlignment, writeChunkSize) != 0) {
      ::close(fd);
      AD_THROW("posix_memalign failed");
    }

    auto* bytePtr = static_cast<char*>(writeBuf);
    std::mt19937_64 rng(42);

    // Populate with simulated vocabulary entries: prefix IDs, string tokens,
    // offsets
    size_t bytesWritten = 0;
    while (bytesWritten < kTotalFileSizeBytes) {
      for (size_t i = 0; i < writeChunkSize; i += sizeof(uint64_t)) {
        uint64_t val = rng();
        std::memcpy(bytePtr + i, &val, sizeof(uint64_t));
      }

      ssize_t written = ::write(fd, writeBuf, writeChunkSize);
      if (written != static_cast<ssize_t>(writeChunkSize)) {
        std::free(writeBuf);
        ::close(fd);
        AD_THROW("Failed to write full chunk to simulated vocabulary file");
      }
      bytesWritten += writeChunkSize;
    }

    // `fdatasync` is Linux-specific; macOS only provides `fsync`.
#ifdef __APPLE__
    ::fsync(fd);
#else
    ::fdatasync(fd);
#endif
    ::close(fd);
    std::free(writeBuf);
    isCreated_ = true;
    std::cout << "Done (1,073,741,824 bytes written)." << std::endl;
  }

  ~SimulatedVocabularyFile() {
    if (isCreated_ && !filePath_.empty()) {
      ::unlink(filePath_.c_str());
    }
  }

  SimulatedVocabularyFile(const SimulatedVocabularyFile&) = delete;
  SimulatedVocabularyFile& operator=(const SimulatedVocabularyFile&) = delete;

  [[nodiscard]] const std::string& path() const noexcept { return filePath_; }
};

// _____________________________________________________________________________
// Benchmark result metrics struct.
struct BenchmarkMetric {
  std::string name;
  double elapsedSeconds = 0.0;
  double throughputMBs = 0.0;
  double throughputGBs = 0.0;
  double iops = 0.0;
  double avgBatchLatencyUs = 0.0;
  double speedupVsBaseline = 1.0;
};

// _____________________________________________________________________________
// Benchmark test harness evaluating I/O paradigms across the 1GB simulated
// dataset.
class IoUringDirectBenchmarkRunner {
 private:
  std::string filePath_;
  size_t batchBlocks_ = kDefaultBatchBlocks;

 public:
  explicit IoUringDirectBenchmarkRunner(
      std::string filePath, size_t batchBlocks = kDefaultBatchBlocks)
      : filePath_{std::move(filePath)}, batchBlocks_{batchBlocks} {}

  // 1. Baseline: Synchronous pread() with standard page cache
  BenchmarkMetric runSyncPread(bool randomAccess = false) const {
    DirectIoFile file(filePath_, /*useDirectIo=*/false);
    AD_CONTRACT_CHECK(file.isOpen());

    PinnedArena bufferArena(batchBlocks_, kBlockSizeBytes);

    std::vector<uint64_t> offsets = generateOffsets(randomAccess);
    const size_t numBatches = offsets.size();

    auto startTime = std::chrono::steady_clock::now();
    size_t totalBytes = 0;

    for (size_t b = 0; b < numBatches; ++b) {
      uint64_t baseOffset = offsets[b];
      for (size_t i = 0; i < batchBlocks_; ++i) {
        uint64_t blockOffset = baseOffset + (i * kBlockSizeBytes);
        if (blockOffset + kBlockSizeBytes > kTotalFileSizeBytes) {
          blockOffset = 0;
        }
        RegisteredIoUringReader::readSync(file.fd(), blockOffset,
                                          bufferArena.getSlotSpan(i),
                                          /*directIo=*/false);
        totalBytes += kBlockSizeBytes;
      }
    }

    auto endTime = std::chrono::steady_clock::now();
    return calculateMetric("1. Sync pread (Page Cache)", startTime, endTime,
                           totalBytes, numBatches);
  }

  // 2. Synchronous pread() with Direct I/O (O_DIRECT)
  BenchmarkMetric runSyncDirectPread(bool randomAccess = false) const {
    DirectIoFile file(filePath_, /*useDirectIo=*/true);
    AD_CONTRACT_CHECK(file.isOpen());

    PinnedArena bufferArena(batchBlocks_, kBlockSizeBytes);
    std::vector<uint64_t> offsets = generateOffsets(randomAccess);
    const size_t numBatches = offsets.size();

    auto startTime = std::chrono::steady_clock::now();
    size_t totalBytes = 0;

    for (size_t b = 0; b < numBatches; ++b) {
      uint64_t baseOffset = offsets[b];
      for (size_t i = 0; i < batchBlocks_; ++i) {
        uint64_t blockOffset = baseOffset + (i * kBlockSizeBytes);
        if (blockOffset + kBlockSizeBytes > kTotalFileSizeBytes) {
          blockOffset = 0;
        }
        RegisteredIoUringReader::readSync(file.fd(), blockOffset,
                                          bufferArena.getSlotSpan(i),
                                          /*directIo=*/true);
        totalBytes += kBlockSizeBytes;
      }
    }

    auto endTime = std::chrono::steady_clock::now();
    return calculateMetric("2. Sync pread (O_DIRECT)", startTime, endTime,
                           totalBytes, numBatches);
  }

  // 3. io_uring Standard (Unpinned buffers & Unregistered files)
  BenchmarkMetric runIoUringUnpinned(bool randomAccess = false) const {
    DirectIoFile file(filePath_, /*useDirectIo=*/false);
    AD_CONTRACT_CHECK(file.isOpen());

    RegisteredReaderConfig config;
    config.ringEntries = 512;
    config.useDirectIo = false;
    config.useRegisteredFiles = false;
    config.useRegisteredBuffers = false;

    RegisteredIoUringReader reader(config);
    PinnedArena bufferArena(batchBlocks_, kBlockSizeBytes);

    std::vector<uint64_t> offsets = generateOffsets(randomAccess);
    const size_t numBatches = offsets.size();
    std::vector<BlockReadRequest> requests(batchBlocks_);

    auto startTime = std::chrono::steady_clock::now();
    size_t totalBytes = 0;

    for (size_t b = 0; b < numBatches; ++b) {
      uint64_t baseOffset = offsets[b];
      for (size_t i = 0; i < batchBlocks_; ++i) {
        uint64_t blockOffset = baseOffset + (i * kBlockSizeBytes);
        if (blockOffset + kBlockSizeBytes > kTotalFileSizeBytes) {
          blockOffset = 0;
        }
        requests[i] = BlockReadRequest(file.fd(), blockOffset, /*bufIndex=*/0,
                                       /*bufOffset=*/0, kBlockSizeBytes,
                                       bufferArena.getSlotSpan(i).data(),
                                       /*requireDirectIoAlignment=*/false);
      }

      auto batchId = reader.submitBatch(requests);
      auto res = reader.waitBatch(batchId);
      totalBytes += res.totalBytesRead;
    }

    auto endTime = std::chrono::steady_clock::now();
    return calculateMetric("3. io_uring (Unpinned + Unregistered)", startTime,
                           endTime, totalBytes, numBatches);
  }

  // 4. io_uring with O_DIRECT (Unpinned buffers)
  BenchmarkMetric runIoUringDirectUnpinned(bool randomAccess = false) const {
    DirectIoFile file(filePath_, /*useDirectIo=*/true);
    AD_CONTRACT_CHECK(file.isOpen());

    RegisteredReaderConfig config;
    config.ringEntries = 512;
    config.useDirectIo = true;
    config.useRegisteredFiles = false;
    config.useRegisteredBuffers = false;

    RegisteredIoUringReader reader(config);
    PinnedArena bufferArena(batchBlocks_, kBlockSizeBytes);

    std::vector<uint64_t> offsets = generateOffsets(randomAccess);
    const size_t numBatches = offsets.size();
    std::vector<BlockReadRequest> requests(batchBlocks_);

    auto startTime = std::chrono::steady_clock::now();
    size_t totalBytes = 0;

    for (size_t b = 0; b < numBatches; ++b) {
      uint64_t baseOffset = offsets[b];
      for (size_t i = 0; i < batchBlocks_; ++i) {
        uint64_t blockOffset = baseOffset + (i * kBlockSizeBytes);
        if (blockOffset + kBlockSizeBytes > kTotalFileSizeBytes) {
          blockOffset = 0;
        }
        requests[i] = BlockReadRequest(file.fd(), blockOffset, /*bufIndex=*/0,
                                       /*bufOffset=*/0, kBlockSizeBytes,
                                       bufferArena.getSlotSpan(i).data(),
                                       /*requireDirectIoAlignment=*/true);
      }

      auto batchId = reader.submitBatch(requests);
      auto res = reader.waitBatch(batchId);
      totalBytes += res.totalBytesRead;
    }

    auto endTime = std::chrono::steady_clock::now();
    return calculateMetric("4. io_uring O_DIRECT (Unpinned)", startTime,
                           endTime, totalBytes, numBatches);
  }

  // 5. io_uring with Registered Files (IORING_REGISTER_FILES) + Unpinned
  // Buffers
  BenchmarkMetric runIoUringRegisteredFiles(bool randomAccess = false) const {
    DirectIoFile file(filePath_, /*useDirectIo=*/true);
    AD_CONTRACT_CHECK(file.isOpen());

    RegisteredReaderConfig config;
    config.ringEntries = 512;
    config.useDirectIo = true;
    config.useRegisteredFiles = true;
    config.useRegisteredBuffers = false;

    RegisteredIoUringReader reader(config);
    int fd = file.fd();
    reader.registerFiles({&fd, 1});

    PinnedArena bufferArena(batchBlocks_, kBlockSizeBytes);
    std::vector<uint64_t> offsets = generateOffsets(randomAccess);
    const size_t numBatches = offsets.size();
    std::vector<BlockReadRequest> requests(batchBlocks_);

    auto startTime = std::chrono::steady_clock::now();
    size_t totalBytes = 0;

    for (size_t b = 0; b < numBatches; ++b) {
      uint64_t baseOffset = offsets[b];
      for (size_t i = 0; i < batchBlocks_; ++i) {
        uint64_t blockOffset = baseOffset + (i * kBlockSizeBytes);
        if (blockOffset + kBlockSizeBytes > kTotalFileSizeBytes) {
          blockOffset = 0;
        }
        requests[i] = BlockReadRequest(
            /*fileIndex=*/0, blockOffset, /*bufIndex=*/0, /*bufOffset=*/0,
            kBlockSizeBytes, bufferArena.getSlotSpan(i).data(),
            /*requireDirectIoAlignment=*/true);
      }

      auto batchId = reader.submitBatch(requests);
      auto res = reader.waitBatch(batchId);
      totalBytes += res.totalBytesRead;
    }

    auto endTime = std::chrono::steady_clock::now();
    return calculateMetric("5. io_uring (Registered Files + O_DIRECT)",
                           startTime, endTime, totalBytes, numBatches);
  }

  // 6. io_uring Fully Registered: IORING_REGISTER_FILES +
  // IORING_REGISTER_BUFFERS + O_DIRECT
  BenchmarkMetric runIoUringFullyRegistered(bool randomAccess = false) const {
    DirectIoFile file(filePath_, /*useDirectIo=*/true);
    AD_CONTRACT_CHECK(file.isOpen());

    RegisteredReaderConfig config;
    config.ringEntries = 512;
    config.useDirectIo = true;
    config.useRegisteredFiles = true;
    config.useRegisteredBuffers = true;

    RegisteredIoUringReader reader(config);
    int fd = file.fd();
    reader.registerFiles({&fd, 1});

    PinnedArena bufferArena(batchBlocks_, kBlockSizeBytes);
    reader.registerBuffers(bufferArena.iovecs());

    std::vector<uint64_t> offsets = generateOffsets(randomAccess);
    const size_t numBatches = offsets.size();
    std::vector<BlockReadRequest> requests(batchBlocks_);

    auto startTime = std::chrono::steady_clock::now();
    size_t totalBytes = 0;

    for (size_t b = 0; b < numBatches; ++b) {
      uint64_t baseOffset = offsets[b];
      for (size_t i = 0; i < batchBlocks_; ++i) {
        uint64_t blockOffset = baseOffset + (i * kBlockSizeBytes);
        if (blockOffset + kBlockSizeBytes > kTotalFileSizeBytes) {
          blockOffset = 0;
        }
        // Zero-copy DMA fixed buffer request
        requests[i] = BlockReadRequest(
            /*fileIndex=*/0, blockOffset,
            /*bufferIndex=*/static_cast<uint32_t>(i),
            /*bufferOffset=*/0, kBlockSizeBytes,
            bufferArena.getSlotSpan(i).data(),
            /*requireDirectIoAlignment=*/true);
      }

      auto batchId = reader.submitBatch(requests);
      auto res = reader.waitBatch(batchId);
      totalBytes += res.totalBytesRead;
    }

    auto endTime = std::chrono::steady_clock::now();
    return calculateMetric(
        "6. io_uring (Fully Registered Files+Buffers+O_DIRECT)", startTime,
        endTime, totalBytes, numBatches);
  }

 private:
  std::vector<uint64_t> generateOffsets(bool randomAccess) const {
    const size_t batchSizeBytes = batchBlocks_ * kBlockSizeBytes;
    const size_t numBatches = kTotalFileSizeBytes / batchSizeBytes;
    std::vector<uint64_t> offsets(numBatches);

    for (size_t i = 0; i < numBatches; ++i) {
      offsets[i] = i * batchSizeBytes;
    }

    if (randomAccess) {
      std::mt19937_64 rng(1337);
      std::shuffle(offsets.begin(), offsets.end(), rng);
    }

    return offsets;
  }

  BenchmarkMetric calculateMetric(
      std::string_view name, std::chrono::steady_clock::time_point startTime,
      std::chrono::steady_clock::time_point endTime, size_t totalBytes,
      size_t numBatches) const {
    std::chrono::duration<double> elapsed = endTime - startTime;
    double elapsedSec = elapsed.count();
    double mbRead = static_cast<double>(totalBytes) / (1024.0 * 1024.0);
    double gbRead =
        static_cast<double>(totalBytes) / (1024.0 * 1024.0 * 1024.0);
    double totalBlocks = static_cast<double>(totalBytes) / kBlockSizeBytes;

    BenchmarkMetric m;
    m.name = std::string(name);
    m.elapsedSeconds = elapsedSec;
    m.throughputMBs = mbRead / elapsedSec;
    m.throughputGBs = gbRead / elapsedSec;
    m.iops = totalBlocks / elapsedSec;
    m.avgBatchLatencyUs =
        (elapsedSec * 1'000'000.0) / static_cast<double>(numBatches);
    return m;
  }
};

// _____________________________________________________________________________
// Formatter for benchmark results table (only used by the standalone entry
// point below; the registered infrastructure benchmark reports JSON instead).
#ifndef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
void printResultsTable(std::string_view accessMode,
                       std::vector<BenchmarkMetric>& results) {
  if (results.empty()) return;

  double baselineThroughput = results[0].throughputMBs;
  for (auto& r : results) {
    r.speedupVsBaseline = r.throughputMBs / baselineThroughput;
  }

  std::cout << "\n============================================================="
               "===========================\n";
  std::cout << "  BENCHMARK: 1GB Simulated Vocabulary Scan (" << accessMode
            << ")\n";
  std::cout << "  Dataset: 1,073,741,824 bytes | Block Size: 4 KB | Total "
               "Blocks: 262,144\n";
  std::cout << "==============================================================="
               "=========================\n";
  std::cout << std::left << std::setw(50) << "I/O Paradigm" << std::right
            << std::setw(12) << "Time (s)" << std::setw(14) << "MB/s"
            << std::setw(12) << "GB/s" << std::setw(14) << "IOPS"
            << std::setw(12) << "Speedup" << "\n";
  std::cout << "---------------------------------------------------------------"
               "-------------------------\n";

  for (const auto& r : results) {
    std::cout << std::left << std::setw(50) << r.name << std::right
              << std::fixed << std::setprecision(3) << std::setw(12)
              << r.elapsedSeconds << std::setw(14) << r.throughputMBs
              << std::setw(12) << r.throughputGBs << std::fixed
              << std::setprecision(0) << std::setw(14) << r.iops << std::fixed
              << std::setprecision(2) << std::setw(11) << r.speedupVsBaseline
              << "x\n";
  }
  std::cout << "==============================================================="
               "=========================\n\n";
}
#endif  // QLEVER_HAS_BENCHMARK_INFRASTRUCTURE

}  // namespace

#ifdef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
// Integration into QLever's Benchmark Framework
class IoUringDirectBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "io_uring Registered Files, Fixed Buffers, and O_DIRECT Vocabulary "
           "Scan";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    auto& group = results.addGroup("1GB Vocabulary I/O Strategies");

    SimulatedVocabularyFile vocabFile;
    IoUringDirectBenchmarkRunner runner(vocabFile.path());

    group.addMeasurement("1. Sync pread (Page Cache)", [&]() {
      return runner.runSyncPread().elapsedSeconds;
    });
    group.addMeasurement("2. Sync pread (O_DIRECT)", [&]() {
      return runner.runSyncDirectPread().elapsedSeconds;
    });
    group.addMeasurement("3. io_uring (Unpinned)", [&]() {
      return runner.runIoUringUnpinned().elapsedSeconds;
    });
    group.addMeasurement("4. io_uring (O_DIRECT)", [&]() {
      return runner.runIoUringDirectUnpinned().elapsedSeconds;
    });
    group.addMeasurement("5. io_uring (Registered Files)", [&]() {
      return runner.runIoUringRegisteredFiles().elapsedSeconds;
    });
    group.addMeasurement("6. io_uring (Fully Registered DMA)", [&]() {
      return runner.runIoUringFullyRegistered().elapsedSeconds;
    });

    return results;
  }
};

AD_REGISTER_BENCHMARK(IoUringDirectBenchmark);
#endif

}  // namespace ad_benchmark

#ifndef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
// Standalone executable entry point
int main(int argc, char** argv) {
  std::cout
      << "=================================================================\n";
  std::cout << " QLever Export Prototype: Registered io_uring & Direct I/O "
               "Benchmark\n";
  std::cout
      << "=================================================================\n";

  try {
    ad_benchmark::SimulatedVocabularyFile vocabFile;
    ad_benchmark::IoUringDirectBenchmarkRunner runner(vocabFile.path());

    // 1. Sequential Sweep Benchmark
    std::cout << "\n>>> Running Sequential 1GB Vocabulary Block Sweep <<<\n";
    std::vector<ad_benchmark::BenchmarkMetric> seqResults;
    seqResults.push_back(runner.runSyncPread(/*randomAccess=*/false));
    seqResults.push_back(runner.runSyncDirectPread(/*randomAccess=*/false));
    seqResults.push_back(runner.runIoUringUnpinned(/*randomAccess=*/false));
    seqResults.push_back(
        runner.runIoUringDirectUnpinned(/*randomAccess=*/false));
    seqResults.push_back(
        runner.runIoUringRegisteredFiles(/*randomAccess=*/false));
    seqResults.push_back(
        runner.runIoUringFullyRegistered(/*randomAccess=*/false));
    ad_benchmark::printResultsTable("Sequential Sweep", seqResults);

    // 2. Random Batch Access Benchmark
    std::cout
        << "\n>>> Running Random Access 1GB Vocabulary Block Lookup <<<\n";
    std::vector<ad_benchmark::BenchmarkMetric> randResults;
    randResults.push_back(runner.runSyncPread(/*randomAccess=*/true));
    randResults.push_back(runner.runSyncDirectPread(/*randomAccess=*/true));
    randResults.push_back(runner.runIoUringUnpinned(/*randomAccess=*/true));
    randResults.push_back(
        runner.runIoUringDirectUnpinned(/*randomAccess=*/true));
    randResults.push_back(
        runner.runIoUringRegisteredFiles(/*randomAccess=*/true));
    randResults.push_back(
        runner.runIoUringFullyRegistered(/*randomAccess=*/true));
    ad_benchmark::printResultsTable("Random Access Lookup", randResults);

  } catch (const std::exception& e) {
    std::cerr << "Benchmark failed with exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
#endif
