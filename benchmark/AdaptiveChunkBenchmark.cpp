// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "engine/AdaptiveChunkSizer.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"

namespace ad_benchmark {

using namespace std::chrono_literals;
using qlever::AdaptiveChunkBuffer;
using qlever::AdaptiveChunkConfig;
using qlever::AdaptiveChunkSizer;

// _____________________________________________________________________________
// Helper to generate a realistic RDF N-Triples row into a target buffer.
// Appends `<http://qlever.cs.uni-freiburg.de/entity/ID>
// <http://www.w3.org/2000/01/rdf-schema#label> "Label for entity ID with
// description text"@en .\n`
static inline void formatTripleToBuffer(size_t entityId, std::string& buffer) {
  buffer.append("<http://qlever.cs.uni-freiburg.de/entity/");
  buffer.append(std::to_string(entityId));
  buffer.append(
      "> <http://www.w3.org/2000/01/rdf-schema#label> \"Label for entity ");
  buffer.append(std::to_string(entityId));
  buffer.append(
      " with description text and escaped chars \\\" \\n \\t\"@en .\n");
}

// _____________________________________________________________________________
// Benchmark Result Record
struct ChunkBenchmarkResult {
  std::string mode;
  size_t totalTriples{0};
  size_t totalBytes{0};
  size_t chunksEmitted{0};
  double ttfbMs{0.0};
  double durationSeconds{0.0};
  double throughputMBPerSec{0.0};
  double throughputTriplesPerSec{0.0};
  double avgChunkSizeKb{0.0};
};

// _____________________________________________________________________________
// Benchmark Class: Fixed 100K Chunk Size vs AdaptiveChunkSizer (64 KB -> 4 MB).
class AdaptiveChunkBenchmark : public BenchmarkInterface {
 private:
  const size_t totalTriples_{1'000'000};

 public:
  std::string name() const override {
    return "QLever Adaptive Chunk Sizing Export Benchmark (1,000,000 Triples)";
  }

  // ___________________________________________________________________________
  // Benchmark 1: Fixed 100,000 Triples per Chunk.
  ChunkBenchmarkResult runFixed100kTriples() const {
    const size_t chunkSize = 100'000;
    const size_t numChunks = (totalTriples_ + chunkSize - 1) / chunkSize;

    ad_utility::timer::Timer totalTimer(ad_utility::timer::Timer::Started);
    ad_utility::timer::Timer ttfbTimer(ad_utility::timer::Timer::Started);

    double ttfbMs = 0.0;
    size_t totalBytes = 0;
    size_t chunksEmitted = 0;

    std::string currentChunk;
    currentChunk.reserve(chunkSize * 130);

    for (size_t c = 0; c < numChunks; ++c) {
      const size_t currentBatchSize =
          std::min(chunkSize, totalTriples_ - c * chunkSize);

      currentChunk.clear();
      for (size_t i = 0; i < currentBatchSize; ++i) {
        formatTripleToBuffer(c * chunkSize + i, currentChunk);
      }

      totalBytes += currentChunk.size();
      ++chunksEmitted;

      // First chunk completed: record Time-To-First-Byte
      if (chunksEmitted == 1) {
        ttfbTimer.stop();
        ttfbMs =
            ad_utility::timer::Timer::toSeconds(ttfbTimer.value()) * 1000.0;
      }
    }

    totalTimer.stop();
    const double duration =
        ad_utility::timer::Timer::toSeconds(totalTimer.value());
    const double mb = static_cast<double>(totalBytes) / (1024.0 * 1024.0);

    return ChunkBenchmarkResult{
        .mode = "Fixed 100k Triples",
        .totalTriples = totalTriples_,
        .totalBytes = totalBytes,
        .chunksEmitted = chunksEmitted,
        .ttfbMs = ttfbMs,
        .durationSeconds = duration,
        .throughputMBPerSec = duration > 0 ? (mb / duration) : 0.0,
        .throughputTriplesPerSec =
            duration > 0 ? (static_cast<double>(totalTriples_) / duration)
                         : 0.0,
        .avgChunkSizeKb = chunksEmitted > 0 ? (static_cast<double>(totalBytes) /
                                               (chunksEmitted * 1024.0))
                                            : 0.0,
    };
  }

  // ___________________________________________________________________________
  // Benchmark 2: AdaptiveChunkSizer (64 KB -> 4 MB exponential ramp-up).
  ChunkBenchmarkResult runAdaptiveChunkSizer() const {
    AdaptiveChunkSizer sizer(64 * 1024, 4 * 1024 * 1024, 2.0, 120.0);

    ad_utility::timer::Timer totalTimer(ad_utility::timer::Timer::Started);
    ad_utility::timer::Timer ttfbTimer(ad_utility::timer::Timer::Started);

    double ttfbMs = 0.0;
    size_t totalBytes = 0;
    size_t chunksEmitted = 0;
    size_t triplesProcessed = 0;

    std::string currentChunk;
    currentChunk.reserve(sizer.currentChunkBytes());

    while (triplesProcessed < totalTriples_) {
      const size_t remaining = totalTriples_ - triplesProcessed;
      const size_t batchRows = sizer.targetRowCount(remaining);

      currentChunk.clear();
      for (size_t i = 0; i < batchRows; ++i) {
        formatTripleToBuffer(triplesProcessed + i, currentChunk);
      }

      triplesProcessed += batchRows;
      totalBytes += currentChunk.size();
      ++chunksEmitted;

      if (chunksEmitted == 1) {
        ttfbTimer.stop();
        ttfbMs =
            ad_utility::timer::Timer::toSeconds(ttfbTimer.value()) * 1000.0;
      }

      sizer.recordChunk(currentChunk.size(), batchRows);
      currentChunk.reserve(sizer.currentChunkBytes());
    }

    totalTimer.stop();
    const double duration =
        ad_utility::timer::Timer::toSeconds(totalTimer.value());
    const double mb = static_cast<double>(totalBytes) / (1024.0 * 1024.0);

    return ChunkBenchmarkResult{
        .mode = "Adaptive (64KB -> 4MB)",
        .totalTriples = totalTriples_,
        .totalBytes = totalBytes,
        .chunksEmitted = chunksEmitted,
        .ttfbMs = ttfbMs,
        .durationSeconds = duration,
        .throughputMBPerSec = duration > 0 ? (mb / duration) : 0.0,
        .throughputTriplesPerSec =
            duration > 0 ? (static_cast<double>(totalTriples_) / duration)
                         : 0.0,
        .avgChunkSizeKb = chunksEmitted > 0 ? (static_cast<double>(totalBytes) /
                                               (chunksEmitted * 1024.0))
                                            : 0.0,
    };
  }

  // ___________________________________________________________________________
  // Benchmark 3: AdaptiveChunkBuffer integrated self-managing stream.
  ChunkBenchmarkResult runAdaptiveChunkBuffer() const {
    AdaptiveChunkBuffer buffer(AdaptiveChunkConfig{
        .initialChunkBytes_ = 64 * 1024,
        .maxChunkBytes_ = 4 * 1024 * 1024,
        .growthFactor_ = 2.0,
        .initialEstimatedRowBytes_ = 120.0,
    });

    ad_utility::timer::Timer totalTimer(ad_utility::timer::Timer::Started);
    ad_utility::timer::Timer ttfbTimer(ad_utility::timer::Timer::Started);

    double ttfbMs = 0.0;
    size_t totalBytes = 0;
    size_t chunksEmitted = 0;

    std::string tempRow;
    tempRow.reserve(256);

    for (size_t i = 0; i < totalTriples_; ++i) {
      tempRow.clear();
      formatTripleToBuffer(i, tempRow);
      buffer.write(tempRow);
      buffer.recordRow();

      if (buffer.isReadyToFlush()) {
        std::string chunk = buffer.flush();
        totalBytes += chunk.size();
        ++chunksEmitted;

        if (chunksEmitted == 1) {
          ttfbTimer.stop();
          ttfbMs =
              ad_utility::timer::Timer::toSeconds(ttfbTimer.value()) * 1000.0;
        }
      }
    }

    if (buffer.bytesBuffered() > 0) {
      std::string finalChunk = buffer.flush();
      totalBytes += finalChunk.size();
      ++chunksEmitted;
      if (chunksEmitted == 1) {
        ttfbTimer.stop();
        ttfbMs =
            ad_utility::timer::Timer::toSeconds(ttfbTimer.value()) * 1000.0;
      }
    }

    totalTimer.stop();
    const double duration =
        ad_utility::timer::Timer::toSeconds(totalTimer.value());
    const double mb = static_cast<double>(totalBytes) / (1024.0 * 1024.0);

    return ChunkBenchmarkResult{
        .mode = "Adaptive Buffer Stream",
        .totalTriples = totalTriples_,
        .totalBytes = totalBytes,
        .chunksEmitted = chunksEmitted,
        .ttfbMs = ttfbMs,
        .durationSeconds = duration,
        .throughputMBPerSec = duration > 0 ? (mb / duration) : 0.0,
        .throughputTriplesPerSec =
            duration > 0 ? (static_cast<double>(totalTriples_) / duration)
                         : 0.0,
        .avgChunkSizeKb = chunksEmitted > 0 ? (static_cast<double>(totalBytes) /
                                               (chunksEmitted * 1024.0))
                                            : 0.0,
    };
  }

  // ___________________________________________________________________________
  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    std::cout << "\n==========================================================="
                 "=============================================\n"
              << " QLever Export Benchmark: Fixed 100k Chunking vs Adaptive "
                 "Chunk Sizing (64 KB -> 4 MB)\n"
              << " Workload: 1,000,000 Formatted RDF Triples\n"
              << "============================================================="
                 "===========================================\n";

    std::cout << std::left << std::setw(26) << "Mode" << std::setw(14)
              << "TTFB (ms)" << std::setw(14) << "Total (s)" << std::setw(18)
              << "Throughput(MB/s)" << std::setw(18) << "Triples/sec"
              << std::setw(10) << "Chunks" << std::setw(14) << "Avg Chunk(KB)"
              << "\n"
              << std::string(104, '-') << "\n";

    // Run Fixed 100k
    ChunkBenchmarkResult fixedRes{};
    results.addMeasurement("Fixed 100k Triples Chunking", [this, &fixedRes]() {
      fixedRes = runFixed100kTriples();
    });

    // Run Adaptive Sizer
    ChunkBenchmarkResult adaptiveRes{};
    results.addMeasurement(
        "Adaptive Chunk Sizer (64KB -> 4MB)",
        [this, &adaptiveRes]() { adaptiveRes = runAdaptiveChunkSizer(); });

    // Run Adaptive Buffer
    ChunkBenchmarkResult bufferRes{};
    results.addMeasurement(
        "Adaptive Chunk Buffer Stream",
        [this, &bufferRes]() { bufferRes = runAdaptiveChunkBuffer(); });

    auto printRow = [](const ChunkBenchmarkResult& res) {
      std::cout << std::left << std::setw(26) << res.mode << std::fixed
                << std::setprecision(3) << std::setw(14) << res.ttfbMs
                << std::fixed << std::setprecision(4) << std::setw(14)
                << res.durationSeconds << std::fixed << std::setprecision(2)
                << std::setw(18) << res.throughputMBPerSec << std::fixed
                << std::setprecision(0) << std::setw(18)
                << res.throughputTriplesPerSec << std::setw(10)
                << res.chunksEmitted << std::fixed << std::setprecision(1)
                << std::setw(14) << res.avgChunkSizeKb << "\n";
    };

    printRow(fixedRes);
    printRow(adaptiveRes);
    printRow(bufferRes);

    const double ttfbImprovement =
        adaptiveRes.ttfbMs > 0 ? (fixedRes.ttfbMs / adaptiveRes.ttfbMs) : 1.0;

    std::cout << std::string(104, '-') << "\n";
    std::cout << ">> TTFB Latency Improvement: " << std::fixed
              << std::setprecision(1) << ttfbImprovement
              << "x faster time to first byte with Adaptive Chunk Sizing!\n";
    std::cout << "============================================================="
                 "===========================================\n\n";

    return results;
  }
};

AD_REGISTER_BENCHMARK(AdaptiveChunkBenchmark);

}  // namespace ad_benchmark
