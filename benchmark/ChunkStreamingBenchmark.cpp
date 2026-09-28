// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "engine/AsyncChunkPipeline.h"
#include "util/Timer.h"
#include "util/jthread.h"

namespace ad_benchmark {

using namespace std::chrono_literals;
using qlever::export_pipeline::AsyncChunkPipeline;
using qlever::export_pipeline::PipelineStats;

// _____________________________________________________________________________
// Helper to simulate CPU-bound chunk generation:
// Generates a chunk of serialized RDF N-Triples/Turtle triples, including
// mock IRI/literal formatting, string escaping, and buffer population.
static std::string generateRdfChunk(size_t numTriples, size_t chunkIndex) {
  std::string chunk;
  // Pre-allocate approximately 120 bytes per triple
  chunk.reserve(numTriples * 120);

  for (size_t i = 0; i < numTriples; ++i) {
    const size_t entityId = chunkIndex * numTriples + i;
    chunk.append("<http://qlever.cs.uni-freiburg.de/entity/");
    chunk.append(std::to_string(entityId));
    chunk.append(
        "> <http://www.w3.org/2000/01/rdf-schema#label> \"Label for entity ");
    chunk.append(std::to_string(entityId));
    chunk.append(" with escaped chars \\\" \\n \\t and description\"@en .\n");
  }
  return chunk;
}

// _____________________________________________________________________________
// Helper to simulate network socket transmission delay (latency + bandwidth
// delay).
static void simulateNetworkTransmission(size_t chunkBytes,
                                        std::chrono::milliseconds latency) {
  if (latency > 0ms) {
    std::this_thread::sleep_for(latency);
  }
  // Simulate transmission on a 1 Gbps (125 MB/s) link: ~8 ns per byte
  if (chunkBytes > 0) {
    const auto wireNanos = std::chrono::nanoseconds(chunkBytes * 8);
    // Only sleep if delay is non-trivial (> 100 microseconds)
    if (wireNanos > 100us) {
      std::this_thread::sleep_for(wireNanos);
    }
  }
}

// _____________________________________________________________________________
// Benchmark Result Record.
// NOTE: initialized positionally below (no designated initializers), so this
// file also compiles with `CMAKE_CXX_STANDARD_MANUALLY_OVERRIDDEN=17`.
struct BenchmarkRunResult {
  std::string mode;
  size_t chunkSizeTriples;
  size_t latencyMs;
  size_t totalTriples;
  size_t totalBytes;
  double durationSeconds;
  double throughputMBPerSec;
  double throughputTriplesPerSec;
  size_t backpressureStalls{0};
  size_t consumerWaitStalls{0};
};

// _____________________________________________________________________________
// Benchmark class comparing Synchronous Lockstep Streaming vs Asynchronous
// Double-Buffered Streaming (`AsyncChunkPipeline`).
class ChunkStreamingBenchmark : public BenchmarkInterface {
 private:
  const std::vector<size_t> chunkSizesTriples_{10'000, 50'000, 100'000};
  const std::vector<size_t> latenciesMs_{0, 5, 20};
  const size_t totalTriples_{300'000};

 public:
  std::string name() const override {
    return "QLever Export Chunk Pipelining (Double-Buffering) Benchmark";
  }

  // ___________________________________________________________________________
  // Execute synchronous lockstep export for a given configuration.
  BenchmarkRunResult runSyncLockstep(size_t chunkSize, size_t latencyMs) const {
    const size_t numChunks = (totalTriples_ + chunkSize - 1) / chunkSize;
    const auto latency = std::chrono::milliseconds(latencyMs);

    ad_utility::timer::Timer timer(ad_utility::timer::Timer::Started);
    size_t totalBytes = 0;

    for (size_t c = 0; c < numChunks; ++c) {
      const size_t currentChunkSize =
          std::min(chunkSize, totalTriples_ - c * chunkSize);

      // Phase 1: CPU evaluates and formats chunk (lockstep: network sits idle)
      std::string chunk = generateRdfChunk(currentChunkSize, c);
      totalBytes += chunk.size();

      // Phase 2: Socket transmits chunk (lockstep: CPU sits idle)
      simulateNetworkTransmission(chunk.size(), latency);
    }

    timer.stop();
    const double duration = ad_utility::timer::Timer::toSeconds(timer.value());
    const double mb = static_cast<double>(totalBytes) / (1024.0 * 1024.0);

    return BenchmarkRunResult{
        "Sync Lockstep",
        chunkSize,
        latencyMs,
        totalTriples_,
        totalBytes,
        duration,
        duration > 0 ? (mb / duration) : 0.0,
        duration > 0 ? (static_cast<double>(totalTriples_) / duration) : 0.0,
    };
  }

  // ___________________________________________________________________________
  // Execute asynchronous double-buffered export using `AsyncChunkPipeline`.
  BenchmarkRunResult runAsyncDoubleBuffered(size_t chunkSize,
                                            size_t latencyMs) const {
    const size_t numChunks = (totalTriples_ + chunkSize - 1) / chunkSize;
    const auto latency = std::chrono::milliseconds(latencyMs);

    auto pipeline =
        std::make_shared<AsyncChunkPipeline<std::string>>(/*capacity=*/2);

    ad_utility::timer::Timer timer(ad_utility::timer::Timer::Started);

    // Spawn background worker to generate chunks concurrently into Slot 2.
    ad_utility::JThread producerThread(
        [pipeline, numChunks, chunkSize, totalTriples = totalTriples_]() {
          try {
            for (size_t c = 0; c < numChunks; ++c) {
              if (pipeline->isCancelled()) {
                break;
              }
              const size_t currentChunkSize =
                  std::min(chunkSize, totalTriples - c * chunkSize);
              std::string chunk = generateRdfChunk(currentChunkSize, c);
              if (!pipeline->push(std::move(chunk))) {
                break;
              }
            }
            pipeline->finish();
          } catch (...) {
            pipeline->setException(std::current_exception());
          }
        });

    // If the consumer loop throws (e.g. `pop()` rethrows a producer
    // exception), cancel the pipeline so the producer stops blocking in
    // `push()`; the `JThread` destructor then joins it during unwinding.
    absl::Cleanup cancelOnError{[&pipeline, &producerThread] {
      if (producerThread.joinable()) {
        pipeline->cancel();
      }
    }};

    // Consumer loop: transmits chunks over simulated network socket.
    size_t totalBytes = 0;
    while (auto chunkOpt = pipeline->pop()) {
      std::string chunk = std::move(*chunkOpt);
      totalBytes += chunk.size();
      // Socket transmits chunk while worker concurrently prepares next chunk
      simulateNetworkTransmission(chunk.size(), latency);
    }
    producerThread.join();

    timer.stop();
    const double duration = ad_utility::timer::Timer::toSeconds(timer.value());
    const double mb = static_cast<double>(totalBytes) / (1024.0 * 1024.0);
    const PipelineStats stats = pipeline->stats();

    return BenchmarkRunResult{
        "Async Double-Buffered",
        chunkSize,
        latencyMs,
        totalTriples_,
        totalBytes,
        duration,
        duration > 0 ? (mb / duration) : 0.0,
        duration > 0 ? (static_cast<double>(totalTriples_) / duration) : 0.0,
        stats.backpressureStalls,
        stats.consumerWaitStalls,
    };
  }

  // ___________________________________________________________________________
  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    std::cout << "\n==========================================================="
                 "=============================================\n"
              << " QLever SPARQL Export Streaming Benchmark: Lockstep vs "
                 "Asynchronous Double-Buffering\n"
              << " Total Triples: " << totalTriples_
              << " | Buffer Capacity: 2 Chunks\n"
              << "============================================================="
                 "===========================================\n";

    std::cout << std::left << std::setw(24) << "Mode" << std::setw(14)
              << "Chunk Size" << std::setw(14) << "Latency" << std::setw(12)
              << "Time (s)" << std::setw(16) << "Throughput(MB/s)"
              << std::setw(18) << "Triples/sec" << std::setw(10) << "Speedup"
              << std::setw(10) << "Stalls"
              << "\n"
              << std::string(104, '-') << "\n";

    for (const size_t chunkSize : chunkSizesTriples_) {
      for (const size_t latencyMs : latenciesMs_) {
        const std::string testDesc = "Chunk " + std::to_string(chunkSize) +
                                     " triples, Latency " +
                                     std::to_string(latencyMs) + "ms";

        // Measure Sync
        BenchmarkRunResult syncRes{};
        results.addMeasurement(
            "Sync: " + testDesc, [this, chunkSize, latencyMs, &syncRes]() {
              syncRes = runSyncLockstep(chunkSize, latencyMs);
            });

        // Measure Async Double-Buffered
        BenchmarkRunResult asyncRes{};
        results.addMeasurement("Async Double-Buffered: " + testDesc,
                               [this, chunkSize, latencyMs, &asyncRes]() {
                                 asyncRes = runAsyncDoubleBuffered(chunkSize,
                                                                   latencyMs);
                               });

        const double speedup =
            asyncRes.durationSeconds > 0
                ? (syncRes.durationSeconds / asyncRes.durationSeconds)
                : 1.0;

        // Print formatted summary row
        std::cout << std::left << std::setw(24) << syncRes.mode << std::setw(14)
                  << (std::to_string(chunkSize) + " trp") << std::setw(14)
                  << (std::to_string(latencyMs) + " ms") << std::fixed
                  << std::setprecision(4) << std::setw(12)
                  << syncRes.durationSeconds << std::fixed
                  << std::setprecision(2) << std::setw(16)
                  << syncRes.throughputMBPerSec << std::fixed
                  << std::setprecision(0) << std::setw(18)
                  << syncRes.throughputTriplesPerSec << std::setw(10) << "1.00x"
                  << std::setw(10) << "-"
                  << "\n";

        std::cout << std::left << std::setw(24) << asyncRes.mode
                  << std::setw(14) << (std::to_string(chunkSize) + " trp")
                  << std::setw(14) << (std::to_string(latencyMs) + " ms")
                  << std::fixed << std::setprecision(4) << std::setw(12)
                  << asyncRes.durationSeconds << std::fixed
                  << std::setprecision(2) << std::setw(16)
                  << asyncRes.throughputMBPerSec << std::fixed
                  << std::setprecision(0) << std::setw(18)
                  << asyncRes.throughputTriplesPerSec << std::fixed
                  << std::setprecision(2)
                  << (std::to_string(speedup).substr(0, 4) + "x   ")
                  << std::setw(10) << asyncRes.backpressureStalls << "\n"
                  << std::string(104, '.') << "\n";
      }
    }

    std::cout << "============================================================="
                 "===========================================\n\n";

    return results;
  }
};

// Register the benchmark with the QLever benchmark infrastructure.
AD_REGISTER_BENCHMARK(ChunkStreamingBenchmark);

}  // namespace ad_benchmark
