// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "util/FastIntToString.h"

namespace ad_benchmark {
namespace {

using namespace ad_utility;

constexpr size_t NUM_INTEGERS = 1'000'000;
// Stack buffers for the formatting benchmarks. 32 bytes hold any `int64_t`
// (at most 20 chars + sign, plus slack); 64 bytes hold the 32-byte Wikidata
// entity prefix plus any `uint64_t` (at most 20 digits, plus slack).
constexpr size_t INT_BUFFER_SIZE = 32;
constexpr size_t QID_BUFFER_SIZE = 64;

// Benchmark suite comparing std::to_string, std::to_chars, and
// formatIntBranchless
class FastNumberFormatterBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "Fast Number and RDF Entity Formatter Micro-Benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    // Generate test data: 1,000,000 integers
    std::vector<int64_t> sequentialInts;
    sequentialInts.reserve(NUM_INTEGERS);
    for (size_t i = 1; i <= NUM_INTEGERS; ++i) {
      sequentialInts.push_back(static_cast<int64_t>(i));
    }

    std::vector<int64_t> randomInts;
    randomInts.reserve(NUM_INTEGERS);
    std::mt19937_64 rng(1337);
    // Draw signed values directly: narrowing an out-of-range `uint64_t` to
    // `int64_t` is implementation-defined before C++20.
    std::uniform_int_distribution<int64_t> intDist(
        std::numeric_limits<int64_t>::min(),
        std::numeric_limits<int64_t>::max());
    for (size_t i = 0; i < NUM_INTEGERS; ++i) {
      randomInts.push_back(intDist(rng));
    }

    std::vector<uint64_t> qids;
    qids.reserve(NUM_INTEGERS);
    std::uniform_int_distribution<uint64_t> qidDist(1, 120'000'000);
    for (size_t i = 0; i < NUM_INTEGERS; ++i) {
      qids.push_back(qidDist(rng));
    }

    // Benchmark Group 1: Sequential 64-bit Integer Formatting
    {
      auto& group =
          results.addGroup("Sequential Integer Formatting (1..1,000,000)");

      // 1. std::to_string (baseline: dynamic allocation + standard division
      // loop). Every block below times inside its callback: `addMeasurement`
      // runs (and times) the callback itself, so an outer timer would include
      // framework overhead in the reported metadata.
      {
        size_t totalBytes = 0;
        std::chrono::nanoseconds::rep elapsedNs = 0;
        auto& m = group.addMeasurement("std::to_string", [&]() {
          auto start = std::chrono::high_resolution_clock::now();
          size_t bytes = 0;
          for (int64_t val : sequentialInts) {
            std::string s = std::to_string(val);
            bytes += s.size();
          }
          totalBytes = bytes;
          elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::high_resolution_clock::now() - start)
                          .count();
          return bytes;
        });
        double throughputMPerSec = (static_cast<double>(NUM_INTEGERS) /
                                    (static_cast<double>(elapsedNs) / 1e9)) /
                                   1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 2. std::to_chars (stack buffer, standard library fast path)
      {
        size_t totalBytes = 0;
        std::chrono::nanoseconds::rep elapsedNs = 0;
        char buffer[INT_BUFFER_SIZE];
        auto& m = group.addMeasurement("std::to_chars", [&]() {
          auto start = std::chrono::high_resolution_clock::now();
          size_t bytes = 0;
          for (int64_t val : sequentialInts) {
            auto [ptr, ec] =
                std::to_chars(buffer, buffer + sizeof(buffer), val);
            AD_CORRECTNESS_CHECK(ec == std::errc{});
            bytes += static_cast<size_t>(ptr - buffer);
          }
          totalBytes = bytes;
          elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::high_resolution_clock::now() - start)
                          .count();
          return bytes;
        });
        double throughputMPerSec = (static_cast<double>(NUM_INTEGERS) /
                                    (static_cast<double>(elapsedNs) / 1e9)) /
                                   1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 3. formatIntBranchless (Fast SIMD / lookup table zero-allocation)
      {
        size_t totalBytes = 0;
        std::chrono::nanoseconds::rep elapsedNs = 0;
        char buffer[INT_BUFFER_SIZE];
        auto& m = group.addMeasurement("formatIntBranchless (SIMD/LUT)", [&]() {
          auto start = std::chrono::high_resolution_clock::now();
          size_t bytes = 0;
          for (int64_t val : sequentialInts) {
            char* end = formatIntBranchless(val, buffer);
            bytes += static_cast<size_t>(end - buffer);
          }
          totalBytes = bytes;
          elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::high_resolution_clock::now() - start)
                          .count();
          return bytes;
        });
        double throughputMPerSec = (static_cast<double>(NUM_INTEGERS) /
                                    (static_cast<double>(elapsedNs) / 1e9)) /
                                   1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }
    }

    // Benchmark Group 2: Full 64-bit Random Integer Formatting
    {
      auto& group =
          results.addGroup("Random 64-bit Integer Formatting (1,000,000 ints)");

      // 1. std::to_string
      {
        size_t totalBytes = 0;
        std::chrono::nanoseconds::rep elapsedNs = 0;
        auto& m = group.addMeasurement("std::to_string (random 64-bit)", [&]() {
          auto start = std::chrono::high_resolution_clock::now();
          size_t bytes = 0;
          for (int64_t val : randomInts) {
            std::string s = std::to_string(val);
            bytes += s.size();
          }
          totalBytes = bytes;
          elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::high_resolution_clock::now() - start)
                          .count();
          return bytes;
        });
        double throughputMPerSec = (static_cast<double>(NUM_INTEGERS) /
                                    (static_cast<double>(elapsedNs) / 1e9)) /
                                   1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 2. std::to_chars
      {
        size_t totalBytes = 0;
        std::chrono::nanoseconds::rep elapsedNs = 0;
        char buffer[INT_BUFFER_SIZE];
        auto& m = group.addMeasurement("std::to_chars (random 64-bit)", [&]() {
          auto start = std::chrono::high_resolution_clock::now();
          size_t bytes = 0;
          for (int64_t val : randomInts) {
            auto [ptr, ec] =
                std::to_chars(buffer, buffer + sizeof(buffer), val);
            AD_CORRECTNESS_CHECK(ec == std::errc{});
            bytes += static_cast<size_t>(ptr - buffer);
          }
          totalBytes = bytes;
          elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::high_resolution_clock::now() - start)
                          .count();
          return bytes;
        });
        double throughputMPerSec = (static_cast<double>(NUM_INTEGERS) /
                                    (static_cast<double>(elapsedNs) / 1e9)) /
                                   1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 3. formatIntBranchless
      {
        size_t totalBytes = 0;
        std::chrono::nanoseconds::rep elapsedNs = 0;
        char buffer[INT_BUFFER_SIZE];
        auto& m =
            group.addMeasurement("formatIntBranchless (random 64-bit)", [&]() {
              auto start = std::chrono::high_resolution_clock::now();
              size_t bytes = 0;
              for (int64_t val : randomInts) {
                char* end = formatIntBranchless(val, buffer);
                bytes += static_cast<size_t>(end - buffer);
              }
              totalBytes = bytes;
              elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::high_resolution_clock::now() - start)
                              .count();
              return bytes;
            });
        double throughputMPerSec = (static_cast<double>(NUM_INTEGERS) /
                                    (static_cast<double>(elapsedNs) / 1e9)) /
                                   1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }
    }

    // Benchmark Group 3: RDF Wikidata QID Formatting
    // ("http://www.wikidata.org/entity/Q" + id)
    {
      auto& group = results.addGroup(
          "RDF Wikidata Entity QID Formatting (1,000,000 QIDs)");

      // 1. std::string concatenation
      {
        size_t totalBytes = 0;
        std::chrono::nanoseconds::rep elapsedNs = 0;
        auto& m = group.addMeasurement(
            "std::string concat (prefix + to_string)", [&]() {
              auto start = std::chrono::high_resolution_clock::now();
              size_t bytes = 0;
              for (uint64_t id : qids) {
                std::string s =
                    std::string{WIKIDATA_ENTITY_PREFIX} + std::to_string(id);
                bytes += s.size();
              }
              totalBytes = bytes;
              elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::high_resolution_clock::now() - start)
                              .count();
              return bytes;
            });
        double throughputMPerSec = (static_cast<double>(NUM_INTEGERS) /
                                    (static_cast<double>(elapsedNs) / 1e9)) /
                                   1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 2. std::to_chars with manual prefix copy
      {
        size_t totalBytes = 0;
        std::chrono::nanoseconds::rep elapsedNs = 0;
        char buffer[QID_BUFFER_SIZE];
        auto& m = group.addMeasurement("memcpy prefix + std::to_chars", [&]() {
          auto start = std::chrono::high_resolution_clock::now();
          size_t bytes = 0;
          for (uint64_t id : qids) {
            std::memcpy(buffer, WIKIDATA_ENTITY_PREFIX.data(),
                        WIKIDATA_ENTITY_PREFIX.size());
            auto [ptr, ec] =
                std::to_chars(buffer + WIKIDATA_ENTITY_PREFIX.size(),
                              buffer + sizeof(buffer), id);
            AD_CORRECTNESS_CHECK(ec == std::errc{});
            bytes += static_cast<size_t>(ptr - buffer);
          }
          totalBytes = bytes;
          elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::high_resolution_clock::now() - start)
                          .count();
          return bytes;
        });
        double throughputMPerSec = (static_cast<double>(NUM_INTEGERS) /
                                    (static_cast<double>(elapsedNs) / 1e9)) /
                                   1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 3. formatQid (single-pass SIMD/branchless formatting)
      {
        size_t totalBytes = 0;
        std::chrono::nanoseconds::rep elapsedNs = 0;
        char buffer[QID_BUFFER_SIZE];
        auto& m =
            group.addMeasurement("formatQid (Single-pass SIMD/LUT)", [&]() {
              auto start = std::chrono::high_resolution_clock::now();
              size_t bytes = 0;
              for (uint64_t id : qids) {
                char* end = formatQid(id, buffer);
                bytes += static_cast<size_t>(end - buffer);
              }
              totalBytes = bytes;
              elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::high_resolution_clock::now() - start)
                              .count();
              return bytes;
            });
        double throughputMPerSec = (static_cast<double>(NUM_INTEGERS) /
                                    (static_cast<double>(elapsedNs) / 1e9)) /
                                   1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(FastNumberFormatterBenchmark);

}  // namespace
}  // namespace ad_benchmark
