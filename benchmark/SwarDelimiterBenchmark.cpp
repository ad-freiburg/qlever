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
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
#include <x86intrin.h>
#endif

#include "../benchmark/infrastructure/Benchmark.h"
#include "util/SwarDelimiterPacker.h"

namespace ad_benchmark {
namespace {

using namespace ad_utility;

constexpr size_t NUM_ROWS = 5'000'000;

// Read hardware CPU timestamp counter for precise cycles per row measurement
inline uint64_t readCpuCycles() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
  return __rdtsc();
#else
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
#endif
}

// _____________________________________________________________________________
// Benchmark suite comparing scalar byte-by-byte writes vs SWAR 64-bit delimiter
// packing
class SwarDelimiterBenchmark : public BenchmarkInterface {
 private:
  // Pre-allocated recycling streaming buffer (64 MB) to prevent cache thrashing
  std::vector<char> outputBuffer_;

 public:
  SwarDelimiterBenchmark() : outputBuffer_(64 * 1024 * 1024) {}

  std::string name() const final {
    return "SWAR Delimiter Packing vs Scalar Serialization Micro-Benchmark "
           "(5,000,000 rows)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    // Synthetic column payload data
    const std::string sSub = "http://www.wikidata.org/entity/Q42";
    const std::string sPred = "http://www.wikidata.org/prop/direct/P31";
    const std::string sObj = "http://www.wikidata.org/entity/Q515";

    const std::string_view sub = sSub;
    const std::string_view pred = sPred;
    const std::string_view obj = sObj;

    const std::string sCsv1 = "Douglas Adams";
    const std::string sCsv2 = "English author and screenwriter";
    const std::string sCsv3 = "Cambridge";

    const std::string_view csv1 = sCsv1;
    const std::string_view csv2 = sCsv2;
    const std::string_view csv3 = sCsv3;

    char* const bufStart = outputBuffer_.data();
    char* const bufLimit = outputBuffer_.data() + outputBuffer_.size() - 4096;

    // =========================================================================
    // Group 1: N-Triples 3-Column IRI Serialization (<s> <p> <o> .\n)
    // =========================================================================
    {
      auto& group = results.addGroup(
          "1. N-Triples IRI Row Serialization (5,000,000 rows, <s> <p> <o> "
          ".\\n)");

      // 1.1 Scalar byte-by-byte writes (11 scalar stores per row)
      {
        size_t totalBytesWritten = 0;
        uint64_t startCycles = readCpuCycles();
        auto start = std::chrono::high_resolution_clock::now();

        auto& m =
            group.addMeasurement("Scalar 1-byte stores (11 stores/row)", [&]() {
              char* out = bufStart;
              for (size_t i = 0; i < NUM_ROWS; ++i) {
                if (out >= bufLimit) {
                  totalBytesWritten += static_cast<size_t>(out - bufStart);
                  out = bufStart;
                }
                *out++ = '<';
                std::memcpy(out, sub.data(), sub.size());
                out += sub.size();
                *out++ = '>';
                *out++ = ' ';
                *out++ = '<';
                std::memcpy(out, pred.data(), pred.size());
                out += pred.size();
                *out++ = '>';
                *out++ = ' ';
                *out++ = '<';
                std::memcpy(out, obj.data(), obj.size());
                out += obj.size();
                *out++ = '>';
                *out++ = ' ';
                *out++ = '.';
                *out++ = '\n';
              }
              totalBytesWritten += static_cast<size_t>(out - bufStart);
              return totalBytesWritten;
            });

        auto end = std::chrono::high_resolution_clock::now();
        uint64_t endCycles = readCpuCycles();
        uint64_t elapsedNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
        uint64_t totalCycles =
            (endCycles >= startCycles) ? (endCycles - startCycles) : 0;

        double throughputMRowsPerSec =
            (static_cast<double>(NUM_ROWS) /
             (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double cyclesPerRow =
            static_cast<double>(totalCycles) / static_cast<double>(NUM_ROWS);
        double latencyNsPerRow =
            static_cast<double>(elapsedNs) / static_cast<double>(NUM_ROWS);
        double totalMbWritten =
            static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0);

        m.metadata().addKeyValuePair("rows-processed", NUM_ROWS);
        m.metadata().addKeyValuePair("throughput-m-rows-per-sec",
                                     throughputMRowsPerSec);
        m.metadata().addKeyValuePair("cycles-per-row", cyclesPerRow);
        m.metadata().addKeyValuePair("latency-ns-per-row", latencyNsPerRow);
        m.metadata().addKeyValuePair("total-mb-written", totalMbWritten);
      }

      // 1.2 SwarDelimiterPacker 64-bit branchless stores (4 stores per row)
      {
        size_t totalBytesWritten = 0;
        uint64_t startCycles = readCpuCycles();
        auto start = std::chrono::high_resolution_clock::now();

        auto& m = group.addMeasurement(
            "SwarDelimiterPacker::writeDelim64 (4 stores/row)", [&]() {
              char* out = bufStart;
              for (size_t i = 0; i < NUM_ROWS; ++i) {
                if (out >= bufLimit) {
                  totalBytesWritten += static_cast<size_t>(out - bufStart);
                  out = bufStart;
                }
                *out++ = '<';
                std::memcpy(out, sub.data(), sub.size());
                out += sub.size();
                out = SwarDelimiterPacker::writeDelim64<3>(
                    out, SwarDelimiterPacker::TRIPLE_S_TO_P_IRI.pattern());
                std::memcpy(out, pred.data(), pred.size());
                out += pred.size();
                out = SwarDelimiterPacker::writeDelim64<3>(
                    out, SwarDelimiterPacker::TRIPLE_P_TO_O_IRI.pattern());
                std::memcpy(out, obj.data(), obj.size());
                out += obj.size();
                out = SwarDelimiterPacker::writeDelim64<4>(
                    out, SwarDelimiterPacker::TRIPLE_O_IRI_END.pattern());
              }
              totalBytesWritten += static_cast<size_t>(out - bufStart);
              return totalBytesWritten;
            });

        auto end = std::chrono::high_resolution_clock::now();
        uint64_t endCycles = readCpuCycles();
        uint64_t elapsedNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
        uint64_t totalCycles =
            (endCycles >= startCycles) ? (endCycles - startCycles) : 0;

        double throughputMRowsPerSec =
            (static_cast<double>(NUM_ROWS) /
             (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double cyclesPerRow =
            static_cast<double>(totalCycles) / static_cast<double>(NUM_ROWS);
        double latencyNsPerRow =
            static_cast<double>(elapsedNs) / static_cast<double>(NUM_ROWS);
        double totalMbWritten =
            static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0);

        m.metadata().addKeyValuePair("rows-processed", NUM_ROWS);
        m.metadata().addKeyValuePair("throughput-m-rows-per-sec",
                                     throughputMRowsPerSec);
        m.metadata().addKeyValuePair("cycles-per-row", cyclesPerRow);
        m.metadata().addKeyValuePair("latency-ns-per-row", latencyNsPerRow);
        m.metadata().addKeyValuePair("total-mb-written", totalMbWritten);
      }
    }

    // =========================================================================
    // Group 2: CSV 3-Column Quoted Serialization ("col1","col2","col3"\n)
    // =========================================================================
    {
      auto& group = results.addGroup(
          "2. CSV 3-Column Quoted Row Serialization (5,000,000 rows)");

      // 2.1 Scalar CSV
      {
        size_t totalBytesWritten = 0;
        uint64_t startCycles = readCpuCycles();
        auto start = std::chrono::high_resolution_clock::now();

        auto& m =
            group.addMeasurement("Scalar 1-byte stores (9 stores/row)", [&]() {
              char* out = bufStart;
              for (size_t i = 0; i < NUM_ROWS; ++i) {
                if (out >= bufLimit) {
                  totalBytesWritten += static_cast<size_t>(out - bufStart);
                  out = bufStart;
                }
                *out++ = '"';
                std::memcpy(out, csv1.data(), csv1.size());
                out += csv1.size();
                *out++ = '"';
                *out++ = ',';
                *out++ = '"';
                std::memcpy(out, csv2.data(), csv2.size());
                out += csv2.size();
                *out++ = '"';
                *out++ = ',';
                *out++ = '"';
                std::memcpy(out, csv3.data(), csv3.size());
                out += csv3.size();
                *out++ = '"';
                *out++ = '\n';
              }
              totalBytesWritten += static_cast<size_t>(out - bufStart);
              return totalBytesWritten;
            });

        auto end = std::chrono::high_resolution_clock::now();
        uint64_t endCycles = readCpuCycles();
        uint64_t elapsedNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
        uint64_t totalCycles =
            (endCycles >= startCycles) ? (endCycles - startCycles) : 0;

        double throughputMRowsPerSec =
            (static_cast<double>(NUM_ROWS) /
             (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double cyclesPerRow =
            static_cast<double>(totalCycles) / static_cast<double>(NUM_ROWS);
        double latencyNsPerRow =
            static_cast<double>(elapsedNs) / static_cast<double>(NUM_ROWS);
        double totalMbWritten =
            static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0);

        m.metadata().addKeyValuePair("rows-processed", NUM_ROWS);
        m.metadata().addKeyValuePair("throughput-m-rows-per-sec",
                                     throughputMRowsPerSec);
        m.metadata().addKeyValuePair("cycles-per-row", cyclesPerRow);
        m.metadata().addKeyValuePair("latency-ns-per-row", latencyNsPerRow);
        m.metadata().addKeyValuePair("total-mb-written", totalMbWritten);
      }

      // 2.2 SwarDelimiterPacker CSV
      {
        size_t totalBytesWritten = 0;
        uint64_t startCycles = readCpuCycles();
        auto start = std::chrono::high_resolution_clock::now();

        auto& m = group.addMeasurement(
            "SwarDelimiterPacker::writeDelim64 (4 stores/row)", [&]() {
              char* out = bufStart;
              for (size_t i = 0; i < NUM_ROWS; ++i) {
                if (out >= bufLimit) {
                  totalBytesWritten += static_cast<size_t>(out - bufStart);
                  out = bufStart;
                }
                *out++ = '"';
                std::memcpy(out, csv1.data(), csv1.size());
                out += csv1.size();
                out = SwarDelimiterPacker::writeDelim64<3>(
                    out, SwarDelimiterPacker::CSV_QUOTE_COMMA_QUOTE);
                std::memcpy(out, csv2.data(), csv2.size());
                out += csv2.size();
                out = SwarDelimiterPacker::writeDelim64<3>(
                    out, SwarDelimiterPacker::CSV_QUOTE_COMMA_QUOTE);
                std::memcpy(out, csv3.data(), csv3.size());
                out += csv3.size();
                out = SwarDelimiterPacker::writeDelim64<2>(
                    out, SwarDelimiterPacker::CSV_QUOTE_NEWLINE);
              }
              totalBytesWritten += static_cast<size_t>(out - bufStart);
              return totalBytesWritten;
            });

        auto end = std::chrono::high_resolution_clock::now();
        uint64_t endCycles = readCpuCycles();
        uint64_t elapsedNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
        uint64_t totalCycles =
            (endCycles >= startCycles) ? (endCycles - startCycles) : 0;

        double throughputMRowsPerSec =
            (static_cast<double>(NUM_ROWS) /
             (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double cyclesPerRow =
            static_cast<double>(totalCycles) / static_cast<double>(NUM_ROWS);
        double latencyNsPerRow =
            static_cast<double>(elapsedNs) / static_cast<double>(NUM_ROWS);
        double totalMbWritten =
            static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0);

        m.metadata().addKeyValuePair("rows-processed", NUM_ROWS);
        m.metadata().addKeyValuePair("throughput-m-rows-per-sec",
                                     throughputMRowsPerSec);
        m.metadata().addKeyValuePair("cycles-per-row", cyclesPerRow);
        m.metadata().addKeyValuePair("latency-ns-per-row", latencyNsPerRow);
        m.metadata().addKeyValuePair("total-mb-written", totalMbWritten);
      }
    }

    // =========================================================================
    // Group 3: Pure Delimiter Micro-Benchmark (5,000,000 row delimiter
    // transitions)
    // =========================================================================
    {
      auto& group = results.addGroup(
          "3. Pure Delimiter Writing Micro-Benchmark (5,000,000 rows, zero "
          "payload copy)");

      // 3.1 Scalar pure delimiter writes
      {
        size_t totalBytesWritten = 0;
        uint64_t startCycles = readCpuCycles();
        auto start = std::chrono::high_resolution_clock::now();

        auto& m = group.addMeasurement(
            "Scalar pure delimiter writing (10 scalar stores/row)", [&]() {
              char* out = bufStart;
              for (size_t i = 0; i < NUM_ROWS; ++i) {
                if (out >= bufLimit) {
                  totalBytesWritten += static_cast<size_t>(out - bufStart);
                  out = bufStart;
                }
                *out++ = '>';
                *out++ = ' ';
                *out++ = '<';
                *out++ = '>';
                *out++ = ' ';
                *out++ = '<';
                *out++ = '>';
                *out++ = ' ';
                *out++ = '.';
                *out++ = '\n';
              }
              totalBytesWritten += static_cast<size_t>(out - bufStart);
              return totalBytesWritten;
            });

        auto end = std::chrono::high_resolution_clock::now();
        uint64_t endCycles = readCpuCycles();
        uint64_t elapsedNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
        uint64_t totalCycles =
            (endCycles >= startCycles) ? (endCycles - startCycles) : 0;

        double throughputMRowsPerSec =
            (static_cast<double>(NUM_ROWS) /
             (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double cyclesPerRow =
            static_cast<double>(totalCycles) / static_cast<double>(NUM_ROWS);
        double latencyNsPerRow =
            static_cast<double>(elapsedNs) / static_cast<double>(NUM_ROWS);
        double totalMbWritten =
            static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0);

        m.metadata().addKeyValuePair("rows-processed", NUM_ROWS);
        m.metadata().addKeyValuePair("throughput-m-rows-per-sec",
                                     throughputMRowsPerSec);
        m.metadata().addKeyValuePair("cycles-per-row", cyclesPerRow);
        m.metadata().addKeyValuePair("latency-ns-per-row", latencyNsPerRow);
        m.metadata().addKeyValuePair("total-mb-written", totalMbWritten);
      }

      // 3.2 SWAR 64-bit pure delimiter writes
      {
        size_t totalBytesWritten = 0;
        uint64_t startCycles = readCpuCycles();
        auto start = std::chrono::high_resolution_clock::now();

        auto& m = group.addMeasurement(
            "SwarDelimiterPacker pure delimiter writing (3 64-bit stores/row)",
            [&]() {
              char* out = bufStart;
              for (size_t i = 0; i < NUM_ROWS; ++i) {
                if (out >= bufLimit) {
                  totalBytesWritten += static_cast<size_t>(out - bufStart);
                  out = bufStart;
                }
                out = SwarDelimiterPacker::writeDelim64<3>(
                    out, SwarDelimiterPacker::TRIPLE_S_TO_P_IRI.pattern());
                out = SwarDelimiterPacker::writeDelim64<3>(
                    out, SwarDelimiterPacker::TRIPLE_P_TO_O_IRI.pattern());
                out = SwarDelimiterPacker::writeDelim64<4>(
                    out, SwarDelimiterPacker::TRIPLE_O_IRI_END.pattern());
              }
              totalBytesWritten += static_cast<size_t>(out - bufStart);
              return totalBytesWritten;
            });

        auto end = std::chrono::high_resolution_clock::now();
        uint64_t endCycles = readCpuCycles();
        uint64_t elapsedNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
        uint64_t totalCycles =
            (endCycles >= startCycles) ? (endCycles - startCycles) : 0;

        double throughputMRowsPerSec =
            (static_cast<double>(NUM_ROWS) /
             (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double cyclesPerRow =
            static_cast<double>(totalCycles) / static_cast<double>(NUM_ROWS);
        double latencyNsPerRow =
            static_cast<double>(elapsedNs) / static_cast<double>(NUM_ROWS);
        double totalMbWritten =
            static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0);

        m.metadata().addKeyValuePair("rows-processed", NUM_ROWS);
        m.metadata().addKeyValuePair("throughput-m-rows-per-sec",
                                     throughputMRowsPerSec);
        m.metadata().addKeyValuePair("cycles-per-row", cyclesPerRow);
        m.metadata().addKeyValuePair("latency-ns-per-row", latencyNsPerRow);
        m.metadata().addKeyValuePair("total-mb-written", totalMbWritten);
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(SwarDelimiterBenchmark);

}  // namespace
}  // namespace ad_benchmark
