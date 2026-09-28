// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <charconv>
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

#include "backports/span.h"
#include "engine/BranchlessTypeDispatcher.h"
#include "global/Id.h"
#include "global/ValueId.h"

using namespace ql::engine;

namespace {

// Volatile sink for formatted output bytes. The benchmark otherwise only
// counts bytes, so without a downstream read the optimizer could discard
// the formatting work being measured.
volatile size_t gFormattedBytesSink = 0;

// _____________________________________________________________________________
// Copy a string literal into `out` without a magic length: `sizeof` counts
// the terminator, so `N - 1` is exactly the payload size.
template <size_t N>
char* copyLiteral(char* out, const char (&literal)[N]) {
  static_assert(N >= 1);
  std::memcpy(out, literal, N - 1);
  return out + (N - 1);
}

// _____________________________________________________________________________
// Linux perf_event hardware branch counter tracker.
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
      if (ioctl(branchFd_, PERF_EVENT_IOC_RESET, 0) != 0 ||
          ioctl(missFd_, PERF_EVENT_IOC_RESET, 0) != 0 ||
          ioctl(branchFd_, PERF_EVENT_IOC_ENABLE, 0) != 0 ||
          ioctl(missFd_, PERF_EVENT_IOC_ENABLE, 0) != 0) {
        // A counter that fails to arm would report stale data, so fall
        // back to zeros instead of reading invalid descriptors.
        isSupported_ = false;
      }
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
      if (r1 != static_cast<ssize_t>(sizeof(uint64_t))) {
        branchCount = 0;
      }
      if (r2 != static_cast<ssize_t>(sizeof(uint64_t))) {
        missCount = 0;
      }
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
// Baseline 1: Standard switch-based branching dispatcher.
struct BranchingSwitchDispatcher {
  static char* dispatchTermFormat(ValueId id, std::string_view rawTerm,
                                  char* out) noexcept {
    switch (id.getDatatype()) {
      case Datatype::Undefined:
        return out;
      case Datatype::Bool: {
        out = copyLiteral(out, "\"");
        const bool b = id.getBool();
        if (b) {
          out = copyLiteral(out, "true");
        } else {
          out = copyLiteral(out, "false");
        }
        out =
            copyLiteral(out, "\"^^<http://www.w3.org/2001/XMLSchema#boolean>");
        return out;
      }
      case Datatype::Int: {
        out = copyLiteral(out, "\"");
        auto [p, ec] = std::to_chars(out, out + 24, id.getInt());
        (void)ec;
        out = p;
        out =
            copyLiteral(out, "\"^^<http://www.w3.org/2001/XMLSchema#integer>");
        return out;
      }
      case Datatype::Double: {
        out = copyLiteral(out, "\"");
        out = ql::engine::detail::formatDoubleValue(out, out + 32,
                                                    id.getDouble());
        out = copyLiteral(out, "\"^^<http://www.w3.org/2001/XMLSchema#double>");
        return out;
      }
      case Datatype::VocabIndex:
      case Datatype::LocalVocabIndex:
      case Datatype::SecondaryVocabIndex:
      case Datatype::EncodedVal: {
        out = copyLiteral(out, "<");
        std::memcpy(out, rawTerm.data(), rawTerm.size());
        out += rawTerm.size();
        out = copyLiteral(out, ">");
        return out;
      }
      case Datatype::TextRecordIndex:
      case Datatype::WordVocabIndex: {
        out = copyLiteral(out, "\"");
        std::memcpy(out, rawTerm.data(), rawTerm.size());
        out += rawTerm.size();
        out = copyLiteral(out, "\"");
        return out;
      }
      case Datatype::Date: {
        out = copyLiteral(out, "\"");
        auto [str, type] = id.getDate().toStringAndType();
        (void)type;
        std::memcpy(out, str.data(), str.size());
        out += str.size();
        out =
            copyLiteral(out, "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>");
        return out;
      }
      case Datatype::GeoPoint: {
        out = copyLiteral(out, "\"");
        auto [str, type] = id.getGeoPoint().toStringAndType();
        (void)type;
        std::memcpy(out, str.data(), str.size());
        out += str.size();
        out = copyLiteral(
            out, "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
        return out;
      }
      case Datatype::BlankNodeIndex: {
        out = copyLiteral(out, "_:bn");
        auto [p, ec] =
            std::to_chars(out, out + 24, id.getBlankNodeIndex().get());
        (void)ec;
        out = p;
        return out;
      }
      default:
        return out;
    }
  }

  static size_t dispatchBatchTermFormat(
      ql::span<const ValueId> ids, ql::span<const std::string_view> rawTerms,
      char* out) noexcept {
    char* curr = out;
    const size_t numTerms = ids.size();
    for (size_t i = 0; i < numTerms; ++i) {
      curr = dispatchTermFormat(ids[i], rawTerms[i], curr);
    }
    return static_cast<size_t>(curr - out);
  }
};

// _____________________________________________________________________________
// Baseline 2: Chained if-else branching dispatcher.
struct BranchingIfElseDispatcher {
  static char* dispatchTermFormat(ValueId id, std::string_view rawTerm,
                                  char* out) noexcept {
    const Datatype dt = id.getDatatype();
    if (dt == Datatype::VocabIndex || dt == Datatype::LocalVocabIndex ||
        dt == Datatype::SecondaryVocabIndex || dt == Datatype::EncodedVal) {
      out = copyLiteral(out, "<");
      std::memcpy(out, rawTerm.data(), rawTerm.size());
      out += rawTerm.size();
      out = copyLiteral(out, ">");
      return out;
    } else if (dt == Datatype::TextRecordIndex ||
               dt == Datatype::WordVocabIndex) {
      out = copyLiteral(out, "\"");
      std::memcpy(out, rawTerm.data(), rawTerm.size());
      out += rawTerm.size();
      out = copyLiteral(out, "\"");
      return out;
    } else if (dt == Datatype::Int) {
      out = copyLiteral(out, "\"");
      auto [p, ec] = std::to_chars(out, out + 24, id.getInt());
      (void)ec;
      out = p;
      out = copyLiteral(out, "\"^^<http://www.w3.org/2001/XMLSchema#integer>");
      return out;
    } else if (dt == Datatype::BlankNodeIndex) {
      out = copyLiteral(out, "_:bn");
      auto [p, ec] = std::to_chars(out, out + 24, id.getBlankNodeIndex().get());
      (void)ec;
      out = p;
      return out;
    } else if (dt == Datatype::Double) {
      out = copyLiteral(out, "\"");
      out =
          ql::engine::detail::formatDoubleValue(out, out + 32, id.getDouble());
      out = copyLiteral(out, "\"^^<http://www.w3.org/2001/XMLSchema#double>");
      return out;
    } else if (dt == Datatype::Bool) {
      out = copyLiteral(out, "\"");
      if (id.getBool()) {
        out = copyLiteral(out, "true");
      } else {
        out = copyLiteral(out, "false");
      }
      out = copyLiteral(out, "\"^^<http://www.w3.org/2001/XMLSchema#boolean>");
      return out;
    } else if (dt == Datatype::Date) {
      out = copyLiteral(out, "\"");
      auto [str, type] = id.getDate().toStringAndType();
      (void)type;
      std::memcpy(out, str.data(), str.size());
      out += str.size();
      out = copyLiteral(out, "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>");
      return out;
    } else if (dt == Datatype::GeoPoint) {
      out = copyLiteral(out, "\"");
      auto [str, type] = id.getGeoPoint().toStringAndType();
      (void)type;
      std::memcpy(out, str.data(), str.size());
      out += str.size();
      out = copyLiteral(
          out, "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
      return out;
    }
    return out;
  }

  static size_t dispatchBatchTermFormat(
      ql::span<const ValueId> ids, ql::span<const std::string_view> rawTerms,
      char* out) noexcept {
    char* curr = out;
    const size_t numTerms = ids.size();
    for (size_t i = 0; i < numTerms; ++i) {
      curr = dispatchTermFormat(ids[i], rawTerms[i], curr);
    }
    return static_cast<size_t>(curr - out);
  }
};

// _____________________________________________________________________________
// Benchmark dataset generator for mixed RDF distribution.
// 50% IRIs, 30% Literals, 10% Blank Nodes, 10% Integers.
struct BenchmarkDataset {
  std::vector<ValueId> ids_;
  std::vector<std::string> stringStorage_;
  std::vector<std::string_view> rawTerms_;

  static BenchmarkDataset generate(size_t numTerms, uint32_t seed = 42) {
    BenchmarkDataset ds;
    ds.ids_.reserve(numTerms);
    ds.stringStorage_.reserve(numTerms);
    ds.rawTerms_.reserve(numTerms);

    std::mt19937 gen(seed);
    std::uniform_int_distribution<int> dist(0, 99);

    for (size_t i = 0; i < numTerms; ++i) {
      int roll = dist(gen);
      if (roll < 50) {
        // 50% IRIs
        ds.ids_.push_back(ValueId::makeFromVocabIndex(VocabIndex::make(i)));
        ds.stringStorage_.push_back("http://example.org/entity/resource_" +
                                    std::to_string(i % 10000));
        ds.rawTerms_.push_back(ds.stringStorage_.back());
      } else if (roll < 80) {
        // 30% Literals
        ds.ids_.push_back(
            ValueId::makeFromTextRecordIndex(TextRecordIndex::make(i)));
        ds.stringStorage_.push_back("Sample textual literal term value " +
                                    std::to_string(i % 5000));
        ds.rawTerms_.push_back(ds.stringStorage_.back());
      } else if (roll < 90) {
        // 10% Blank Nodes
        ds.ids_.push_back(
            ValueId::makeFromBlankNodeIndex(BlankNodeIndex::make(i % 100000)));
        ds.rawTerms_.push_back("");
      } else {
        // 10% Integers
        ds.ids_.push_back(
            ValueId::makeFromInt(static_cast<int64_t>(i * 31 + 7)));
        ds.rawTerms_.push_back("");
      }
    }
    return ds;
  }
};

struct BenchmarkResult {
  std::string name_;
  double elapsedMs_ = 0.0;
  double throughputMTermsPerSec_ = 0.0;
  double nsPerTerm_ = 0.0;
  uint64_t totalBranches_ = 0;
  uint64_t branchMisses_ = 0;
  double branchMissRate_ = 0.0;
  double branchMissesPerTerm_ = 0.0;
  size_t bytesWritten_ = 0;
};

template <typename Dispatcher>
BenchmarkResult runBenchmark(const std::string& name,
                             const BenchmarkDataset& ds,
                             std::vector<char>& outputBuffer,
                             HardwarePerfCounter& perf, size_t iterations = 5) {
  if (ds.ids_.empty()) {
    return BenchmarkResult{name, 0.0, 0.0, 0.0, 0, 0, 0.0, 0.0, 0};
  }
  // Warmup
  Dispatcher::dispatchBatchTermFormat(ds.ids_, ds.rawTerms_,
                                      outputBuffer.data());

  double totalMs = 0.0;
  uint64_t totalBranches = 0;
  uint64_t totalMisses = 0;
  size_t bytesWritten = 0;

  for (size_t iter = 0; iter < iterations; ++iter) {
    perf.start();
    auto t0 = std::chrono::high_resolution_clock::now();

    bytesWritten = Dispatcher::dispatchBatchTermFormat(ds.ids_, ds.rawTerms_,
                                                       outputBuffer.data());

    auto t1 = std::chrono::high_resolution_clock::now();
    uint64_t branches = 0;
    uint64_t misses = 0;
    perf.stop(branches, misses);

    totalMs += std::chrono::duration<double, std::milli>(t1 - t0).count();
    totalBranches += branches;
    totalMisses += misses;
  }

  // Fold every written byte into the sink so the timed formatting loops
  // stay observable to the optimizer. Plain assignment (not `+=`): compound
  // assignment on a volatile operand is deprecated in C++20 and fails
  // `-Werror=volatile` builds.
  for (size_t i = 0; i < bytesWritten; ++i) {
    gFormattedBytesSink =
        gFormattedBytesSink +
        static_cast<size_t>(static_cast<unsigned char>(outputBuffer[i]));
  }

  double avgMs = totalMs / static_cast<double>(iterations);
  double totalTerms = static_cast<double>(ds.ids_.size());
  double mTermsPerSec = (totalTerms / (avgMs / 1000.0)) / 1e6;
  double nsPerTerm = (avgMs * 1e6) / totalTerms;

  uint64_t avgBranches = totalBranches / iterations;
  uint64_t avgMisses = totalMisses / iterations;
  double missRate = avgBranches > 0 ? (100.0 * static_cast<double>(avgMisses) /
                                       static_cast<double>(avgBranches))
                                    : 0.0;
  double missesPerTerm = static_cast<double>(avgMisses) / totalTerms;

  return BenchmarkResult{name,      avgMs,         mTermsPerSec,
                         nsPerTerm, avgBranches,   avgMisses,
                         missRate,  missesPerTerm, bytesWritten};
}

void printResults(const std::vector<BenchmarkResult>& results) {
  std::cout << "\n============================================================="
               "===========================================\n";
  std::cout
      << " Optimization 15: Branchless LUT Type Dispatcher Microbenchmark\n";
  std::cout << " Workload: Mixed RDF (50% IRIs, 30% Literals, 10% Blank Nodes, "
               "10% Integers)\n";
  std::cout << "==============================================================="
               "=========================================\n\n";

  std::cout << std::left << std::setw(28) << "Dispatcher" << std::right
            << std::setw(12) << "Time (ms)" << std::setw(18)
            << "Throughput (M/s)" << std::setw(15) << "ns / term"
            << std::setw(16) << "Branch Misses" << std::setw(14) << "Miss Rate"
            << std::setw(15) << "Misses/Term" << std::setw(14) << "Branches"
            << std::setw(12) << "Bytes"
            << "\n";
  std::cout << std::string(144, '-') << "\n";

  for (const auto& r : results) {
    std::cout << std::left << std::setw(28) << r.name_ << std::right
              << std::fixed << std::setprecision(2) << std::setw(12)
              << r.elapsedMs_ << std::setw(18) << r.throughputMTermsPerSec_
              << std::setw(15) << r.nsPerTerm_ << std::setw(16)
              << r.branchMisses_ << std::setw(13) << r.branchMissRate_ << "%"
              << std::setw(15) << std::setprecision(4) << r.branchMissesPerTerm_
              << std::setw(14) << r.totalBranches_ << std::setw(12)
              << r.bytesWritten_ << "\n";
  }

  std::cout << std::string(144, '-') << "\n\n";

  const BenchmarkResult* base = nullptr;
  const BenchmarkResult* lut = nullptr;
  for (const auto& r : results) {
    if (r.name_ == "Branching Switch") {
      base = &r;
    } else if (r.name_ == "Branchless LUT Dispatcher") {
      lut = &r;
    }
  }
  if (base != nullptr && lut != nullptr &&
      base->throughputMTermsPerSec_ > 0.0) {
    double speedup =
        lut->throughputMTermsPerSec_ / base->throughputMTermsPerSec_;
    std::cout << ">> Branchless LUT Speedup over Switch Dispatcher: "
              << std::fixed << std::setprecision(2) << speedup << "x (+"
              << ((speedup - 1.0) * 100.0) << "% throughput)\n";
  }
  std::cout << "==============================================================="
               "=========================================\n\n";
}

}  // namespace

int main(int argc, char** argv) {
  size_t numTerms = 5'000'000;
  if (argc > 1) {
    numTerms = std::stoull(argv[1]);
  }
  if (numTerms == 0) {
    std::cerr << "Number of terms must be greater than 0.\n";
    return 1;
  }

  std::cout << "Generating synthetic mixed RDF dataset with " << numTerms
            << " terms...\n";
  auto dataset = BenchmarkDataset::generate(numTerms);

  // Output buffer: 128 bytes per formatted term.
  std::vector<char> outputBuffer(numTerms * 128);

  HardwarePerfCounter perf;
  if (perf.isSupported()) {
    std::cout
        << "[Hardware Performance Counters: Enabled (Linux perf_event)]\n";
  } else {
    std::cout << "[Hardware Performance Counters: Unavailable / Restricted - "
                 "timing only]\n";
  }

  std::vector<BenchmarkResult> results;
  std::cout << "Running Benchmark 1: Branching Switch Dispatcher...\n";
  results.push_back(runBenchmark<BranchingSwitchDispatcher>(
      "Branching Switch", dataset, outputBuffer, perf));

  std::cout << "Running Benchmark 2: Branching If-Else Dispatcher...\n";
  results.push_back(runBenchmark<BranchingIfElseDispatcher>(
      "Branching If-Else", dataset, outputBuffer, perf));

  std::cout << "Running Benchmark 3: Branchless LUT Dispatcher...\n";
  results.push_back(runBenchmark<BranchlessTypeDispatcher>(
      "Branchless LUT Dispatcher", dataset, outputBuffer, perf));

  printResults(results);
  std::cout << "Formatted output checksum sink: " << gFormattedBytesSink
            << "\n";
  return 0;
}
