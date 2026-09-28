// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/ConstructTypes.h"
#include "engine/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "util/http/MediaTypes.h"

// _____________________________________________________________________________
// Memory allocation tracker for measuring heap allocation counts via the
// scalar and array `operator new` overloads.
struct AllocationTracker {
  static inline std::atomic<bool> enabled_{false};
  static inline std::atomic<size_t> count_{0};

  // Counts allocations for its lifetime; the destructor stops counting on
  // every exit path, including exceptions from the measured code.
  class Scope {
   public:
    Scope() {
      count_.store(0, std::memory_order_seq_cst);
      enabled_.store(true, std::memory_order_seq_cst);
    }
    ~Scope() { enabled_.store(false, std::memory_order_seq_cst); }
    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;

    [[nodiscard]] size_t count() const {
      return count_.load(std::memory_order_seq_cst);
    }
  };
};

// Global new/delete instrumentation for allocation counting during benchmark
// runs. The malloc/free pairing below is intentional and matched, but GCC
// cannot see across the replaceable global operators and reports a false
// positive -Wmismatched-new-delete. GCC raises it while compiling the
// allocation call sites (via inlining), so a pragma around the `operator
// delete` definitions alone does not cover it (observed on GCC 11 with
// -Werror); the warning is therefore suppressed file-wide (GCC only). The
// sized-deallocation overloads must stay: GCC's -Wsized-deallocation (part of
// -Wextra) rejects an unsized `operator delete` without its sized partner.
//
// Skipped under AddressSanitizer or ThreadSanitizer: their runtimes
// already provide these replaceable allocation functions, so defining them
// here causes multiple-definition link errors. Under sanitizers the
// `heap-allocations` metadata below reads 0. Clang signals sanitizers via
// `__has_feature`, GCC via the `__SANITIZE_*` macros; `__has_feature` must
// only be invoked where it is defined, so the checks are nested.
#if defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define SERIALIZER_MICRO_BENCHMARK_UNDER_SANITIZER 1
#endif
#elif defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define SERIALIZER_MICRO_BENCHMARK_UNDER_SANITIZER 1
#endif

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"
#endif

#ifndef SERIALIZER_MICRO_BENCHMARK_UNDER_SANITIZER
void* operator new(std::size_t size) {
  if (AllocationTracker::enabled_.load(std::memory_order_relaxed)) {
    AllocationTracker::count_.fetch_add(1, std::memory_order_relaxed);
  }
  void* ptr = std::malloc(size);
  if (!ptr) {
    throw std::bad_alloc();
  }
  return ptr;
}

void operator delete(void* ptr) noexcept { std::free(ptr); }

void operator delete(void* ptr, std::size_t) noexcept { std::free(ptr); }

void* operator new[](std::size_t size) {
  if (AllocationTracker::enabled_.load(std::memory_order_relaxed)) {
    AllocationTracker::count_.fetch_add(1, std::memory_order_relaxed);
  }
  void* ptr = std::malloc(size);
  if (!ptr) {
    throw std::bad_alloc();
  }
  return ptr;
}

void operator delete[](void* ptr) noexcept { std::free(ptr); }

void operator delete[](void* ptr, std::size_t) noexcept { std::free(ptr); }
#endif  // SERIALIZER_MICRO_BENCHMARK_UNDER_SANITIZER

namespace ad_benchmark {
namespace {

using namespace qlever::constructExport;
using namespace ql::export_formatting;

// _____________________________________________________________________________
// Generates 1,000,000 synthetic triples representing realistic SPARQL exports.
std::vector<EvaluatedTriple> generateSyntheticTriples(size_t numTriples) {
  std::vector<EvaluatedTriple> triples;
  triples.reserve(numTriples);

  // Common predicates
  auto predLabel = std::make_shared<EvaluatedTermData>(
      "<http://www.w3.org/2000/01/rdf-schema#label>", nullptr);
  auto predType = std::make_shared<EvaluatedTermData>(
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", nullptr);
  auto predProp = std::make_shared<EvaluatedTermData>(
      "<http://example.org/prop/hasValue>", nullptr);

  for (size_t i = 0; i < numTriples; ++i) {
    // Subject: Entity IRI or Blank Node
    EvaluatedTerm subj;
    if (i % 10 == 0) {
      subj = std::make_shared<EvaluatedTermData>("_:b" + std::to_string(i),
                                                 nullptr);
    } else {
      subj = std::make_shared<EvaluatedTermData>(
          "<http://example.org/entity/Q" + std::to_string(i) + ">", nullptr);
    }

    // Predicate
    EvaluatedTerm pred = (i % 3 == 0)   ? predLabel
                         : (i % 3 == 1) ? predType
                                        : predProp;

    // Object: realistic mixture
    EvaluatedTerm obj;
    size_t kind = i % 5;
    switch (kind) {
      case 0:
        // Plain IRI
        obj = std::make_shared<EvaluatedTermData>(
            "<http://example.org/entity/Q" + std::to_string(i * 3 + 7) + ">",
            nullptr);
        break;
      case 1:
        // Plain literal
        obj = std::make_shared<EvaluatedTermData>(
            "\"Simple Label " + std::to_string(i) + "\"@en", nullptr);
        break;
      case 2:
        // Literal requiring escaping (quotes, newlines, tabs). In a
        // normalized literal an embedded quote is a real `"` character
        // (only escaped at the C++ source level); a backslash-quote
        // sequence would denote a literal backslash and would measure
        // double-escaping instead of the real export path.
        obj = std::make_shared<EvaluatedTermData>(
            "\"Title with \"quotes\" and \nnewline and \ttab " +
                std::to_string(i) + "\"",
            nullptr);
        break;
      case 3:
        // Encoded integer literal
        obj = std::make_shared<EvaluatedTermData>(std::to_string(i * 42),
                                                  XSD_INT_TYPE);
        break;
      case 4:
      default:
        // Encoded decimal literal
        obj = std::make_shared<EvaluatedTermData>(std::to_string(i) + ".75",
                                                  XSD_DECIMAL_TYPE);
        break;
    }

    triples.push_back(
        EvaluatedTriple{std::move(subj), std::move(pred), std::move(obj)});
  }

  return triples;
}

// _____________________________________________________________________________
class SerializerMicroBenchmark : public BenchmarkInterface {
 private:
  static constexpr size_t NUM_TRIPLES = 1'000'000;
  std::vector<EvaluatedTriple> triples_;

 public:
  SerializerMicroBenchmark() {
    std::cout << "Generating " << NUM_TRIPLES
              << " synthetic triples for export microbenchmark..." << std::endl;
    triples_ = generateSyntheticTriples(NUM_TRIPLES);
    std::cout << "Synthetic dataset generation complete." << std::endl;
  }

  std::string name() const final {
    return "SPARQL Export Serializer MicroBenchmark (1,000,000 Triples)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    const std::vector<std::pair<std::string, ad_utility::MediaType>> formats = {
        {"Turtle", ad_utility::MediaType::turtle},
        {"CSV", ad_utility::MediaType::csv},
        {"TSV", ad_utility::MediaType::tsv}};

    for (const auto& [formatName, mediaType] : formats) {
      auto& group = results.addGroup(formatName +
                                     " Serialization Comparison (1M Triples)");
      const ExportFormat exportFmt = toExportFormat(mediaType);

      // 1. Baseline: string-constructing serialization
      {
        size_t baselineAllocations = 0;
        size_t totalBytesWritten = 0;

        auto& m = group.addMeasurement(
            "Baseline string-constructing (" + formatName + ")", [&]() {
              AllocationTracker::Scope allocationScope;
              size_t bytes = 0;
              for (const auto& triple : triples_) {
                std::string formatted = formatTriple(triple, mediaType);
                bytes += formatted.size();
              }
              baselineAllocations = allocationScope.count();
              totalBytesWritten = bytes;
              return bytes;
            });

        m.metadata().addKeyValuePair("total-triples", NUM_TRIPLES);
        m.metadata().addKeyValuePair("heap-allocations", baselineAllocations);
        m.metadata().addKeyValuePair(
            "total-bytes-mb",
            static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0));
      }

      // 2. FastExportStreamFormatter: zero-allocation serialization
      {
        size_t fastAllocations = 0;
        size_t totalBytesWritten = 0;
        size_t chunksEmitted = 0;

        auto& m = group.addMeasurement(
            "FastExportStreamFormatter zero-allocation (" + formatName + ")",
            [&]() {
              AllocationTracker::Scope allocationScope;
              size_t bytes = 0;
              size_t chunkCount = 0;

              // Chunk sink receives 1MB memory views directly
              auto sink = [&](std::string_view chunk) {
                bytes += chunk.size();
                ++chunkCount;
              };

              FastExportStreamFormatter formatter(
                  sink, FastExportStreamFormatter::DEFAULT_CHUNK_SIZE);
              for (const auto& triple : triples_) {
                formatter.writeTriple(exportFmt, triple);
              }
              auto summary = std::move(formatter).finalize();

              fastAllocations = allocationScope.count();
              totalBytesWritten = summary.totalBytesWritten_;
              chunksEmitted = summary.chunksEmitted_;
              return totalBytesWritten;
            });

        m.metadata().addKeyValuePair("total-triples", NUM_TRIPLES);
        m.metadata().addKeyValuePair("heap-allocations", fastAllocations);
        m.metadata().addKeyValuePair("chunks-emitted", chunksEmitted);
        m.metadata().addKeyValuePair(
            "total-bytes-mb",
            static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0));
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(SerializerMicroBenchmark);

}  // namespace
}  // namespace ad_benchmark

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
