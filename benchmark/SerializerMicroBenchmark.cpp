// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/ConstructTypes.h"
#include "engine/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "util/Iterators.h"
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
// A realistic mixture of export terms: IRIs, blank nodes, plain literals,
// literals that need escaping, and encoded integers and decimals.
std::vector<EvaluatedTriple> generateMixedTriples(size_t numTriples) {
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
// Triples shaped like the DBLP query `H-vocab-title-large`
// (`CONSTRUCT { ?paper dblp:title ?title }`): a record IRI, one shared
// predicate, and a long plain title literal (about 60 to 190 bytes, on average
// about 125). One title in 100 contains an embedded quote, so it takes the
// escaping path.
std::vector<EvaluatedTriple> generateTitleTriples(size_t numTriples) {
  std::vector<EvaluatedTriple> triples;
  triples.reserve(numTriples);
  auto predTitle = std::make_shared<EvaluatedTermData>(
      "<https://dblp.org/rdf/schema#title>", nullptr);
  static constexpr std::string_view words[] = {
      "Efficient",  "Query",  "Processing",  "for",      "Large",
      "Knowledge",  "Graphs", "with",        "Learned",  "Index",
      "Structures", "on",     "Modern",      "Hardware", "A",
      "Survey",     "of",     "Distributed", "Systems",  "and"};
  for (size_t i = 0; i < numTriples; ++i) {
    auto subj = std::make_shared<EvaluatedTermData>(
        absl::StrCat("<https://dblp.org/rec/conf/example/Author", i % 9973, "_",
                     i, ">"),
        nullptr);
    std::string title = "\"";
    const size_t numWords = 8 + (i * 7) % 16;
    for (size_t w = 0; w < numWords; ++w) {
      if (w > 0) {
        title += ' ';
      }
      title += words[(i + w * 13) % std::size(words)];
    }
    if (i % 100 == 0) {
      title += " \"Revisited\"";
    }
    title += ".\"";
    triples.push_back(EvaluatedTriple{
        std::move(subj), predTitle,
        std::make_shared<EvaluatedTermData>(std::move(title), nullptr)});
  }
  return triples;
}

// _____________________________________________________________________________
// The triples as the input range that `ConstructTripleGenerator` gets from
// `evaluateTables`: one `EvaluatedTriple` (three `shared_ptr` copies) per
// `get()`.
ad_utility::InputRangeTypeErased<EvaluatedTriple> asEvaluatedRange(
    const std::vector<EvaluatedTriple>& triples) {
  return ad_utility::InputRangeTypeErased<EvaluatedTriple>{
      ad_utility::InputRangeFromGetCallable{[&triples, i = size_t{0}]() mutable
                                            -> std::optional<EvaluatedTriple> {
        if (i == triples.size()) {
          return std::nullopt;
        }
        return triples[i++];
      }}};
}

// The consumer of the formatted strings: like the HTTP layer, copy them into a
// reusable 1 MiB response buffer. Returns the number of bytes and, only in the
// untimed verification pass (`computeChecksum_`), a checksum of the bytes, so
// that the arms can be compared.
struct Consumed {
  size_t bytes_ = 0;
  uint64_t checksum_ = 0;
};
class ResponseBufferSink {
 public:
  static inline bool computeChecksum_ = false;

  void consume(std::string_view piece) {
    if (used_ + piece.size() > buffer_.size()) {
      flush();
    }
    if (piece.size() > buffer_.size()) {
      absorb(piece);
      return;
    }
    std::memcpy(buffer_.data() + used_, piece.data(), piece.size());
    used_ += piece.size();
  }
  Consumed finish() {
    flush();
    return result_;
  }

 private:
  void flush() {
    absorb(std::string_view{buffer_.data(), used_});
    used_ = 0;
  }
  void absorb(std::string_view chunk) {
    result_.bytes_ += chunk.size();
    if (!computeChecksum_) {
      return;
    }
    for (char c : chunk) {
      result_.checksum_ = result_.checksum_ * 31 + static_cast<uint8_t>(c);
    }
  }
  std::string buffer_ = std::string(1 << 20, '\0');
  size_t used_ = 0;
  Consumed result_;
};

// One way of turning all triples into the export byte stream.
struct Arm {
  std::string name_;
  std::function<Consumed(const std::vector<EvaluatedTriple>&)> run_;
};

// _____________________________________________________________________________
// The Turtle arms: the three production paths of
// `ConstructTripleGenerator::generateFormattedTriples` (legacy `formatTriple`,
// one fast-formatted string per triple, fast-formatted ~64 KiB batches), each
// fed from and consumed like in the export, plus the formatter in streaming
// mode as a lower bound.
std::vector<Arm> turtleArms() {
  using ad_utility::InputRangeTypeErased;
  std::vector<Arm> arms;
  arms.push_back({"legacy formatTriple (flag off)", [](const auto& triples) {
                    InputRangeTypeErased<std::string> range{
                        asEvaluatedRange(triples) |
                        ql::views::transform([](const EvaluatedTriple& t) {
                          return formatTriple(t, ad_utility::MediaType::turtle);
                        })};
                    ResponseBufferSink sink;
                    for (const std::string& s : range) {
                      sink.consume(s);
                    }
                    return sink.finish();
                  }});
  arms.push_back({"fast, one string per triple", [](const auto& triples) {
                    InputRangeTypeErased<std::string> range{
                        asEvaluatedRange(triples) |
                        ql::views::transform([](const EvaluatedTriple& t) {
                          return formatTripleAsTurtleWithFastFormatter(t);
                        })};
                    ResponseBufferSink sink;
                    for (const std::string& s : range) {
                      sink.consume(s);
                    }
                    return sink.finish();
                  }});
  arms.push_back({"fast, 64 KiB batches (flag on)", [](const auto& triples) {
                    auto range = formatTriplesAsTurtleInBatches(
                        asEvaluatedRange(triples));
                    ResponseBufferSink sink;
                    for (const std::string& s : range) {
                      sink.consume(s);
                    }
                    return sink.finish();
                  }});
  arms.push_back(
      {"formatter streaming mode (lower bound)", [](const auto& triples) {
         ResponseBufferSink sink;
         FastExportStreamFormatter formatter(
             [&sink](std::string_view chunk) { sink.consume(chunk); },
             FastExportStreamFormatter::DEFAULT_CHUNK_SIZE);
         for (const auto& triple : triples) {
           formatter.writeTriple(ExportFormat::Turtle, triple);
         }
         [[maybe_unused]] auto summary = std::move(formatter).finalize();
         return sink.finish();
       }});
  return arms;
}

// _____________________________________________________________________________
// CSV / TSV: legacy `formatTriple` vs the formatter in streaming mode.
std::vector<Arm> tabularArms(ad_utility::MediaType mediaType) {
  std::vector<Arm> arms;
  arms.push_back({"legacy formatTriple", [mediaType](const auto& triples) {
                    ResponseBufferSink sink;
                    for (const auto& triple : triples) {
                      sink.consume(formatTriple(triple, mediaType));
                    }
                    return sink.finish();
                  }});
  arms.push_back(
      {"formatter streaming mode", [mediaType](const auto& triples) {
         ResponseBufferSink sink;
         FastExportStreamFormatter formatter(
             [&sink](std::string_view chunk) { sink.consume(chunk); },
             FastExportStreamFormatter::DEFAULT_CHUNK_SIZE);
         const ExportFormat format = toExportFormat(mediaType);
         for (const auto& triple : triples) {
           formatter.writeTriple(format, triple);
         }
         [[maybe_unused]] auto summary = std::move(formatter).finalize();
         return sink.finish();
       }});
  return arms;
}

// _____________________________________________________________________________
// Measurement protocol: per arm one untimed warm-up pass (which also
// calibrates the number of passes so that one trial takes at least
// `MIN_TRIAL_SECONDS`) and one pass with the allocation counter; then
// `NUM_TRIALS` trials, interleaved across the arms (arm 0, arm 1, ..., arm 0,
// ...). Reported per triple: median, min and max time over the trials, heap
// allocations and output bytes. All arms must produce the same bytes.
class SerializerMicroBenchmark : public BenchmarkInterface {
 private:
  static constexpr size_t NUM_TRIPLES = 1'000'000;
  static constexpr size_t NUM_TRIALS = 10;
  static constexpr double MIN_TRIAL_SECONDS = 1.0;
  std::vector<EvaluatedTriple> mixedTriples_;
  std::vector<EvaluatedTriple> titleTriples_;

  using Clock = std::chrono::steady_clock;

  static double secondsOf(const Arm& arm,
                          const std::vector<EvaluatedTriple>& triples,
                          size_t passes, uint64_t& sinkChecksum) {
    const auto start = Clock::now();
    for (size_t p = 0; p < passes; ++p) {
      sinkChecksum ^= arm.run_(triples).checksum_;
    }
    return std::chrono::duration<double>(Clock::now() - start).count();
  }

  static void measure(BenchmarkResults& results, const std::string& title,
                      const std::vector<Arm>& arms,
                      const std::vector<EvaluatedTriple>& triples) {
    const size_t n = triples.size();
    std::vector<size_t> passes(arms.size());
    std::vector<size_t> allocationsPerRun(arms.size());
    std::vector<size_t> bytesPerRun(arms.size());
    std::vector<std::vector<double>> nsPerTriple(arms.size());
    uint64_t sinkChecksum = 0;
    std::optional<Consumed> reference;
    for (size_t a = 0; a < arms.size(); ++a) {
      // Untimed warm-up and calibration.
      const double warmUp = secondsOf(arms[a], triples, 1, sinkChecksum);
      passes[a] = std::max<size_t>(
          1, static_cast<size_t>(std::ceil(MIN_TRIAL_SECONDS / warmUp)));
      Consumed consumed;
      {
        AllocationTracker::Scope allocationScope;
        ResponseBufferSink::computeChecksum_ = true;
        consumed = arms[a].run_(triples);
        ResponseBufferSink::computeChecksum_ = false;
        allocationsPerRun[a] = allocationScope.count();
      }
      bytesPerRun[a] = consumed.bytes_;
      if (!reference.has_value()) {
        reference = consumed;
      }
      AD_CORRECTNESS_CHECK(consumed.bytes_ == reference->bytes_ &&
                               consumed.checksum_ == reference->checksum_,
                           "Arm \"", arms[a].name_, "\" of \"", title,
                           "\" does not produce the reference output");
    }
    for (size_t trial = 0; trial < NUM_TRIALS; ++trial) {
      for (size_t a = 0; a < arms.size(); ++a) {
        const double seconds =
            secondsOf(arms[a], triples, passes[a], sinkChecksum);
        nsPerTriple[a].push_back(seconds * 1e9 /
                                 static_cast<double>(passes[a] * n));
      }
    }

    std::vector<std::string> rowNames;
    for (const auto& arm : arms) {
      rowNames.push_back(arm.name_);
    }
    auto& table = results.addTable(
        title, rowNames,
        {"arm", "median ns/triple", "min ns/triple", "max ns/triple",
         "passes per trial", "heap allocations per triple", "bytes per triple",
         "median vs arm 0"});
    table.metadata().addKeyValuePair("triples", n);
    table.metadata().addKeyValuePair("trials", NUM_TRIALS);
    table.metadata().addKeyValuePair("sink-checksum", sinkChecksum);
    double baseMedian = 0;
    for (size_t a = 0; a < arms.size(); ++a) {
      auto sorted = nsPerTriple[a];
      std::sort(sorted.begin(), sorted.end());
      const double median =
          (sorted[(NUM_TRIALS - 1) / 2] + sorted[NUM_TRIALS / 2]) / 2;
      if (a == 0) {
        baseMedian = median;
      }
      table.setEntry(a, 1, static_cast<float>(median));
      table.setEntry(a, 2, static_cast<float>(sorted.front()));
      table.setEntry(a, 3, static_cast<float>(sorted.back()));
      table.setEntry(a, 4, passes[a]);
      table.setEntry(
          a, 5,
          static_cast<float>(static_cast<double>(allocationsPerRun[a]) /
                             static_cast<double>(n)));
      table.setEntry(a, 6,
                     static_cast<float>(static_cast<double>(bytesPerRun[a]) /
                                        static_cast<double>(n)));
      table.setEntry(
          a, 7, absl::StrCat(100.0 * (median - baseMedian) / baseMedian, " %"));
    }
  }

 public:
  SerializerMicroBenchmark() {
    std::cout << "Generating " << NUM_TRIPLES
              << " synthetic triples per dataset..." << std::endl;
    mixedTriples_ = generateMixedTriples(NUM_TRIPLES);
    titleTriples_ = generateTitleTriples(NUM_TRIPLES);
    std::cout << "Synthetic dataset generation complete." << std::endl;
  }

  std::string name() const final {
    return "SPARQL Export Serializer MicroBenchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    measure(results, "Turtle, title-like triples (H-vocab-title-large shape)",
            turtleArms(), titleTriples_);
    measure(results, "Turtle, mixed triples", turtleArms(), mixedTriples_);
    measure(results, "CSV, mixed triples",
            tabularArms(ad_utility::MediaType::csv), mixedTriples_);
    measure(results, "TSV, mixed triples",
            tabularArms(ad_utility::MediaType::tsv), mixedTriples_);
    return results;
  }
};

AD_REGISTER_BENCHMARK(SerializerMicroBenchmark);

}  // namespace
}  // namespace ad_benchmark

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
