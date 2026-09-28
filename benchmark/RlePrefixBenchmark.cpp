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

#include "engine/RlePrefixCompressor.h"
#include "global/Id.h"
#include "global/ValueId.h"

using namespace ql::engine::rle;

namespace {

// _____________________________________________________________________________
// Mock Vocabulary Index for fast ID-to-string lookup simulation.
class MockVocabulary {
 private:
  std::vector<std::string> words_;

 public:
  MockVocabulary() = default;

  uint64_t insert(std::string word) {
    uint64_t idx = words_.size();
    words_.push_back(std::move(word));
    return idx;
  }

  [[nodiscard]] std::string_view lookup(ValueId id) const noexcept {
    uint64_t idx = id.getVocabIndex().get();
    if (idx < words_.size()) {
      return words_[idx];
    }
    return "";
  }

  [[nodiscard]] size_t size() const noexcept { return words_.size(); }
};

// _____________________________________________________________________________
// Synthetic Wikidata sorted SPO triple stream dataset.
// Subject runs average 500 triples per entity.
struct WikidataStreamDataset {
  std::vector<ValueId> subjects_;
  std::vector<ValueId> predicates_;
  std::vector<ValueId> objects_;
  MockVocabulary vocab_;

  static WikidataStreamDataset generate(size_t targetTriples,
                                        size_t avgTriplesPerSubject = 500,
                                        uint32_t seed = 42) {
    WikidataStreamDataset ds;
    ds.subjects_.reserve(targetTriples);
    ds.predicates_.reserve(targetTriples);
    ds.objects_.reserve(targetTriples);

    std::mt19937 gen(seed);
    std::poisson_distribution<size_t> runDist(
        static_cast<double>(avgTriplesPerSubject));
    std::uniform_int_distribution<size_t> objDist(0, 999'999);

    // Populate common predicates in vocabulary
    std::vector<ValueId> predicateIds;
    predicateIds.reserve(50);
    for (size_t p = 0; p < 50; ++p) {
      uint64_t idx = ds.vocab_.insert("http://www.wikidata.org/prop/direct/P" +
                                      std::to_string(p + 1));
      predicateIds.push_back(
          ValueId::makeFromVocabIndex(VocabIndex::make(idx)));
    }

    size_t currentSubjectNum = 1;
    while (ds.subjects_.size() < targetTriples) {
      size_t runLen = std::max<size_t>(1, runDist(gen));
      if (ds.subjects_.size() + runLen > targetTriples) {
        runLen = targetTriples - ds.subjects_.size();
      }

      uint64_t subjIdx = ds.vocab_.insert("http://www.wikidata.org/entity/Q" +
                                          std::to_string(currentSubjectNum++));
      ValueId subjId = ValueId::makeFromVocabIndex(VocabIndex::make(subjIdx));

      for (size_t r = 0; r < runLen; ++r) {
        ds.subjects_.push_back(subjId);

        // Clustered predicates
        size_t pIndex = (r / 20) % predicateIds.size();
        ds.predicates_.push_back(predicateIds[pIndex]);

        // Object
        uint64_t objIdx = ds.vocab_.insert("http://www.wikidata.org/entity/Q" +
                                           std::to_string(objDist(gen)));
        ds.objects_.push_back(
            ValueId::makeFromVocabIndex(VocabIndex::make(objIdx)));
      }
    }

    return ds;
  }
};

// _____________________________________________________________________________
// Baseline 1: Standard Per-Row Dynamic Serializer (lookup + format every row).
struct StandardRowSerializer {
  static size_t serializeTriples(const WikidataStreamDataset& ds, char* out,
                                 size_t& totalLookups) {
    char* curr = out;
    const size_t n = ds.subjects_.size();
    totalLookups = 0;

    for (size_t i = 0; i < n; ++i) {
      // 3 lookups per triple
      std::string_view s = ds.vocab_.lookup(ds.subjects_[i]);
      std::string_view p = ds.vocab_.lookup(ds.predicates_[i]);
      std::string_view o = ds.vocab_.lookup(ds.objects_[i]);
      totalLookups += 3;

      // Subject
      *curr++ = '<';
      std::memcpy(curr, s.data(), s.size());
      curr += s.size();
      *curr++ = '>';
      *curr++ = ' ';

      // Predicate
      *curr++ = '<';
      std::memcpy(curr, p.data(), p.size());
      curr += p.size();
      *curr++ = '>';
      *curr++ = ' ';

      // Object
      *curr++ = '<';
      std::memcpy(curr, o.data(), o.size());
      curr += o.size();
      *curr++ = '>';

      // Terminator
      std::memcpy(curr, " .\n", 3);
      curr += 3;
    }
    return static_cast<size_t>(curr - out);
  }
};

// _____________________________________________________________________________
// Optimization 24: RleTripleFormatter Serializer with Constant Folding.
struct RleConstantFoldingSerializer {
  static size_t serializeTriples(const WikidataStreamDataset& ds, char* out,
                                 size_t& totalLookups) {
    auto formatter = RleTripleFormatter::makeNTriplesFormatter();
    totalLookups = 0;

    auto lookupFunctor = [&](ValueId id) noexcept -> std::string_view {
      ++totalLookups;
      return ds.vocab_.lookup(id);
    };

    char* end = formatter.formatTriplesBatch(ds.subjects_, ds.predicates_,
                                             ds.objects_, lookupFunctor, out);
    return static_cast<size_t>(end - out);
  }
};

// _____________________________________________________________________________
struct BenchmarkResult {
  std::string name_;
  double elapsedMs_ = 0.0;
  double throughputMTriplesPerSec_ = 0.0;
  double nsPerTriple_ = 0.0;
  size_t totalLookups_ = 0;
  double lookupReductionPct_ = 0.0;
  size_t bytesWritten_ = 0;
};

template <typename Serializer>
BenchmarkResult runBenchmark(const std::string& name,
                             const WikidataStreamDataset& ds,
                             std::vector<char>& outputBuffer,
                             size_t iterations = 5) {
  // Warmup
  size_t dummyLookups = 0;
  Serializer::serializeTriples(ds, outputBuffer.data(), dummyLookups);

  double totalMs = 0.0;
  size_t totalLookups = 0;
  size_t bytesWritten = 0;

  for (size_t iter = 0; iter < iterations; ++iter) {
    auto t0 = std::chrono::high_resolution_clock::now();

    bytesWritten =
        Serializer::serializeTriples(ds, outputBuffer.data(), totalLookups);

    auto t1 = std::chrono::high_resolution_clock::now();
    totalMs += std::chrono::duration<double, std::milli>(t1 - t0).count();
  }

  double avgMs = totalMs / static_cast<double>(iterations);
  double totalTriples = static_cast<double>(ds.subjects_.size());
  double mTriplesPerSec = (totalTriples / (avgMs / 1000.0)) / 1e6;
  double nsPerTriple = (avgMs * 1e6) / totalTriples;

  size_t baselineLookups = ds.subjects_.size() * 3;
  double reductionPct = 100.0 * (1.0 - (static_cast<double>(totalLookups) /
                                        static_cast<double>(baselineLookups)));

  return BenchmarkResult{name,        avgMs,        mTriplesPerSec,
                         nsPerTriple, totalLookups, reductionPct,
                         bytesWritten};
}

void printResults(const std::vector<BenchmarkResult>& results) {
  std::cout << "\n============================================================="
               "===========================================\n";
  std::cout << " Optimization 24: RLE Prefix Constant Folding Benchmark "
               "(DuckDB-style)\n";
  std::cout << " Workload: Sorted Wikidata SPO Triples Stream (Avg 500 triples "
               "/ subject)\n";
  std::cout << "==============================================================="
               "=========================================\n\n";

  std::cout << std::left << std::setw(32) << "Serializer Strategy" << std::right
            << std::setw(12) << "Time (ms)" << std::setw(20)
            << "Throughput (M/s)" << std::setw(16) << "ns / triple"
            << std::setw(18) << "Vocab Lookups" << std::setw(16)
            << "Lookup Savings"
            << "\n";
  std::cout << std::string(114, '-') << "\n";

  for (const auto& r : results) {
    std::cout << std::left << std::setw(32) << r.name_ << std::right
              << std::fixed << std::setprecision(2) << std::setw(12)
              << r.elapsedMs_ << std::setw(20) << r.throughputMTriplesPerSec_
              << std::setw(16) << r.nsPerTriple_ << std::setw(18)
              << r.totalLookups_ << std::setw(15) << r.lookupReductionPct_
              << "%"
              << "\n";
  }

  std::cout << std::string(114, '-') << "\n\n";

  if (results.size() >= 2) {
    double baseThroughput = results[0].throughputMTriplesPerSec_;
    double rleThroughput = results[1].throughputMTriplesPerSec_;
    double speedup = rleThroughput / baseThroughput;
    std::cout << ">> RLE Prefix Constant Folding Speedup: " << std::fixed
              << std::setprecision(2) << speedup << "x ("
              << ((speedup - 1.0) * 100.0) << "% throughput improvement)\n";
    std::cout << ">> Total Vocabulary String Lookups Reduced by: " << std::fixed
              << std::setprecision(1) << results[1].lookupReductionPct_
              << "% across the triple stream\n";
  }
  std::cout << "==============================================================="
               "=========================================\n\n";
}

}  // namespace

int main(int argc, char** argv) {
  size_t numTriples = 2'000'000;
  if (argc > 1) {
    try {
      numTriples = std::stoull(argv[1]);
    } catch (const std::exception& e) {
      std::cerr << "Invalid argument '" << argv[1] << "': " << e.what()
                << ", using default " << numTriples << ".\n";
    }
  }

  std::cout << "Generating sorted Wikidata SPO stream with " << numTriples
            << " triples (avg 500 triples / subject)...\n";
  auto dataset = WikidataStreamDataset::generate(numTriples, 500);

  // Allocate 512 MB buffer for formatted outputs
  std::vector<char> outputBuffer(numTriples * 256);

  std::vector<BenchmarkResult> results;
  std::cout << "Running Benchmark 1: Standard Per-Row Dynamic Serializer...\n";
  results.push_back(runBenchmark<StandardRowSerializer>(
      "Standard Per-Row Serializer", dataset, outputBuffer));

  std::cout << "Running Benchmark 2: RlePrefixConstantFolding Serializer...\n";
  results.push_back(runBenchmark<RleConstantFoldingSerializer>(
      "RLE Constant Folding (DuckDB)", dataset, outputBuffer));

  printResults(results);
  return 0;
}
