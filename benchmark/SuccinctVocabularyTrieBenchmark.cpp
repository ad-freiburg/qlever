// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "index/vocabulary/SuccinctVocabularyTrie.h"

using namespace ql::index::vocab;
using namespace ad_benchmark;

class BMSuccinctVocabularyTrieRank1 : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "SuccinctVocabularyTrie POPCNT Rank1";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    constexpr size_t NUM_QUERIES = 20'000'000;
    constexpr size_t BIT_WORDS = 100'000;  // 6.4 million bits

    std::vector<uint64_t> bits(BIT_WORDS,
                               0xAAAAAAAAAAAAAAAAULL);  // alternating bits
    std::vector<char> labels(BIT_WORDS, 'x');

    SuccinctVocabularyTrie trie;
    trie.setMockTopology(bits, labels);

    // Wall time is measured by the benchmark framework. The accumulated total
    // is written to a volatile sink, so the compiler cannot drop the queries.
    results.addMeasurement("Rank1 queries", [&trie, &bits]() {
      size_t totalRank = 0;
      for (size_t i = 0; i < NUM_QUERIES; ++i) {
        size_t bitIdx = (i * 137) % (bits.size() * 64);
        totalRank += trie.rank1(bitIdx);
      }
      [[maybe_unused]] volatile size_t sink = totalRank;
    });

    return results;
  }
};

AD_REGISTER_BENCHMARK(BMSuccinctVocabularyTrieRank1);
