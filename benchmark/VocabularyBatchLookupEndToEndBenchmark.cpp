// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Full end-to-end benchmark for the vocabulary `lookupBatch` implementations
// added for `VocabularyOnDisk` and `VocabularyInternalExternal`: large hybrid
// vocabulary (200k words, half of them disk-only) and 50k random lookups
// across the whole id range. Measures the wall-clock time of resolving all
// queries one word at a time (the previous behavior for repeated lookups)
// against resolving them with batched `lookupBatch` calls. The speedup is the
// ratio of the `sequential single-word lookups` time to the `lookupBatch`
// time. Every measurement repeats all lookups until at least
// `VOCAB_LOOKUP_MIN_SECONDS` (default 10) seconds have passed; the time per
// word is in the measurement's metadata and in a tab-separated
// `VOCAB_LOOKUP` line. `VOCAB_LOOKUP_ORDER=batch-first` measures
// `lookupBatch` first.

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "index/vocabulary/VocabularyInternalExternal.h"

namespace ad_benchmark {
namespace {

// Create `numWords` synthetic words with varying lengths (8 to 72 bytes) that
// resemble short IRIs/literals without requiring a real dataset.
std::vector<std::string> makeWords(size_t numWords) {
  std::vector<std::string> words;
  words.reserve(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    std::string word = "<http://example.org/entity/" + std::to_string(i) + ">";
    // Vary the length so that batches cover small and large words.
    word.append(i % 53, 'x');
    words.push_back(std::move(word));
  }
  return words;
}

// Deterministic shuffled query ids in `[0, vocabSize)`.
std::vector<size_t> makeQueryIds(size_t vocabSize, size_t numQueries,
                                 uint32_t seed) {
  std::vector<size_t> ids(vocabSize);
  std::iota(ids.begin(), ids.end(), size_t{0});
  std::shuffle(ids.begin(), ids.end(), std::mt19937{seed});
  ids.resize(numQueries);
  return ids;
}

// Write `words` to a hybrid `VocabularyInternalExternal` at `filename`: every
// second word is disk-only, the rest is additionally cached in RAM (plus the
// regular milestones).
VocabularyInternalExternal buildHybridVocabulary(
    const std::string& filename, const std::vector<std::string>& words) {
  {
    auto writerPtr = VocabularyInternalExternal::makeDiskWriterPtr(filename);
    for (size_t i = 0; i < words.size(); ++i) {
      (*writerPtr)(words[i], i % 2 == 0);
    }
    writerPtr->finish();
  }
  VocabularyInternalExternal vocab;
  vocab.open(filename);
  return vocab;
}
}  // namespace

class BMVocabBatchLookupEndToEnd : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "Vocabulary lookupBatch end-to-end benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    const auto benchmarkDir =
        std::filesystem::temp_directory_path() / "qleverVocabBatchEndToEnd";
    std::filesystem::remove_all(benchmarkDir);
    std::filesystem::create_directories(benchmarkDir);

    constexpr size_t numWords = 200'000;
    constexpr size_t numQueries = 50'000;
    // Minimum duration of one measurement: `VOCAB_LOOKUP_MIN_SECONDS`,
    // default 10 (so a measurement is never a single short sample).
    const char* minSecondsValue = std::getenv("VOCAB_LOOKUP_MIN_SECONDS");
    const double minSeconds =
        minSecondsValue != nullptr ? std::stod(minSecondsValue) : 10.0;
    const auto words = makeWords(numWords);
    auto vocab = buildHybridVocabulary(
        (benchmarkDir / "endtoend.hybrid").string(), words);
    const auto ids = makeQueryIds(numWords, numQueries, 7);

    auto& group = results.addGroup(
        "VocabularyInternalExternal, 200k words, 50k shuffled lookups");
    group.metadata().addKeyValuePair("num-words", numWords);
    group.metadata().addKeyValuePair("num-queries", numQueries);
    group.metadata().addKeyValuePair("min-seconds-per-measurement", minSeconds);
    size_t checksum = 0;
    // Repeat `resolveAll` (all `numQueries` lookups) until at least
    // `minSeconds` have passed; record the time per word.
    auto measure = [&](const std::string& name, const auto& resolveAll) {
      using Clock = std::chrono::steady_clock;
      size_t rounds = 0;
      double seconds = 0;
      auto& entry = group.addMeasurement(name, [&]() {
        const auto start = Clock::now();
        do {
          checksum += resolveAll();
          ++rounds;
          seconds = std::chrono::duration<double>(Clock::now() - start).count();
        } while (seconds < minSeconds);
      });
      const double nsPerWord =
          seconds * 1e9 / static_cast<double>(rounds * numQueries);
      entry.metadata().addKeyValuePair("ns-per-word", nsPerWord);
      entry.metadata().addKeyValuePair("rounds", rounds);
      std::cout << "VOCAB_LOOKUP\te2e-200k-50k\t" << name << '\t' << nsPerWord
                << '\t' << rounds << '\t' << seconds << std::endl;
    };
    auto single = [&]() {
      measure("sequential single-word lookups", [&]() {
        size_t total = 0;
        for (size_t idx : ids) {
          total += vocab[idx].size();
        }
        return total;
      });
    };
    auto batched = [&]() {
      measure("lookupBatch", [&]() {
        size_t total = 0;
        auto result = vocab.lookupBatch(ids);
        for (std::string_view word : result) {
          total += word.size();
        }
        return total;
      });
    };
    const char* orderValue = std::getenv("VOCAB_LOOKUP_ORDER");
    if (orderValue != nullptr && std::string{orderValue} == "batch-first") {
      batched();
      single();
    } else {
      single();
      batched();
    }
    // Print the checksum so that the measured lookups cannot be optimized
    // away (see the same pattern in `BenchmarkExamples.cpp`).
    std::cout << "end-to-end checksum: " << checksum << '\n';

    std::filesystem::remove_all(benchmarkDir);
    return results;
  }
};

AD_REGISTER_BENCHMARK(BMVocabBatchLookupEndToEnd);
}  // namespace ad_benchmark
