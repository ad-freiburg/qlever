// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// End-to-end benchmark for the `lookupBatch` implementation of the hybrid
// `VocabularyInternalExternal` (the on-disk part is covered by the micro
// benchmark): a large vocabulary (200k words, half of them disk-only) and 50k
// random lookups across the whole id range. The benchmark measures the
// wall-clock time of resolving all queries one word at a time (the previous
// behavior for repeated lookups) and of resolving them with batched
// `lookupBatch` calls. Compare the reported `ns-per-word` values of the two
// measurements to obtain the speedup. Every measurement repeats all lookups
// until at least `VOCAB_LOOKUP_MIN_SECONDS` (default 10) seconds have passed;
// the time per word is in the measurement's metadata and in a tab-separated
// `VOCAB_LOOKUP <group> <measurement> <ns/word> <rounds> <seconds>` line.
// `VOCAB_LOOKUP_ORDER=batch-first` measures `lookupBatch` first (the default
// is `single-first`), which reveals a bias from warming the page cache.

#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "backports/filesystem.h"
#include "backports/span.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"

namespace ad_benchmark {
namespace {

// Create `numWords` synthetic words with varying lengths (29 to 86 bytes for
// `numWords <= 200'000`) that resemble short IRIs/literals without requiring a
// real dataset. The words are not sorted, which is fine because
// `lookupBatch` only looks up by index.
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

// Return `numQueries` distinct deterministic shuffled query ids in
// `[0, vocabSize)`. `numQueries` must not exceed `vocabSize`.
std::vector<size_t> makeQueryIds(size_t vocabSize, size_t numQueries,
                                 uint32_t seed) {
  AD_CONTRACT_CHECK(numQueries <= vocabSize);
  std::vector<size_t> ids(vocabSize);
  std::iota(ids.begin(), ids.end(), size_t{0});
  std::shuffle(ids.begin(), ids.end(), std::mt19937{seed});
  ids.resize(numQueries);
  return ids;
}

// Write `words` to a hybrid `VocabularyInternalExternal` at `filename`, open it
// and return it. Word `words[i]` gets id `i`. Every second word is disk-only,
// the others are additionally cached in RAM (so the milestone rule, which
// caches every 1000th word, never triggers for this input). The returned
// vocabulary keeps the files open, so they must exist while it is used.
VocabularyInternalExternal buildHybridVocabulary(
    const std::string& filename, ql::span<const std::string> words) {
  {
    auto writerPtr = VocabularyInternalExternal::makeDiskWriterPtr(filename);
    for (size_t i = 0; i < words.size(); ++i) {
      // The second argument is `isExternal`: true means disk-only.
      (*writerPtr)(words[i], i % 2 == 0);
    }
    writerPtr->finish();
  }
  VocabularyInternalExternal vocab;
  vocab.open(filename);
  return vocab;
}

// Parse a positive, finite number of seconds from `VOCAB_LOOKUP_MIN_SECONDS`
// and return `defaultValue` if it is unset. Trigger a contract violation with
// a clear message for a malformed or non-positive value.
double parseMinSecondsFromEnvironment(double defaultValue) {
  const char* value = std::getenv("VOCAB_LOOKUP_MIN_SECONDS");
  if (value == nullptr) {
    return defaultValue;
  }
  errno = 0;
  char* end = nullptr;
  const double parsed = std::strtod(value, &end);
  AD_CONTRACT_CHECK(end != value && *end == '\0' && errno != ERANGE &&
                        std::isfinite(parsed) && parsed > 0,
                    "VOCAB_LOOKUP_MIN_SECONDS must be a positive number");
  return parsed;
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
        ql::filesystem::temp_directory_path() / "qleverVocabBatchEndToEnd";
    ql::filesystem::remove_all(benchmarkDir);
    ql::filesystem::create_directories(benchmarkDir);
    // Also remove the directory if a check or a measurement throws.
    absl::Cleanup removeBenchmarkDir{
        [&benchmarkDir] { ql::filesystem::remove_all(benchmarkDir); }};

    constexpr size_t numWords = 200'000;
    constexpr size_t numQueries = 50'000;
    // Read the minimum duration of one measurement from
    // `VOCAB_LOOKUP_MIN_SECONDS` (default 10), so that a measurement is never a
    // single short sample.
    const double minSeconds = parseMinSecondsFromEnvironment(10.0);
    const auto words = makeWords(numWords);
    auto vocab = buildHybridVocabulary(
        (benchmarkDir / "endtoend.hybrid").string(), words);
    AD_CONTRACT_CHECK(vocab.size() == numWords);
    const auto ids = makeQueryIds(numWords, numQueries, 7);
    {
      // Both lookup paths must return the same words, otherwise the
      // comparison below is meaningless.
      const auto batchWords = vocab.lookupBatch(ids);
      AD_CONTRACT_CHECK(batchWords.size() == ids.size());
      for (size_t i = 0; i < ids.size(); ++i) {
        AD_CONTRACT_CHECK(batchWords[i] == vocab[ids[i]]);
      }
    }

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
        const auto result = vocab.lookupBatch(ids);
        for (std::string_view word : result) {
          total += word.size();
        }
        return total;
      });
    };
    const char* orderValue = std::getenv("VOCAB_LOOKUP_ORDER");
    const std::string_view order =
        orderValue != nullptr ? orderValue : "single-first";
    AD_CONTRACT_CHECK(order == "single-first" || order == "batch-first",
                      "VOCAB_LOOKUP_ORDER must be `single-first` or "
                      "`batch-first`");
    if (order == "batch-first") {
      batched();
      single();
    } else {
      single();
      batched();
    }
    // Print the checksum so that the measured lookups cannot be optimized
    // away (see the same pattern in `BenchmarkExamples.cpp`).
    std::cout << "end-to-end checksum: " << checksum << '\n';

    return results;
  }
};

AD_REGISTER_BENCHMARK(BMVocabBatchLookupEndToEnd);
}  // namespace ad_benchmark
